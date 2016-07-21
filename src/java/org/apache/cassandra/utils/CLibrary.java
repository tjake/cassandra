/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.utils;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jna.*;
import com.sun.jna.ptr.IntByReference;

public final class CLibrary
{
    private static final Logger logger = LoggerFactory.getLogger(CLibrary.class);

    private static final VersionHelper UNKNOWN = new VersionHelper(0,0,0);
    private static final VersionHelper VERSION_2_6 = new VersionHelper(2,6,0);

    private static final VersionHelper version;
    private static final int SYS_gettid = Platform.is64Bit() ? 186 : 224;
    private static final int SYS_getcpu = 318;
    private static final Object[] NO_ARGS = {};

    private static final int MCL_CURRENT;
    private static final int MCL_FUTURE;

    private static final int ENOMEM = 12;

    private static final int F_GETFL   = 3;  /* get file status flags */
    private static final int F_SETFL   = 4;  /* set file status flags */
    private static final int F_NOCACHE = 48; /* Mac OS X specific flag, turns cache on/off */
    private static final int O_DIRECT  = 040000; /* fcntl.h */
    private static final int O_RDONLY  = 00000000; /* fcntl.h */

    private static final int POSIX_FADV_NORMAL     = 0; /* fadvise.h */
    private static final int POSIX_FADV_RANDOM     = 1; /* fadvise.h */
    private static final int POSIX_FADV_SEQUENTIAL = 2; /* fadvise.h */
    private static final int POSIX_FADV_WILLNEED   = 3; /* fadvise.h */
    private static final int POSIX_FADV_DONTNEED   = 4; /* fadvise.h */
    private static final int POSIX_FADV_NOREUSE    = 5; /* fadvise.h */


    static boolean jnaAvailable = true;
    static boolean jnaLockable = false;

    static
    {
        try
        {
            Native.register("c");
        }
        catch (NoClassDefFoundError e)
        {
            logger.warn("JNA not found. Native methods will be disabled.");
            jnaAvailable = false;
        }
        catch (UnsatisfiedLinkError e)
        {
            logger.warn("JNA link failure, one or more native method will be unavailable.");
            logger.trace("JNA link failure details: {}", e.getMessage());
        }
        catch (NoSuchMethodError e)
        {
            logger.warn("Obsolete version of JNA present; unable to register C library. Upgrade to JNA 3.2.7 or later");
            jnaAvailable = false;
        }

        //Find the version name
        final utsname uname = new utsname();
        VersionHelper ver = UNKNOWN;
        try
        {
            if(uname(uname) == 0)
                ver = new VersionHelper(uname.getRealeaseVersion());
        }
        catch(Throwable e)
        {
            logger.warn("Failed to determine Linux version: " + e);
        }

        version = ver;

        if (System.getProperty("os.arch").toLowerCase().contains("ppc"))
        {
            if (System.getProperty("os.name").toLowerCase().contains("linux"))
            {
               MCL_CURRENT = 0x2000;
               MCL_FUTURE = 0x4000;
            }
            else if (System.getProperty("os.name").toLowerCase().contains("aix"))
            {
                MCL_CURRENT = 0x100;
                MCL_FUTURE = 0x200;
            }
            else
            {
                MCL_CURRENT = 1;
                MCL_FUTURE = 2;
            }
        }
        else
        {
            MCL_CURRENT = 1;
            MCL_FUTURE = 2;
        }
    }

    public static class cpu_set_t extends Structure
    {
        static List<String> FIELD_ORDER = Arrays.asList("__bits");
        static final int  __CPU_SETSIZE = 1024;
        static final int __NCPUBITS = 8 * NativeLong.SIZE;
        static final int SIZE_OF_CPU_SET_T = (__CPU_SETSIZE / __NCPUBITS) * NativeLong.SIZE;
        public NativeLong[] __bits = new NativeLong[__CPU_SETSIZE / __NCPUBITS];
        public cpu_set_t() {
            for(int i = 0; i < __bits.length; i++) {
                __bits[i] = new NativeLong(0);
            }
        }

        @Override
        protected List getFieldOrder() {
            return FIELD_ORDER;
        }

        @SuppressWarnings({"UnusedDeclaration"})
        public static void __CPU_ZERO(cpu_set_t cpuset) {
            for(NativeLong bits : cpuset.__bits) {
                bits.setValue(0l);
            }
        }

        public static int __CPUELT(int cpu) {
            return cpu / __NCPUBITS;
        }

        public static long __CPUMASK(int cpu) {
            return 1l << (cpu % __NCPUBITS);
        }

        @SuppressWarnings({"UnusedDeclaration"})
        public static void __CPU_SET(int cpu, cpu_set_t cpuset ) {
            cpuset.__bits[__CPUELT(cpu)].setValue(
            cpuset.__bits[__CPUELT(cpu)].longValue() | __CPUMASK(cpu));
        }

        @SuppressWarnings({"UnusedDeclaration"})
        public static void __CPU_CLR(int cpu, cpu_set_t cpuset ) {
            cpuset.__bits[__CPUELT(cpu)].setValue(
            cpuset.__bits[__CPUELT(cpu)].longValue() & ~__CPUMASK(cpu));
        }

        @SuppressWarnings({"UnusedDeclaration"})
        public static boolean __CPU_ISSET(int cpu, cpu_set_t cpuset ) {
            return (cpuset.__bits[__CPUELT(cpu)].longValue() & __CPUMASK(cpu)) != 0;
        }
    }

    /** Structure describing the system and machine.  */
    public static class utsname extends Structure {
        public static final int _UTSNAME_LENGTH = 65;

        static List<String> FIELD_ORDER = Arrays.asList(
        "sysname",
        "nodename",
        "release",
        "version",
        "machine",
        "domainname"
        );

        /** Name of the implementation of the operating system.  */
        public byte[] sysname = new byte[_UTSNAME_LENGTH];

        /** Name of this node on the network.  */
        public byte[] nodename = new byte[_UTSNAME_LENGTH];

        /** Current release level of this implementation.  */
        public byte[] release = new byte[_UTSNAME_LENGTH];

        /** Current version level of this release.  */
        public byte[] version = new byte[_UTSNAME_LENGTH];

        /** Name of the hardware type the system is running on.  */
        public byte[] machine = new byte[_UTSNAME_LENGTH];

        /** NIS or YP domain name */
        public byte[] domainname = new byte[_UTSNAME_LENGTH];

        @Override
        protected List getFieldOrder() {
            return FIELD_ORDER;
        }

        static int length(final byte[] data) {
            int len = 0;
            final int datalen = data.length;
            while(len < datalen && data[len] != 0)
                len++;
            return len;
        }

        public String getSysname() {
            return new String(sysname, 0, length(sysname));
        }

        @SuppressWarnings({"UnusedDeclaration"})
        public String getNodename() {
            return new String(nodename, 0, length(nodename));
        }

        public String getRelease() {
            return new String(release, 0, length(release));
        }

        public String getRealeaseVersion() {
            final String release = getRelease();
            final int releaseLen = release.length();
            int len = 0;
            for(;len < releaseLen; len++) {
                final char c = release.charAt(len);
                if(Character.isDigit(c) || c == '.') {
                    continue;
                }
                break;
            }
            return release.substring(0, len);
        }

        public String getVersion() {
            return new String(version, 0, length(version));
        }

        public String getMachine() {
            return new String(machine, 0, length(machine));
        }

        @SuppressWarnings({"UnusedDeclaration"})
        public String getDomainname() {
            return new String(domainname, 0, length(domainname));
        }

        @Override
        public String toString() {
            return getSysname() + " " + getRelease() +
                   " " + getVersion() + " " + getMachine();
        }
    }

    private static native int mlockall(int flags) throws LastErrorException;
    private static native int munlockall() throws LastErrorException;
    private static native int fcntl(int fd, int command, long flags) throws LastErrorException;
    private static native int posix_fadvise(int fd, long offset, int len, int flag) throws LastErrorException;
    private static native int open(String path, int flags) throws LastErrorException;
    private static native int fsync(int fd) throws LastErrorException;
    private static native int close(int fd) throws LastErrorException;
    private static native Pointer strerror(int errnum) throws LastErrorException;

    private static native int sched_setaffinity(int pid, int cpusetsize, cpu_set_t cpuset) throws LastErrorException;
    private static native int uname(utsname name) throws LastErrorException;
    private static native int syscall(int number, IntByReference arg1, IntByReference arg2, IntByReference terminator) throws LastErrorException;
    private static native int syscall(int number) throws LastErrorException;
    private static native int sched_getcpu() throws LastErrorException;


    private static int errno(RuntimeException e)
    {
        assert e instanceof LastErrorException;
        try
        {
            return ((LastErrorException) e).getErrorCode();
        }
        catch (NoSuchMethodError x)
        {
            logger.warn("Obsolete version of JNA present; unable to read errno. Upgrade to JNA 3.2.7 or later");
            return 0;
        }
    }

    private CLibrary() {}

    public static boolean jnaAvailable()
    {
        return jnaAvailable;
    }

    public static boolean jnaMemoryLockable()
    {
        return jnaLockable;
    }

    public static void tryMlockall()
    {
        try
        {
            mlockall(MCL_CURRENT);
            jnaLockable = true;
            logger.info("JNA mlockall successful");
        }
        catch (UnsatisfiedLinkError e)
        {
            // this will have already been logged by CLibrary, no need to repeat it
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            if (errno(e) == ENOMEM && System.getProperty("os.name").toLowerCase().contains("linux"))
            {
                logger.warn("Unable to lock JVM memory (ENOMEM)."
                        + " This can result in part of the JVM being swapped out, especially with mmapped I/O enabled."
                        + " Increase RLIMIT_MEMLOCK or run Cassandra as root.");
            }
            else if (!System.getProperty("os.name").toLowerCase().contains("mac"))
            {
                // OS X allows mlockall to be called, but always returns an error
                logger.warn("Unknown mlockall error {}", errno(e));
            }
        }
    }

    public static void trySkipCache(String path, long offset, long len)
    {
        File f = new File(path);
        if (!f.exists())
            return;

        try (FileInputStream fis = new FileInputStream(f))
        {
            trySkipCache(getfd(fis.getChannel()), offset, len, path);
        }
        catch (IOException e)
        {
            logger.warn("Could not skip cache", e);
        }
    }

    public static void trySkipCache(int fd, long offset, long len, String path)
    {
        if (len == 0)
            trySkipCache(fd, 0, 0, path);

        while (len > 0)
        {
            int sublen = (int) Math.min(Integer.MAX_VALUE, len);
            trySkipCache(fd, offset, sublen, path);
            len -= sublen;
            offset -= sublen;
        }
    }

    public static void trySkipCache(int fd, long offset, int len, String path)
    {
        if (fd < 0)
            return;

        try
        {
            if (System.getProperty("os.name").toLowerCase().contains("linux"))
            {
                int result = posix_fadvise(fd, offset, len, POSIX_FADV_DONTNEED);
                if (result != 0)
                    NoSpamLogger.log(
                            logger,
                            NoSpamLogger.Level.WARN,
                            10,
                            TimeUnit.MINUTES,
                            "Failed trySkipCache on file: {} Error: " + strerror(result).getString(0),
                            path);
            }
        }
        catch (UnsatisfiedLinkError e)
        {
            // if JNA is unavailable just skipping Direct I/O
            // instance of this class will act like normal RandomAccessFile
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            logger.warn(String.format("posix_fadvise(%d, %d) failed, errno (%d).", fd, offset, errno(e)));
        }
    }

    public static int tryFcntl(int fd, int command, int flags)
    {
        // fcntl return value may or may not be useful, depending on the command
        int result = -1;

        try
        {
            result = fcntl(fd, command, flags);
        }
        catch (UnsatisfiedLinkError e)
        {
            // if JNA is unavailable just skipping
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            logger.warn(String.format("fcntl(%d, %d, %d) failed, errno (%d).", fd, command, flags, errno(e)));
        }

        return result;
    }

    public static long tryGetThreadId()
    {
        try
        {
            final long ret = syscall(SYS_gettid);
            if (ret < 0)
                throw new IllegalStateException("gettid failed; errno=" + Native.getLastError());

            return ret;
        }
        catch (UnsatisfiedLinkError e)
        {
            // if JNA is unavailable just skipping
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            logger.warn(String.format("gettid failed, errno (%d).", errno(e)));
        }

        return -1L;
    }

    public static boolean trySetAffinity(final long tid, final long affinity)
    {
        if (tid == -1)
            return false;

        final cpu_set_t cpuset = new cpu_set_t();
        final int size = version.isSameOrNewer(VERSION_2_6) ? cpu_set_t.SIZE_OF_CPU_SET_T : NativeLong.SIZE;

        if(Platform.is64Bit())
        {
            cpuset.__bits[0].setValue(affinity);
        }
        else
        {
            cpuset.__bits[0].setValue(affinity & 0xFFFFFFFFL);
            cpuset.__bits[1].setValue((affinity >>> 32) & 0xFFFFFFFFL);
        }
        try
        {
            if(sched_setaffinity((int)tid, size, cpuset) != 0)
            {
                throw new IllegalStateException("sched_setaffinity(0, " + size +
                                                ", 0x" + Long.toHexString(affinity) + " failed; errno=" + Native.getLastError());
            }
        }
        catch (UnsatisfiedLinkError e)
        {
            // if JNA is unavailable just skipping
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            logger.warn("sched_setaffinity(0, " + size + ", 0x" + Long.toHexString(affinity) + " failed; errno=" + errno(e));
            return false;
        }

        return true;
    }


    public static int tryGetCpu() {
        try
        {
            final int ret = sched_getcpu();
            if(ret < 0)
                throw new IllegalStateException("sched_getcpu() failed; errno=" + Native.getLastError());

            return ret;
        }
        catch (LastErrorException e)
        {
            throw new IllegalStateException("sched_getcpu() failed; errno=" + e.getErrorCode(), e);
        }
        catch (UnsatisfiedLinkError ule) {
            try
            {
                final IntByReference cpu = new IntByReference();
                final IntByReference node = new IntByReference();
                final int ret = syscall(SYS_getcpu, cpu, node, null);
                if (ret != 0)
                    throw new IllegalStateException("getcpu() failed; errno=" + Native.getLastError());

                return cpu.getValue();
            }
            catch (UnsatisfiedLinkError ule2)
            {
                // if JNA is unavailable just skipping
            }
            catch (LastErrorException lee)
            {
                if(lee.getErrorCode() == 38 && Platform.is64Bit())
                { // unknown call
                    final Pointer getcpuAddr = new Pointer((-10L << 20) + 1024L * 2L);
                    final Function getcpu = Function.getFunction(getcpuAddr, Function.C_CONVENTION);
                    final IntByReference cpu = new IntByReference();
                    if(getcpu.invokeInt(new Object[] { cpu, null, null }) < 0)
                        throw new IllegalStateException("getcpu() failed; errno=" + Native.getLastError());
                    else
                        return cpu.getValue();

                }
                else
                {
                    throw new IllegalStateException("getcpu() failed; errno=" + lee.getErrorCode(), lee);
                }
            }
        }

        return -1;
    }

    public static int tryOpenDirectory(String path)
    {
        int fd = -1;

        try
        {
            return open(path, O_RDONLY);
        }
        catch (UnsatisfiedLinkError e)
        {
            // JNA is unavailable just skipping Direct I/O
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            logger.warn(String.format("open(%s, O_RDONLY) failed, errno (%d).", path, errno(e)));
        }

        return fd;
    }

    public static void trySync(int fd)
    {
        if (fd == -1)
            return;

        try
        {
            fsync(fd);
        }
        catch (UnsatisfiedLinkError e)
        {
            // JNA is unavailable just skipping Direct I/O
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            logger.warn(String.format("fsync(%d) failed, errno (%d) {}", fd, errno(e)), e);
        }
    }

    public static void tryCloseFD(int fd)
    {
        if (fd == -1)
            return;

        try
        {
            close(fd);
        }
        catch (UnsatisfiedLinkError e)
        {
            // JNA is unavailable just skipping Direct I/O
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            logger.warn(String.format("close(%d) failed, errno (%d).", fd, errno(e)));
        }
    }

    public static int getfd(FileChannel channel)
    {
        Field field = FBUtilities.getProtectedField(channel.getClass(), "fd");

        try
        {
            return getfd((FileDescriptor)field.get(channel));
        }
        catch (IllegalArgumentException|IllegalAccessException e)
        {
            logger.warn("Unable to read fd field from FileChannel");
        }
        return -1;
    }

    /**
     * Get system file descriptor from FileDescriptor object.
     * @param descriptor - FileDescriptor objec to get fd from
     * @return file descriptor, -1 or error
     */
    public static int getfd(FileDescriptor descriptor)
    {
        Field field = FBUtilities.getProtectedField(descriptor.getClass(), "fd");

        try
        {
            return field.getInt(descriptor);
        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            logger.warn("Unable to read fd field from FileDescriptor");
        }

        return -1;
    }
}
