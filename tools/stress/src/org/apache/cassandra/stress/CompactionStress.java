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

package org.apache.cassandra.stress;

import java.io.File;
import java.io.IOError;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import javax.inject.Inject;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;

import io.airlift.command.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.stress.generate.PartitionGenerator;
import org.apache.cassandra.stress.generate.SeedManager;
import org.apache.cassandra.stress.operations.userdefined.SchemaInsert;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.tools.Util;
import org.apache.cassandra.tools.nodetool.CompactionStats;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * Tool that allows fast route to loading data for arbitrary schemas to disk
 * and compacting them.
 */
public abstract class CompactionStress implements Runnable
{
    @Inject
    public HelpOption helpOption;

    @Option(name = { "-p", "--profile" }, description = "a stress yaml file", required = true)
    String profile;

    @Option(name = { "-d", "--datadir" }, description = "data directory", required = true)
    String dataDir;

    File getDataDir()
    {
        File outputDir = new File(dataDir);

        if (!outputDir.exists())
        {
            System.err.print("Invalid output dir (missing): " + outputDir);
            System.exit(1);
        }

        if (!outputDir.isDirectory())
        {
            System.err.print("Invalid output dir (not a directory): " + outputDir);
            System.exit(2);
        }

        if (!outputDir.canWrite())
        {
            System.err.print("Invalid output dir (no write permissions): " + outputDir);
            System.exit(3);
        }

        return outputDir;
    }

    StressProfile getStressProfile()
    {
        try
        {
            File yamlFile = new File(profile);
            return StressProfile.load(yamlFile.exists() ? yamlFile.toURI() : URI.create(profile));
        }
        catch ( IOError e)
        {
            e.printStackTrace();
            System.err.print("Invalid profile URI : " + profile);
            System.exit(4);
        }

        return null;
    }

    public abstract void run();


    @Command(name = "compact", description = "compact data in directory")
    public static class Compaction extends CompactionStress
    {

        @Option(name = {"-m", "--maximal"}, description = "force maximal compaction (default true)")
        Boolean maximal = false;

        @Option(name = {"-c", "--compactors-threads"}, description = "number of compactor threads to use for bg compactions (default 4)")
        Integer threads = 4;

        @Option(name = {"-v", "--vnodes"}, description = "generate local tokens (default: false)")
        boolean generateTokens = false;


        public void run()
        {
            Util.initDatabaseDescriptor();
            File outputDir = getDataDir();

            if (generateTokens)
                generateTokens(StorageService.instance.getTokenMetadata(), DatabaseDescriptor.getNumTokens());

            StressProfile stressProfile = getStressProfile();

            if (Schema.instance.getKSMetaData(stressProfile.keyspaceName) == null)
                Schema.instance.load(KeyspaceMetadata.create(stressProfile.keyspaceName, KeyspaceParams.simple(1)));

            CFMetaData metadata = stressProfile.getCFMetadata();
            Directories directories = new Directories(metadata, new Directories.DataDirectory[]{ new Directories.DataDirectory(outputDir) });

            Keyspace.setInitialized();
            Keyspace ks = Keyspace.open(stressProfile.keyspaceName);
            ColumnFamilyStore cfs = ColumnFamilyStore.createColumnFamilyStore(ks, stressProfile.tableName, metadata, directories, false);

            Directories.SSTableLister lister = cfs.getDirectories().sstableLister(Directories.OnTxnErr.IGNORE).skipTemporary(true);
            List<SSTableReader> sstables = new ArrayList<>();


            //Offline open sstables
            for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet())
            {
                Set<Component> components = entry.getValue();
                if (!components.contains(Component.DATA))
                    continue;

                try
                {
                    SSTableReader sstable = SSTableReader.openNoValidation(entry.getKey(), components, cfs);
                    sstables.add(sstable);
                }
                catch (Exception e)
                {
                    JVMStabilityInspector.inspectThrowable(e);
                    System.err.println(String.format("Error Loading %s: %s", entry.getKey(), e.getMessage()));
                }
            }

            //Register with cfs
            cfs.addSSTables(sstables);
            cfs.getCompactionStrategyManager().compactionLogger.enable();

            //Setup
            CompactionManager.instance.setMaximumCompactorThreads(threads);
            CompactionManager.instance.setCoreCompactorThreads(threads);
            CompactionManager.instance.setRate(0);

            List<Future<?>> futures = new ArrayList<>();
            if (maximal)
            {
                cfs.disableAutoCompaction();
                futures = CompactionManager.instance.submitMaximal(cfs, FBUtilities.nowInSeconds(), false);
            }
            else
            {
                cfs.getCompactionStrategyManager().enable();
                for (int i = 0; i < threads; i++)
                    futures.addAll(CompactionManager.instance.submitBackground(cfs));
            }

            long working;
            //Report compaction stats while working
            while ((working = futures.stream().filter(f -> !f.isDone()).count()) > 0 || CompactionManager.instance.getActiveCompactions() > 0 || cfs.getCompactionStrategyManager().getEstimatedRemainingTasks() > 0)
            {
                //Re-up any bg jobs
                if (!maximal)
                {
                    for (long i = working; i < threads; i++)
                        futures.addAll(CompactionManager.instance.submitBackground(cfs));
                }

                reportCompactionStats();
                Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
            }

            System.out.println("Finished! Shutting down...");
            CompactionManager.instance.forceShutdown();

            //Wait for cleanup to finish before forcing
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            LifecycleTransaction.removeUnfinishedLeftovers(cfs);
        }

        private void generateTokens(TokenMetadata tokenMetadata, Integer numTokens)
        {
            IPartitioner p = tokenMetadata.partitioner;
            tokenMetadata.clearUnsafe();
            for (int i = 1; i <= numTokens; i++)
            {
                InetAddress addr = FBUtilities.getBroadcastAddress();
                List<Token> tokens = Lists.newArrayListWithCapacity(numTokens);
                for (int j = 0; j < numTokens; ++j)
                    tokens.add(p.getRandomToken());

                tokenMetadata.updateNormalTokens(tokens, addr);
            }
        }
    }

    void reportCompactionStats()
    {
        System.out.println("========");
        System.out.println(String.format("Pending compactions: %d\n", CompactionManager.instance.getPendingTasks()));
        CompactionStats.reportCompactionTable(CompactionManager.instance.getCompactions(), 0, true);
    }


    @Command(name = "write", description = "write data directly to disk")
    public static class DataWriter extends CompactionStress
    {
        @Option(name = { "-g", "--gbsize"}, description = "Total GB size on disk you wish to write", required = true)
        Integer totalSizeGb;

        @Option(name = { "-t", "--threads" }, description = "Number of threads (default 2)")
        Integer threads = 2;

        @Option(name = { "-c", "--partition-count"}, description = "Number of partitions to loop over (default 1000000)")
        Integer partitions = 1000000;

        @Option(name = { "-b", "--buffer-size-mb"}, description = "Buffer in MB writes before writing new sstable (default 128)")
        Integer bufferSize = 128;

        public void run()
        {
            File outputDir = getDataDir();
            StressProfile stressProfile = getStressProfile();

            StressSettings settings = StressSettings.parse(new String[]{ "write", "-pop seq=1.." + partitions });
            SeedManager seedManager = new SeedManager(settings);
            PartitionGenerator generator = stressProfile.getOfflineGenerator();
            WorkManager workManager = new WorkManager.FixedWorkManager(Long.MAX_VALUE);

            ExecutorService executorService = Executors.newFixedThreadPool(threads);
            CountDownLatch finished = new CountDownLatch(threads);

            CFMetaData metadata = stressProfile.getCFMetadata();
            Directories directories = new Directories(metadata, new Directories.DataDirectory[]{ new Directories.DataDirectory(outputDir) });


            for (int i = 0; i < threads; i++)
            {
                SchemaInsert insert = stressProfile.getOfflineInsert(null, generator, seedManager, settings);

                final CQLSSTableWriter tableWriter = insert.createWriter(directories.getDirectoryForNewSSTables(), bufferSize);
                executorService.submit(() -> {
                    try
                    {
                        insert.runOffline(tableWriter, workManager);
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                    }
                    finally
                    {
                        FileUtils.closeQuietly(tableWriter);
                        finished.countDown();
                    }
                });
            }

            double currentSizeGB;
            while ((currentSizeGB = ((double) FileUtils.folderSize(outputDir)) / (1024 * 1014 * 1024)) < totalSizeGb)
            {
                if (finished.getCount() == 0)
                    break;

                System.out.println(String.format("Written %.2fGB of %dGB", currentSizeGB, totalSizeGb));

                Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);
            }

            workManager.stop();
            Uninterruptibles.awaitUninterruptibly(finished);

            currentSizeGB = ((double) FileUtils.folderSize(outputDir)) / (1024 * 1024 * 1024);
            System.out.println(String.format("Finished writing %.2fGB", currentSizeGB));
        }
    }

    public static void main(String[] args)
    {
        Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder("compaction-stress")
                                           .withDescription("benchmark for compaction")
                                           .withDefaultCommand(Help.class)
                                           .withCommands(Help.class, DataWriter.class, Compaction.class);

        Cli<Runnable> stress = builder.build();

        try
        {
            stress.parse(args).run();
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            System.exit(6);
        }

        System.exit(0);
    }
}


