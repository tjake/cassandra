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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.index.global;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public final class GlobalIndexUtils
{
    private GlobalIndexUtils()
    {
    }

    public static InetAddress getIndexNaturalEndpoint(String keyspaceName, Token dataToken, Token indexToken)
    {
        List<InetAddress> dataNaturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspaceName, dataToken);
        List<InetAddress> indexNaturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspaceName, indexToken);

        int dataIdx = dataNaturalEndpoints.indexOf(FBUtilities.getBroadcastAddress());
        if (dataIdx < 0)
            throw new RuntimeException("Trying to get the index natural endpoint on a non-data replica");

        return indexNaturalEndpoints.get(dataIdx);
    }
}
