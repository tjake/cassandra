
package org.apache.cassandra.db.index.global;

import java.net.InetAddress;
import java.util.List;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class Utils
{
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
