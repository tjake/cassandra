
package org.apache.cassandra.db.index.global;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.OrderPreservingPartitioner.StringToken;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.PropertyFileSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class UtilsTest
{
    @BeforeClass
    public static void setUp() throws ConfigurationException
    {
        IEndpointSnitch snitch = new PropertyFileSnitch();
        DatabaseDescriptor.setEndpointSnitch(snitch);
        Keyspace.setInitialized();
    }

    @Test
    public void testGetIndexNaturalEndpoint() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        // DC1
        metadata.updateNormalToken(new StringToken("A"), InetAddress.getByName("127.0.0.1"));
        metadata.updateNormalToken(new StringToken("C"), InetAddress.getByName("127.0.0.2"));

        // DC2
        metadata.updateNormalToken(new StringToken("B"), InetAddress.getByName("127.0.0.4"));
        metadata.updateNormalToken(new StringToken("D"), InetAddress.getByName("127.0.0.5"));

        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC1", "1");
        configOptions.put("DC2", "1");

        Keyspace.clear("Keyspace1");
        KSMetaData meta = KSMetaData.newKeyspace("Keyspace1", "NetworkTopologyStrategy", configOptions, false);
        Schema.instance.setKeyspaceDefinition(meta);

        InetAddress naturalEndpoint = Utils.getIndexNaturalEndpoint("Keyspace1",
                                                                    new StringToken("CA"),
                                                                    new StringToken("BB"));

        assert naturalEndpoint.equals(InetAddress.getByName("127.0.0.5"));
    }
}
