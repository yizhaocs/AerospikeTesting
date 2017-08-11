package com.adara.newcache.aerospikecode.AerospikeClient;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.policy.ClientPolicy;

/**
 * Created by yzhao on 8/8/17.
 */
public class AerospikeConnector {
    private static final ClientPolicy policy = new ClientPolicy();
    private static Host[] multipleHost = new Host[]{new Host("localhost", 3000)};
    private static final AerospikeClient instance =  new AerospikeClient(policy, multipleHost);

    protected AerospikeConnector() {
    }

    // Runtime initialization
    // By defualt ThreadSafe
    public static AerospikeClient getInstance() {
        return instance;
    }
}
