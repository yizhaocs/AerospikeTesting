package com.adara.newcache.aerospikecode;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;

/**
 * Created by yzhao on 8/8/17.
 */
public class AerospikeOperation {
    public void put(AerospikeClient client, Key key, Bin... bins) throws Exception{
        client.put(new WritePolicy(), key, bins);
    }
}