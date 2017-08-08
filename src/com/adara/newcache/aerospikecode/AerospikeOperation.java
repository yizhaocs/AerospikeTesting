package com.adara.newcache.aerospikecode;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

/**
 * Created by yzhao on 8/8/17.
 */
public class AerospikeOperation {
    public void put(AerospikeClient client, WritePolicy writePolicy, Key key, Bin... bins) throws Exception{
        client.put(writePolicy, key, bins);
    }


    public Record getAll(AerospikeClient client, Policy policy, Key key){
        Record record = client.get(policy, key);
        return record;
    }

    public void delete(AerospikeClient client, WritePolicy writePolicy, Key key){
        client.delete(writePolicy, key);
    }
}