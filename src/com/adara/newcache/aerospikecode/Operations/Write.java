package com.adara.newcache.aerospikecode.Operations;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;

/**
 * Created by yzhao on 8/8/17.
 */
public class Write {
    /**
     * Writing Single Value
     *
     * @param client
     * @param writePolicy
     * @param key
     * @param column
     * @throws Exception
     */
    public void writingSingleValue(AerospikeClient client, WritePolicy writePolicy, Key key, Bin column) throws Exception{
        client.put(writePolicy, key, column);
    }

    /**
     * Writing Multiple Values
     *
     * @param client
     * @param writePolicy
     * @param key
     * @param columns
     * @throws Exception
     */
    public void writingMultipleValues(AerospikeClient client, WritePolicy writePolicy, Key key, Bin ... columns) throws Exception{
        client.put(writePolicy, key, columns);
    }
}
