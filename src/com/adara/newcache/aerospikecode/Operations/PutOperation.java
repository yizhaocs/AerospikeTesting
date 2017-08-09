package com.adara.newcache.aerospikecode.Operations;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;

/**
 * Created by yzhao on 8/8/17.
 */
public class PutOperation {
    /**
     * Writing Single Value
     *
     * @param client
     * @param writePolicy
     * @param row
     * @param column
     * @throws Exception
     */
    public void writingSingleValue(AerospikeClient client, WritePolicy writePolicy, Key row, Bin column) throws Exception{
        client.put(writePolicy, row, column);
    }

    /**
     * Writing Multiple Values
     *
     * @param client
     * @param writePolicy
     * @param row
     * @param columns
     * @throws Exception
     */
    public void writingMultipleValues(AerospikeClient client, WritePolicy writePolicy, Key row, Bin ... columns) throws Exception{
        client.put(writePolicy, row, columns);
    }
}
