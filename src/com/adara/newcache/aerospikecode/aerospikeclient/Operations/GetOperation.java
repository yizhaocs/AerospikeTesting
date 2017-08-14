package com.adara.newcache.aerospikecode.AerospikeClient.Operations;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;

/**
 * Created by yzhao on 8/8/17.
 */
public class GetOperation {
    /**
     * Read Specific Columns/Bins of a Row/Record
     *
     * @param client
     * @param policy
     * @param key
     * @param columnName
     * @return
     */
    public Record getSpecificColumnsOfaRow(AerospikeClient client, Policy policy, Key key, String... columnName){
        Record row = client.get(policy, key, columnName);
        return row;
    }

    /**
     * Read All Columns/Bins in a Row/Record
     *
     * @param client
     * @param policy
     * @param key
     * @return
     */
    public Record getRow(AerospikeClient client, Policy policy, Key key){
        Record row = client.get(policy, key);
        return row;
    }
}
