package com.adara.newcache.aerospikecode.Operations;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;

/**
 * Created by yzhao on 8/8/17.
 */
public class DeleteOperation {
    /**
     * Delete a Row/Record
     * @param client
     * @param writePolicy
     * @param key
     */
    public void deleteSingleRow(AerospikeClient client, WritePolicy writePolicy, Key key){
        client.delete(writePolicy, key);
    }

    /**
     * Deleting a Column/Bin
     * To delete a bin, set the bin value to NULL:
     * @param client
     * @param writePolicy
     * @param key
     * @param binName
     */
    public void deleteSingleColumn(AerospikeClient client, WritePolicy writePolicy, Key key, String binName){
        Bin column = Bin.asNull(binName); // Set bin value to null to drop bin.
        client.put(writePolicy, key, column);
    }
}
