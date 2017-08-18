package com.adara.newcache.aerospikecode.AerospikeClient.Services;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.opinmind.ssc.cache.*;

import java.util.List;

/**
 * @author yzhao
 */
public interface AerospikeService {
    /**
     * Read Specific Columns/Bins of a Row/Record
     *
     * @param policy
     * @param key
     * @param columnName
     * @return
     * @throws AerospikeException
     */
    public Record getSpecificColumnsForRow(Policy policy, Key key, String... columnName) throws AerospikeException;

    /**
     * Read All Columns/Bins in a Row/Record
     *
     * @param policy
     * @param key
     * @return
     * @throws AerospikeException
     */
    public Record getAllColumnsForRow(Policy policy, Key key) throws AerospikeException;


    /**
     * Writing column/s for a row
     *
     * @param writePolicy
     * @param row
     * @param columns
     * @throws AerospikeException
     */
    public void putColumnForRow(WritePolicy writePolicy, Key row, Bin ... columns) throws AerospikeException;

    /**
     * Writing column/s for a row in Async
     * @param writePolicy
     * @param row
     * @param columns
     * @throws AerospikeException
     */
    public void putColumnForRowInAsync(WritePolicy writePolicy, AerospikeServiceImpl.WriteHandler writeHandler, Key row, Bin ... columns) throws AerospikeException;

    /**
     * @param writePolicy
     * @param row
     * @param columns
     * @throws AerospikeException
     */
    public void addColumnForRow(WritePolicy writePolicy, Key row, Bin ... columns) throws AerospikeException;

    /**
     * To delete a column/bin for a row/key, set the column/bin value to NULL:
     *
     * @param writePolicy
     * @param row
     * @param columnName
     * @throws AerospikeException
     */
    public void deleteSingleColumnForRow(WritePolicy writePolicy, Key row, String columnName) throws AerospikeException;

    /**
     * @param writePolicy
     * @param row
     * @throws AerospikeException
     */
    public void deleteRow(WritePolicy writePolicy, Key row) throws AerospikeException;

    /**
     * This call groups keys based on which Aerospike Server node can best handle the request,
     * and uses a ThreadPool object to concurrently handle each request to each node.
     * After all nodes return the record data, the records are returned to the caller.
     * The array of records returned is in the same order the keys are passed in.
     * If a record is not found in the database, the array entry is null.
     *
     * @param batchPolicy
     * @param rows
     * @return
     * @throws AerospikeException
     */
    public Record[] getForBatchRows(BatchPolicy batchPolicy, Key[] rows) throws AerospikeException;

    /**
     * @param policy
     * @param row
     * @return
     * @throws AerospikeException
     */
    public boolean existsForRow(Policy policy, Key row) throws AerospikeException;

    /**
     * @param queryPolicy
     * @param stmt
     * @return
     * @throws AerospikeException
     */
    public RecordSet query(QueryPolicy queryPolicy, Statement stmt) throws AerospikeException;

    /**
     * @param rs
     * @return
     * @throws AerospikeException
     */
    public List<AerospikeServiceImpl.KeyRecordPair> readRecordSet(RecordSet rs) throws AerospikeException;

    /**
     * Creates a secondary index, secondary indexes are created asynchronously, so the method returns before the secondary index propagates to the cluster.
     * As an option, the client can wait for the asynchronous server task to complete.
     * Secondary indexes can only be created once on the server as a combination of Namespace, set, and bin name with either integer or string data types.
     * For example, if you define a secondary index to index bin x that contains integer values, then only records containing bins named x with integer values are indexed.
     * Other records with a bin named x that contain non-integer values are not indexed.
     * When an index management call is made to any node in the Aerospike Server cluster, the information automatically propagates to the remaining nodes.
     * Example:
     * The following example creates an index idx_foo_bar_baz and waits for index creation to complete. The index is in Namespace foo within set bar and bin baz:
     * IndexTask task = client.createIndex(null, "foo", "bar", "idx_foo_bar_baz", "baz", IndexType.NUMERIC);
     * task.waitTillComplete();
     * @param policy
     * @param namespace
     * @param setName
     * @param indexName
     * @param binName
     * @param indexType
     * @throws AerospikeException
     */
    public void createIndex(Policy policy, String namespace, String setName, String indexName, String binName, IndexType indexType) throws AerospikeException;

    /**
     * Drops a secondary index.
     * @param policy
     * @param namespace
     * @param setName
     * @param indexName
     * @throws AerospikeException
     */
    public void dropIndex(Policy policy, String namespace, String setName, String indexName) throws AerospikeException;
}
