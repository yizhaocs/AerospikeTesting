package com.adara.newcache.aerospikecode.AerospikeClient.Services;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.async.AsyncClientPolicy;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.MaxCommandAction;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.opinmind.clog.data.Event;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.lang.text.StrTokenizer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.ConnectException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * @author yzhao
 */
public class AerospikeServiceImpl implements AerospikeService {
    private static final Logger log = Logger.getLogger(AerospikeServiceImpl.class);

    private AerospikeClient client;
    private List<String> hostList;
    private EventLoops eventLoops;
    private int socketTimeoutForReading;
    private int totalTimeoutForReading;
    private int sleepBetweenRetriesForReading;
    private int socketTimeoutForWriting;
    private int totalTimeoutForWriting;
    private int sleepBetweenRetriesForWriting;
    private int writeTimeout;
    private int maxRetryForWriting;

    private int concurrentMax;

    private StrTokenizer st = StrTokenizer.getCSVInstance();

    private ClientPolicy clientPolicyDefault = new ClientPolicy();
    private Policy readPolicyDefault = new Policy();
    private WritePolicy writePolicyDefault = new WritePolicy();
    private ScanPolicy scanPolicyDefault = new ScanPolicy();
    private QueryPolicy queryPolicyDefault = new QueryPolicy();
    private BatchPolicy batchPolicyDefault = new BatchPolicy();
    private InfoPolicy infoPolicyDefault = new InfoPolicy();
    private EventPolicy eventPolicy = new EventPolicy();

    /**
     * init the AerospikeClient and polocies
     */
    public void init() {
            int eventLoopSize = Runtime.getRuntime().availableProcessors(); // Allocate an event loop for each cpu core.
            eventPolicy.minTimeout = writeTimeout;
            eventLoops = new NioEventLoops(eventPolicy, eventLoopSize); // Direct NIO
            concurrentMax = eventLoopSize * 40; // Allow 40 concurrent commands per event loop.
            policiesInit();



        client = new AerospikeClient(clientPolicyDefault, new Host("localhost", 3000));
        System.out.println("eventLoops.getArray().length:" + eventLoops.getArray().length);
//            } else {
//                log.error("[AerospikeServiceImpl.init]:  hostList is null/empty");
//            }
//        } catch (Exception e) {
//            log.error("[AerospikeServiceImpl.init]: ", e);
//        }
    }

    /**
     * close for entire cluster
     * When all transactions complete and the application is prepared for a clean shutdown, call the close() method to remove resources held by the AerospikeClient object.
     */
    public void destroy() {
        try {
            if (client != null) {
                client.close();
            }
        } catch (Exception e) {
            log.error("[AerospikeServiceImpl.destroy]: ", e);
        } finally {
            try {
                if (eventLoops != null) {
                    eventLoops.close();
                }
            } catch (Exception e) {
                log.error("[AerospikeServiceImpl.destroy]: ", e);
            }
        }
    }

    private void policiesInit(){
        this.clientPolicyDefault.eventLoops = eventLoops;
        this.clientPolicyDefault.maxConnsPerNode = concurrentMax;

        this.clientPolicyDefault.readPolicyDefault.socketTimeout = socketTimeoutForReading;
        this.clientPolicyDefault.readPolicyDefault.totalTimeout = totalTimeoutForReading;
        this.clientPolicyDefault.readPolicyDefault.sleepBetweenRetries = sleepBetweenRetriesForReading;
        this.clientPolicyDefault.writePolicyDefault.socketTimeout = socketTimeoutForWriting;
        this.clientPolicyDefault.writePolicyDefault.totalTimeout = totalTimeoutForWriting;
        this.clientPolicyDefault.writePolicyDefault.sleepBetweenRetries = sleepBetweenRetriesForWriting;
        this.clientPolicyDefault.writePolicyDefault.setTimeout(writeTimeout);

    }

    /**
     * Read Specific Columns/Bins of a Row/Record
     * @param policy
     * @param key
     * @param columnName
     * @return
     * @throws AerospikeException
     */
    public Record getSpecificColumnsForRow(Policy policy, Key key, String... columnName) throws AerospikeException{
        if (policy == null) {
            policy = readPolicyDefault;
        }

        Record row = client.get(policy, key, columnName);
        return row;
    }



    /**
     * Read All Columns/Bins in a Row/Record
     * @param policy
     * @param key
     * @return
     * @throws AerospikeException
     */
    public Record getAllColumnsForRow(Policy policy, Key key) throws AerospikeException{
        if (policy == null) {
            policy = readPolicyDefault;
        }

        Record row = client.get(policy, key);
        return row;
    }


    /**
     * Writing column/s for a row
     *
     * @param writePolicy
     * @param row
     * @param columns
     * @throws AerospikeException
     */
    public void putColumnForRow(WritePolicy writePolicy, Key row, Bin ... columns) throws AerospikeException {
        if (writePolicy == null) {
            writePolicy = writePolicyDefault;
        }

        client.put(writePolicy, row, columns);
    }

    /**
     * Writing column/s for a row in Async
     * @param writePolicy
     * @param row
     * @param columns
     * @throws AerospikeException
     */
    public void putColumnForRowInAsync(WritePolicy writePolicy, WriteHandler writeHandler,Key row, Bin ... columns) throws AerospikeException {
        EventLoop eventloop = eventLoops.next(); // Find an event loop from eventLoops created in connect example.
        client.put(eventloop, writeHandler, writePolicy, row, columns);
    }


    /**
     * @param writePolicy
     * @param row
     * @param columns
     * @throws AerospikeException
     */
    public void addColumnForRow(WritePolicy writePolicy, Key row, Bin ... columns) throws AerospikeException {
        if (writePolicy == null) {
            writePolicy = writePolicyDefault;
        }

        client.add(writePolicy, row, columns);
    }

    /**
     * To delete a column/bin for a row/key, set the column/bin value to NULL:
     *
     * @param writePolicy
     * @param row
     * @param columnName
     * @throws AerospikeException
     */
    public void deleteSingleColumnForRow(WritePolicy writePolicy, Key row, String columnName) throws AerospikeException {
        if (writePolicy == null) {
            writePolicy = writePolicyDefault;
        }

        Bin column = Bin.asNull(columnName); // Set column/bin value to null to drop bin.
        client.put(writePolicy, row, column);
    }


    /**
     * @param writePolicy
     * @param row
     * @throws AerospikeException
     */
    public void deleteRow(WritePolicy writePolicy, Key row) throws AerospikeException {
        if (writePolicy == null) {
            writePolicy = writePolicyDefault;
        }
        client.delete(writePolicy, row);
    }

    /**
     * @param policy
     * @param row
     * @return
     * @throws AerospikeException
     */
    public boolean existsForRow(Policy policy, Key row) throws AerospikeException {
        if (policy == null) {
            policy = readPolicyDefault;
        }
        return client.exists(policy, row);
    }

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
    public Record[] getForBatchRows(BatchPolicy batchPolicy, Key[] rows) throws AerospikeException {
        if (batchPolicy == null) {
            batchPolicy = batchPolicyDefault;
        }

        Record[] records = client.get(batchPolicy, rows);
        return records;
    }

    /**
     *
     * @param queryPolicy
     * @param stmt
     * @return
     * @throws AerospikeException
     */
    public RecordSet query(QueryPolicy queryPolicy, Statement stmt) throws AerospikeException{
        if(queryPolicy == null){
            queryPolicy = queryPolicyDefault;
        }
        RecordSet rs = client.query(queryPolicy, stmt);

        return rs;
    }

    /**
     * @param rs
     * @return
     * @throws AerospikeException
     */
    public List<KeyRecordPair> readRecordSet(RecordSet rs) throws AerospikeException{
        List<KeyRecordPair> keyRecordList = new LinkedList<KeyRecordPair>();
        try {
            while (rs.next()) {
                Key key = rs.getKey();
                Record record = rs.getRecord();
                KeyRecordPair mKeyRecordPair = new KeyRecordPair(key, record);
                keyRecordList.add(mKeyRecordPair);
            }
        } finally {
            rs.close();
        }

        return keyRecordList;
    }

    /**
     * To create a secondary index, secondary indexes are created asynchronously, so the method returns before the secondary index propagates to the cluster.
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
    public void createIndex(Policy policy, String namespace, String setName, String indexName, String binName, IndexType indexType) throws AerospikeException{
        if (policy == null) {
            policy = readPolicyDefault;
        }

        IndexTask task = client.createIndex(policy, namespace, setName, indexName, binName, indexType);
        task.waitTillComplete();
    }


    /**
     * Drops a secondary index.
     * @param policy
     * @param namespace
     * @param setName
     * @param indexName
     * @throws AerospikeException
     */
    public void dropIndex(Policy policy, String namespace, String setName, String indexName) throws AerospikeException{
        if (policy == null) {
            policy = readPolicyDefault;
        }

        client.dropIndex(policy, namespace, setName, indexName);
    }

    /**
     *
     * @param hostList
     */
    public void setHostList(List<String> hostList) {
        this.hostList = hostList;
    }

    /**
     *
     * @param totalTimeoutForReading
     */
    public void setTotalTimeoutForReading(int totalTimeoutForReading) {
        this.totalTimeoutForReading = totalTimeoutForReading;
    }

    /**
     *
     * @param socketTimeoutForReading
     */
    public void setSocketTimeoutForReading(int socketTimeoutForReading) {
        this.socketTimeoutForReading = socketTimeoutForReading;
    }

    /**
     *
     * @param sleepBetweenRetriesForReading
     */
    public void setSleepBetweenRetriesForReading(int sleepBetweenRetriesForReading) {
        this.sleepBetweenRetriesForReading = sleepBetweenRetriesForReading;
    }

    /**
     *
     * @param socketTimeoutForWriting
     */
    public void setSocketTimeoutForWriting(int socketTimeoutForWriting) {
        this.socketTimeoutForWriting = socketTimeoutForWriting;
    }

    /**
     *
     * @param sleepBetweenRetriesForWriting
     */
    public void setSleepBetweenRetriesForWriting(int sleepBetweenRetriesForWriting) {
        this.sleepBetweenRetriesForWriting = sleepBetweenRetriesForWriting;
    }

    /**
     *
     * @param totalTimeoutForWriting
     */
    public void setTotalTimeoutForWriting(int totalTimeoutForWriting) {
        this.totalTimeoutForWriting = totalTimeoutForWriting;
    }

    /**
     *
     * @param maxRetryForWriting
     */
    public void setMaxRetryForWriting(int maxRetryForWriting) {
        this.maxRetryForWriting = maxRetryForWriting;
    }

    /**
     *
     * @param writeTimeout
     */
    public void setWriteTimeout(int writeTimeout) {
        this.writeTimeout = writeTimeout;
    }

    /**
     * used only in readRecordSet method
     */
    protected class KeyRecordPair{
        private Key key;
        private Record record;

        public KeyRecordPair(){

        }

        /**
         *
         * @param key
         * @param record
         */
        public KeyRecordPair(Key key, Record record){
            this.key = key;
            this.record = record;
        }

        /**
         *
         * @return
         */
        public Key getKey() {
            return key;
        }

        /**
         *
         * @param key
         */
        public void setKey(Key key) {
            this.key = key;
        }

        /**
         *
         * @return
         */
        public Record getRecord() {
            return record;
        }

        /**
         *
         * @param record
         */
        public void setRecord(Record record) {
            this.record = record;
        }
    }

    public class WriteHandler implements WriteListener {
        private WritePolicy policy;
        private Key key;
        private Bin[] bins;
        private int failCount = 0;

        public WriteHandler(WritePolicy policy, Key key, Bin ... bins) {
            this.policy = policy;
            this.key = key;
            this.bins = bins;
        }

        // Write success callback.
        public void onSuccess(Key key) {
            // do nothing
        }

        // Error callback.
        public void onFailure(AerospikeException e) {
            if (failCount++ <= maxRetryForWriting) {
                try {
                    // pass "this" to the second argument of WriteHandler to avoid failCount doesn't increment to run into stackoverflow
                    putColumnForRowInAsync(policy, this, key, bins);
                    return;
                } catch (Exception ex) {
                    log.error("[AerospikeServiceImpl.WriteHandler.onFailure]: ", e);
                    throw e;
                }
            } else{
                log.error("[AerospikeServiceImpl.WriteHandler.onFailure]: ", e);
                throw e;
            }
        }
    }
}
