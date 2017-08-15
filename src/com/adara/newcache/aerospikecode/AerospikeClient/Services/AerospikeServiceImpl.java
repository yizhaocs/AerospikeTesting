package com.adara.newcache.aerospikecode.AerospikeClient.Services;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.EventLoop;
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
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.ConnectException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author yzhao
 */
public class AerospikeServiceImpl implements AerospikeService {
    private static final Logger log = Logger.getLogger(AerospikeServiceImpl.class);

    private AerospikeClient client;
    private List<String> hostList;
    private int socketTimeoutForReading;
    private int totalTimeoutForReading;
    private int sleepBetweenRetriesForReading;
    private int socketTimeoutForWriting;
    private int totalTimeoutForWriting;
    private int sleepBetweenRetriesForWriting;
    private int maxRetryForWriting;
    private int maxRetryForReading;

//    private StrTokenizer st = StrTokenizer.getCSVInstance();

    private ClientPolicy clientPolicyDefault;
    private Policy readPolicyDefault;
    private WritePolicy writePolicyDefault;
    private ScanPolicy scanPolicyDefault;
    private QueryPolicy queryPolicyDefault;
    private BatchPolicy batchPolicyDefault;
    private InfoPolicy infoPolicyDefault;

    /**
     * init the AerospikeClient and polocies
     */
    public void init() {
//        try {
            policiesInit();

//            if (hostList != null && hostList.size() > 0) {
//                Host[] hosts = new Host[hostList.size()];
//                int i = 0;
//                for (String ipPort : hostList) {
//                    st.setDelimiterString(":");
//                    String[] result = st.reset(ipPort).getTokenArray(); // ipPort = 192.168.1.114:123
//                    hosts[i] = new Host(result[0], Integer.valueOf(result[1]));
//                    i++;
//                }
                client = new AerospikeClient(clientPolicyDefault, new Host("localhost", 3000));
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
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                log.error("[AerospikeServiceImpl.client]: ", e);
            }
        }
    }

    private void policiesInit(){
        this.clientPolicyDefault =  new ClientPolicy();
        this.clientPolicyDefault.readPolicyDefault.socketTimeout = socketTimeoutForReading;
        this.clientPolicyDefault.readPolicyDefault.totalTimeout = totalTimeoutForReading;
        this.clientPolicyDefault.readPolicyDefault.sleepBetweenRetries = sleepBetweenRetriesForReading;
        this.clientPolicyDefault.writePolicyDefault.socketTimeout = socketTimeoutForWriting;
        this.clientPolicyDefault.writePolicyDefault.totalTimeout = totalTimeoutForWriting;
        this.clientPolicyDefault.writePolicyDefault.sleepBetweenRetries = sleepBetweenRetriesForWriting;

        this.readPolicyDefault = new Policy();
        this.writePolicyDefault = new WritePolicy();
        this.scanPolicyDefault = new ScanPolicy();
        this.queryPolicyDefault = new QueryPolicy();
        this.batchPolicyDefault = new BatchPolicy();
        this.infoPolicyDefault = new InfoPolicy();
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
     * Writing column/s for a row as in Async Task
     * @param eventLoop
     * @param writePolicy
     * @param row
     * @param columns
     * @throws AerospikeException
     */
    public void putColumnForRowInAsyncTask(EventLoop eventLoop, WritePolicy writePolicy, Key row, Bin ... columns) throws AerospikeException{
        client.put(eventLoop, new WriteHandler(client, eventLoop,writePolicy, row, columns), writePolicy, row, columns);
    }

    /**
     * @param writePolicy
     * @param row
     * @param column
     * @throws AerospikeException
     */
    public void addSingleColumnForRow(WritePolicy writePolicy, Key row, Bin column) throws AerospikeException {
        if (writePolicy == null) {
            writePolicy = writePolicyDefault;
        }

        client.add(writePolicy, row, column);
    }

    /**
     * @param writePolicy
     * @param row
     * @param columns
     * @throws AerospikeException
     */
    public void addMultipleColumnForRow(WritePolicy writePolicy, Key row, Bin... columns) throws AerospikeException {
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
     * @param maxRetryForReading
     */
    public void setMaxRetryForReading(int maxRetryForReading) {
        this.maxRetryForReading = maxRetryForReading;
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

    private class ReadHandler implements RecordListener {
        private final AerospikeClient client;
        private final EventLoop eventLoop;
        private final Policy policy;
        private final Key key;
        private final Bin bin;
        private int failCount = 0;

        public ReadHandler(AerospikeClient client, EventLoop eventLoop, Policy policy, Key key, Bin bin) {
            this.client = client;
            this.eventLoop = eventLoop;
            this.policy = policy;
            this.key = key;
            this.bin = bin;
        }

        // Read success callback.
        public void onSuccess(Key key, Record record) {
        }

        // Error callback.
        public void onFailure(AerospikeException e) {
            // Retry up to 2 more times.
            if (failCount++ <= maxRetryForReading) {
                Throwable t = e.getCause();

                // Check for common socket errors.
                if (t != null && (t instanceof ConnectException || t instanceof IOException)) {
                    try {
                        // pass "this" to the second argument of RecordListener to avoid failCount doesn't increment to run into stackoverflow
                        client.get(eventLoop, this, policy, key);
                        return;
                    }
                    catch (Exception ex) {
                        throw e;
                    }
                }
            }
        }
    }


    private class WriteHandler implements WriteListener {
        private final AerospikeClient client;
        private final EventLoop eventLoop;
        private final WritePolicy policy;
        private final Key key;
        private final Bin[] bins;
        private int failCount = 0;

        public WriteHandler(AerospikeClient client, EventLoop eventLoop, WritePolicy policy, Key key, Bin ... bins) {
            this.client = client;
            this.eventLoop = eventLoop;
            this.policy = policy;
            this.key = key;
            this.bins = bins;
        }

        // Write success callback.
        public void onSuccess(Key key) {

        }

        // Error callback.
        public void onFailure(AerospikeException e) {
            if (failCount++ <= maxRetryForWriting) {
                Throwable t = e.getCause();

                // Check for common socket errors.
                if (t != null && (t instanceof ConnectException || t instanceof IOException)) {
                    log.info("[AerospikeServiceImpl.WriteHandler.onFailure] Retrying put: " + key.userKey);
                    try {
                        // pass "this" to the second argument of WriteListener to avoid failCount doesn't increment to run into stackoverflow
                        client.put(eventLoop, this, policy, key, bins);
                        return;
                    } catch (Exception ex) {
                        throw e;
                    }
                }
            }
        }
    }
}