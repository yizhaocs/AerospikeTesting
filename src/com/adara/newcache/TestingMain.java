package com.adara.newcache;

import com.adara.newcache.aerospikecode.AerospikeClient.AerospikeConnector;
import com.adara.newcache.aerospikecode.AerospikeClient.Operations.PutOperation;
import com.adara.newcache.aerospikecode.AerospikeClient.Services.AerospikeServiceImpl;
import com.adara.newcache.udcuv2code.ProcessCkvData;
import com.adara.newcache.utils.UuidGenerator;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.opinmind.ssc.KeyValueTs;

import java.util.HashMap;
import java.util.Map;

/**
 * build it:
 * mvn clean package
 * scp /Users/yzhao/IdeaProjects/ENG835_Backfill/target/Backfill-jar-with-dependencies.jar
 * run it:
 * /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar
 *
 */

/**
 * aerospike: // total time used:2824 milliseconds ,with count:2154, 1.31104921 ms per request
 * big table: // total time used:114714 milliseconds ,with count:2154, 53.2562674 ms per request
 */
public class TestingMain {
    static String database = "database1"; // schema/database
    static String table = "set4"; // set
    static String columnName1 = "id";
    static String columnName2 = "uuid";
    static int start = 0;
    static int end = 10000;


    public static void main(String[] args) throws Exception{
        // testingWithSingleData();
        AerospikeServiceImpl mAerospikeService = new AerospikeServiceImpl();
        mAerospikeService.init();

        Map<String, Map<Integer,KeyValueTs>> map = new HashMap<String, Map<Integer, KeyValueTs>>();
        ProcessCkvData.readThenWrite(map, "/Users/yzhao/IdeaProjects/AerospikeTesting/src/resources/20170712-004428.ps101-lax1.0000000000010309020.csv");
        System.out.println(map.size());
        for(String cookieId: map.keySet()) {
            String userKey = cookieId;
            Key row = new Key(database, table, userKey);
            Bin bin1 = new Bin(columnName1, cookieId);
            Bin bin2 = new Bin(columnName2, map.get(cookieId));
            mAerospikeService.putColumnForRow(null,row, bin1, bin2);
        }

        Thread.sleep(10000);
        mAerospikeService.destroy();


    }

    public static void testingWithSingleData() throws Exception{
        AerospikeClient client = AerospikeConnector.getInstance();
            Key row = new Key(database, table, "1");
            Bin bin1 = new Bin(columnName1, "1");
            Bin bin2 = new Bin(columnName1, "2");
            Bin bin3 = new Bin(columnName2, UuidGenerator.generateRandomUuid());
            PutOperation write = new PutOperation();
            write.writingMultipleValues(client, new WritePolicy(), row, bin1, bin2, bin3);

        client.close();
    }


    public static void testingWithRamdonData() throws Exception{
        AerospikeClient client = AerospikeConnector.getInstance();
        for(int i = start; i < end; i++) {
            Key row = new Key(database, table, "1");
            Bin bin1 = new Bin(columnName1, i);
            Bin bin2 = new Bin(columnName1, UuidGenerator.generateRandomUuid());
            PutOperation write = new PutOperation();
            write.writingMultipleValues(client, new WritePolicy(), row, bin1, bin2);
        }

        client.close();
    }

  /*  public static void testingWithKVmapping() throws Exception{
        Map<String, Map<Integer,KeyValueTs>> map = new HashMap<String, Map<Integer, KeyValueTs>>();
        ProcessCkvData.readThenWrite(map, "/Users/yzhao/IdeaProjects/AerospikeTesting/src/resources/20170712-004428.ps101-lax1.0000000000010309020.csv");
        System.out.println(map.size());
        AerospikeClient client = AerospikeConnector.getInstance();
        for(String cookieId: map.keySet()) {
            Key row = new Key(database, table, table);
            Bin bin1 = new Bin(columnName1, cookieId);
            Bin bin2 = new Bin(columnName2, map.get(cookieId));
            PutOperation write = new PutOperation();
            write.writingMultipleValues(client, new WritePolicy(), row, bin1, bin2);
        }

        client.close();
    }
*/


}
