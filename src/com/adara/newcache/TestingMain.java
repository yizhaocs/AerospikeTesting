package com.adara.newcache;

import com.adara.newcache.aerospikecode.AerospikeConnector;
import com.adara.newcache.aerospikecode.AerospikeOperation;
import com.adara.newcache.aerospikecode.OldCode;
import com.adara.newcache.aerospikecode.Operations.Write;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.opinmind.ssc.KeyValueTs;
import com.adara.newcache.udcuv2code.ProcessCkvData;

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

    static Map<String, Map<Integer,KeyValueTs>> map = new HashMap<String, Map<Integer, KeyValueTs>>();


    static String database = "database1"; // schema/database
    static String table = "set1"; // set
    static String columnName1 = "cookieId";
    static String columnName2 = "ckvMap";
    public static void main(String[] args) throws Exception{
        ProcessCkvData.readThenWrite(map, "/Users/yzhao/IdeaProjects/AerospikeTesting/src/resources/20170712-004428.ps101-lax1.0000000000010309020.csv");
        System.out.println(map.size());
        AerospikeClient client = AerospikeConnector.getInstance();
        AerospikeOperation aerospikeOperation = new AerospikeOperation();
        for(String cookieId: map.keySet()) {
            Key key = new Key(database, table, table);
            Bin bin1 = new Bin(columnName1, cookieId);
            Bin bin2 = new Bin(columnName2, map.get(cookieId));
            Write write = new Write();
            write.writingMultipleValues(client, new WritePolicy(), key, bin1, bin2);
        }

        client.close();

    }

    public static void writeToAerospkie(){
        OldCode.connection(map, "106879103115"); // duration:2824 milliseconds ,with count:2154, 1.31104921 ms per request
    }

}
