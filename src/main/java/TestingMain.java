package main.java;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.opinmind.ssc.CookieData;
import com.opinmind.ssc.KeyValueTs;
import main.java.aerospike.AerospikeConnection;
import main.java.aerospike.WritePolicyHelp;
import main.java.udcuv2.ProcessCkvData;
import main.java.udcuv2.ReadBidgen;

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
public class TestingMain {

    static Map<String, Map<Integer,KeyValueTs>> map = new HashMap<String, Map<Integer, KeyValueTs>>();


    public static void main(String[] args){
        //String cookieId = args[0];
        ProcessCkvData.readThenWrite(map, "/Users/yzhao/Desktop/20170712-004428.ps101-lax1.0000000000010309020.csv");
        AerospikeConnection.connection(map, "106879103115");

    }



}
