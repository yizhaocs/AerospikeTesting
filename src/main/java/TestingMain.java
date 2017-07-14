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
        connection("106879103115");

    }


    public static void connection(String cookieId){
        ClientPolicy policy = null;
        AerospikeClient client = null;
        WritePolicy wp = null;
        //WritePolicy writePolicy = new WritePolicy();

        try{
            policy = new ClientPolicy();
            policy.timeout = 50000;

            // Host[] multipleHost = new Host[]{new Host("172.28.128.3", 3000), new Host("172.28.128.4", 3000)};
            Host[] multipleHost = new Host[]{new Host("172.28.128.10", 3000)}; // ,new Host("172.28.128.11", 3000)
            client = new AerospikeClient(policy, multipleHost);
            //getPutOperations_test(client);
            //getPutOperations_adara(client);
            //getPutOperations_adara_prod(client,cookieId);
            ProcessCkvData.readThenWrite(map, policy, client, WritePolicyHelp.wp, "/Users/yzhao/Desktop/20170712-004428.ps101-lax1.0000000000010309020.csv");
            writeToAerospike(policy, client, wp);
        }finally {
            if(client != null){
                client.close();
            }
        }
    }


    public static void getPutOperations_test(AerospikeClient client){
        Key key = new Key("test", "table1", "mykey1");
        // Key key = new Key("adara", "CookieData", "putgetkey");
        Bin bin1 = new Bin("column1", "value1");
        Bin bin2 = new Bin("column2", "value2");

        client.put(null, key, bin1, bin2);
        Record record = client.get(null, key);
        System.out.println(record.toString());
    }

    public static void getPutOperations_adara(AerospikeClient client){
        Key key = new Key("adara", "CookieData", "putgetkey");

        CookieData mCookieData = new CookieData();
        Bin bin1 = new Bin("bin1", mCookieData);
        Bin bin2 = new Bin("bin2", mCookieData);

        client.put(null, key, bin1, bin2);
        Record record = client.get(null, key);
        System.out.println(record.toString());
    }

    public static void getPutOperations_adara_prod(AerospikeClient client, String cookieId){
        Key key = new Key("adara", "CookieData", "putgetkey");

        CookieData mCookieData = ReadBidgen.getCookieDataFromCookieId(cookieId);
        if(mCookieData != null){
            Bin bin1 = new Bin("bin1", mCookieData);
            Bin bin2 = new Bin("bin2", mCookieData);

            client.put(null, key, bin1, bin2);
            Record record = client.get(null, key);
            CookieData ck = (CookieData)record.bins.get("bin1");
            System.out.println("record from aerospike: " + record.toString());
        }else{
            System.out.println("CookieData is null");
        }
    }





    public static void writeToAerospike(ClientPolicy policy, AerospikeClient client, WritePolicy wp){

        long startTime = System.nanoTime();

        int count = 0;
        for(String cookieId: map.keySet()){
            Key key = new Key("test", "table15", cookieId);
            Bin column1 = new Bin("cookieId", cookieId);
            Bin column2 = new Bin("ckvMap", map.get(cookieId));
            //Record r = client.get(null,key);
            //if(r!= null && !r.bins.containsKey(cookieId)) {
            if(!client.exists(null, key)) {
                // System.out.println(cookieId);
                try {
/*                        EventLoop eventLoop = EventLoopsHelp.eventLoops.get(0);
                        WriteListener listener = new WriteListener() {
                            @Override
                            public void onSuccess(Key key) {

                            }

                            @Override
                            public void onFailure(AerospikeException e) {

                            }
                        };*/
                    // client.put(eventLoop, listener, wp, key, column2);
                    client.put(wp, key, column1, column2);
                    count ++;
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        }
        long endTime = System.nanoTime();

        long duration = (endTime - startTime)/1000000; // in milliseconds
        System.out.println("duration:" + duration + " milliseconds ,with count:" + count); // duration:2824 milliseconds ,with count:2154
    }
}
