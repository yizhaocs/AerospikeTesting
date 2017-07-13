import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.opinmind.bidgen.CookieRouter;
import com.opinmind.ssc.CookieData;
import com.opinmind.ssc.cache.RemoteUserDataCacheImplV3;
import com.opinmind.ssc.cache.UserDataCacheFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * build it:
 * mvn clean package
 * scp /Users/yzhao/IdeaProjects/ENG835_Backfill/target/Backfill-jar-with-dependencies.jar
 * run it:
 * /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar
 *
 */
public class Main {
    public static void main(String[] args){
        //String cookieId = args[0];
        connection("107391385230");
    }


    public static void connection(String cookieId){
        ClientPolicy policy = null;
        AerospikeClient client = null;

        try{
            policy = new ClientPolicy();
            client = new AerospikeClient(policy, "172.28.128.3", 3000);
            getPutOperations_test(client);
            //getPutOperations_adara(client);
            getPutOperations_adara_prod(client,cookieId);
        }finally {
            if(client != null){
                client.close();
            }
        }
    }


    public static void getPutOperations_test(AerospikeClient client){
        Key key = new Key("test", "CookieData", "putgetkey");
        // Key key = new Key("adara", "CookieData", "putgetkey");
        Bin bin1 = new Bin("bin1", "value1");
        Bin bin2 = new Bin("bin2", "value2");

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
        RemoteUserDataCacheImplV3 userDataCacheBDB = null;

        try {
            userDataCacheBDB = getUserDataCacheBDB();
        }catch(Exception e){
            System.out.println();
            e.printStackTrace();
        }
        CookieData mCookieData = null;

        try {
            mCookieData = userDataCacheBDB.getCookieRawData(Long.valueOf(cookieId));
        }catch(Exception e){
            System.out.println("[getPutOperations_adara_prod error]");
            e.printStackTrace();
        }

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

    private static RemoteUserDataCacheImplV3 getUserDataCacheBDB()
            throws NumberFormatException, Exception {

        CookieRouter cookieRouter = new CookieRouter();
        cookieRouter.setNodes("localhost:8080");

        List<String> configFileList = new ArrayList<String>();
        configFileList.add("/opt/opinmind/conf/common.properties");
        configFileList.add("/opt/opinmind/conf/local.properties");
        configFileList.add("/opt/opinmind/conf/bidgen.nodes.properties");
        configFileList.add("/opt/opinmind/conf/credentials/passwords.properties");
        cookieRouter.setConfigFile(configFileList);
        cookieRouter.init();


        int maxItemListLength = 10000000;
        String maxItemListLengthStr = "100000";
        if (maxItemListLengthStr != null && maxItemListLengthStr.length() > 0) {
            maxItemListLength = Integer.valueOf(maxItemListLengthStr);
        }

        RemoteUserDataCacheImplV3 userDataCache = null;

        userDataCache = (RemoteUserDataCacheImplV3) UserDataCacheFactory
                .createRemoteUserDataCache(cookieRouter,
                        maxItemListLength, true);
        userDataCache.setBidgenCacheNodes("localhost:8080");
        userDataCache.init();

        return userDataCache;
    }
}
