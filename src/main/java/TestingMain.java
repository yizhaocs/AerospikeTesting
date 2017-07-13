package main.java;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.opinmind.bidgen.CookieRouter;
import com.opinmind.common.OpinmindConstants;
import com.opinmind.ssc.CookieData;
import com.opinmind.ssc.KeyValueTs;
import com.opinmind.ssc.cache.RemoteUserDataCacheImplV3;
import com.opinmind.ssc.cache.UserDataCacheFactory;
import com.opinmind.util.RetryUtil;
import org.apache.commons.lang.text.StrTokenizer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

/**
 * build it:
 * mvn clean package
 * scp /Users/yzhao/IdeaProjects/ENG835_Backfill/target/Backfill-jar-with-dependencies.jar
 * run it:
 * /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar
 *
 */
public class TestingMain {
    private static final Pattern amperSpliter = Pattern.compile("&");
    private static final Pattern equalSpliter = Pattern.compile("=");


    public static void main(String[] args){
        //String cookieId = args[0];
        connection("106879103115");
    }


    public static void connection(String cookieId){
        ClientPolicy policy = null;
        AerospikeClient client = null;

        try{
            policy = new ClientPolicy();
            client = new AerospikeClient(policy, "172.28.128.3", 3000);
            //getPutOperations_test(client);
            //getPutOperations_adara(client);
            //getPutOperations_adara_prod(client,cookieId);
            readThenWrite(policy, client, "/Users/yzhao/Desktop/20170712-004428.ps101-lax1.0000000000010309020.csv");
        }finally {
            if(client != null){
                client.close();
            }
        }
    }


    public static void getPutOperations_test(AerospikeClient client){
        Key key = new Key("database1", "table1", "mykey1");
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

    public static void readThenWrite(ClientPolicy policy, AerospikeClient client, String fileInput){
        // Location of file to read
        File file = new File(fileInput);
        Scanner scanner = null;
        try {
            scanner = new Scanner(file);
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                StrTokenizer pipeTokenizer = StrTokenizer.getCSVInstance();
                pipeTokenizer.setDelimiterChar('|');
                String[] data = pipeTokenizer.reset(line).getTokenArray();
                if(data!=null && data.length > 1 && data[0].equals("ckvraw")) {
                    processData(policy, client, data, null, 0, true);
                }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanner.close();
        }
    }





    public static void processData(ClientPolicy policy, AerospikeClient client, String[] data, String fileName, int lineNo, Boolean shouldWriteClog) throws Exception {
        // construct the kvt from file
        // note, according to the contract with the file producer;, the strings are url encoded

        if(data[1].equals("")){
            return;
        }

        final Date timeStamp = new Date(Long.valueOf(data[1])*1000);
        final long cookieId = Long.valueOf(data[2]);
        final String keyValues = data[3];

        String eventIdStr = getFromDataArray(data, 4, false);
        String dpIdStr = getFromDataArray(data, 5, false);
        String locationIdStr = getFromDataArray(data, 7, false);
        String refererUrl = getFromDataArray(data, 8, true);
        String domain = getFromDataArray(data, 9, false);
        String userAgent = getFromDataArray(data, 10, true);

        /*Set<Integer> keysGoToEkv = UDCUHelper.getEkvKeys();
        Set<Integer> keysGoToCkv = UDCUHelper.getCkvKeys();
        Set<Integer> keysGoToBidgen = UDCUHelper.getBidgenKeys();*/

        if (keyValues != null) {
            //Map<Integer,KeyValueTs> keyValuesMap = new HashMap<Integer, KeyValueTs>();
            Map<String, String> keyValuesMap = new HashMap<String, String>();

            boolean needsToWriteCache = false;
            // read in the values from file, then compare with the ones in cache
            String[] pairs = amperSpliter.split(keyValues);
            for (String pair : pairs) {
                if (pair != null) {
                    String[] kv = equalSpliter.split(pair);
                    if (kv.length == 2) {
                        // we get a key/value pair
                        needsToWriteCache = true;
                        String keyStr = null;
                        String value = null;
                        if (kv[0] != null)
                            keyStr = kv[0].trim();
                        if (kv[1] != null)
                            value = URLDecoder.decode(kv[1].trim(), OpinmindConstants.UTF_8);

                        keyValuesMap.put(keyStr, value);
                    }
                }
            }

            if(!keyValuesMap.isEmpty()) {
                // process the key/value pair
                // * raw ckv data, process the data and log netezza clogs
                // * ckv data, simply put it in
                final Map<Integer, KeyValueTs> ckvMap = processKeyValue(
                        keyValuesMap,
                        timeStamp,
                        cookieId,
                        eventIdStr,
                        dpIdStr,
                        locationIdStr,
                        refererUrl,
                        domain,
                        userAgent,
                        null,
                        null,
                        null,
                        shouldWriteClog);

                Key key = new Key("database2", "table2", "key1");
                Bin column1 = new Bin("cookieId", cookieId);
                Bin column2 = new Bin("ckvMap", ckvMap);
                client.put(null, key, column1,column2);
            }
        }
    }


    protected static Map<Integer,KeyValueTs> processKeyValue(
            Map<String, String> keyValuesMap,
            Date timeStamp,
            Long cookieId,
            String eventIdStr,
            String dpIdStr,
            String locationIdStr,
            String refererUrl,
            String domain,
            String userAgent,
            Set<Integer> keysGoToEkv,
            Set<Integer> keysGoToCkv,
            Set<Integer> keysGoToBidgen,
            Boolean shouldWriteClog) throws Exception {
        Map<Integer,KeyValueTs> ckvMap = new HashMap<Integer, KeyValueTs>();

        // directly put the content into the map
        for (String keyStr : keyValuesMap.keySet()) {

            int key = Integer.valueOf(keyStr);
            if (keysGoToBidgen==null || keysGoToBidgen.contains(key)) {
                String value = keyValuesMap.get(keyStr);

                // get the new kvt from file
                KeyValueTs kvtInFile = new KeyValueTs(key, value, timeStamp);
                ckvMap.put(key, kvtInFile);
            }
        }

        return ckvMap;

    }

    private static String getFromDataArray(String[] data, Integer index, Boolean needUrlEncode) {
        String result = null;

        try {
            if (data.length > index) {
                String src = data[index];
                if (src!=null) {
                    src = src.trim();
                    if (src.length()>0 && !src.equals("null")) {
                        if (Boolean.TRUE.equals(needUrlEncode)) {
                            result =  URLDecoder.decode(src, OpinmindConstants.UTF_8);
                        }
                        else {
                            result = src;
                        }
                    }
                }
            }
        } catch (Exception e) {
        }

        return result;
    }


}
