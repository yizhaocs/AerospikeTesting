package com.adara.newcache.udcuv2code;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.opinmind.common.OpinmindConstants;
import com.opinmind.ssc.KeyValueTs;
import org.apache.commons.lang.text.StrTokenizer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Created by yzhao on 7/14/17.
 */
public class ProcessCkvData {
    private static final Pattern amperSpliter = Pattern.compile("&");
    private static final Pattern equalSpliter = Pattern.compile("=");

    public static void readThenWrite(Map<String, Map<Integer,KeyValueTs>> map, String fileInput){
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
                    processData(map, data, null, 0, true);
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

    public static void processData(Map<String, Map<Integer,KeyValueTs>> map , String[] data, String fileName, int lineNo, Boolean shouldWriteClog) throws Exception {
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

                map.put(String.valueOf(cookieId), ckvMap);

/*
                Key key = new Key("test", "table10", cookieId);
                Bin column1 = new Bin("cookieId", cookieId);
                Bin column2 = new Bin("ckvMap", ckvMap);
                //Record r = client.get(null,key);
                //if(r!= null && !r.bins.containsKey(cookieId)) {
                if(!client.exists(null, key)) {
                    System.out.println(cookieId);
                    try {
*//*                        EventLoop eventLoop = EventLoopsHelp.eventLoops.get(0);
                        WriteListener listener = new WriteListener() {
                            @Override
                            public void onSuccess(Key key) {

                            }

                            @Override
                            public void onFailure(AerospikeException e) {

                            }
                        };*//*
                        // client.put(eventLoop, listener, wp, key, column2);
                        client.put(wp, key, column1, column2);
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }*/
                //}
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
