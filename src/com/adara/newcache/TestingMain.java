package com.adara.newcache;

import com.adara.newcache.aerospikecode.AerospikeConnection;
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
public class TestingMain {

    static Map<String, Map<Integer,KeyValueTs>> map = new HashMap<String, Map<Integer, KeyValueTs>>();


    public static void main(String[] args){
        //String cookieId = args[0];
        ProcessCkvData.readThenWrite(map, "/Users/yzhao/Desktop/20170712-004428.ps101-lax1.0000000000010309020.csv");
        AerospikeConnection.connection(map, "106879103115");

    }



}
