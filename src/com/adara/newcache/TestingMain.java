package com.adara.newcache;

import com.adara.newcache.aerospikecode.AerospikeConnection;
import com.adara.newcache.gcloudcode.BigTableConnection;
import com.adara.newcache.gcloudcode.CreateTable;
import com.adara.newcache.gcloudcode.InsertTable;
import com.opinmind.ssc.KeyValueTs;
import com.adara.newcache.udcuv2code.ProcessCkvData;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

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
        writeToBigTable( );

    }

    public static void writeToAerospkie(){
        AerospikeConnection.connection(map, "106879103115");
    }


    public static void writeToBigTable( ){
        try {
            byte[] tableName = Bytes.toBytes("table1");
            byte[] columnFaimilyName = Bytes.toBytes("ckvMap");
            Connection connection = BigTableConnection.getConnection();
            //System.out.println(BigtableHelloWorld.create(connection));
            System.out.println(CreateTable.execute(connection, tableName, columnFaimilyName));
            Table table = connection.getTable(TableName.valueOf(tableName));
            writeToBigTable2(table);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    public static void writeToBigTable2(Table table ) throws Exception{
        byte[] tableName = Bytes.toBytes("table1");
        byte[] columnFaimilyName = Bytes.toBytes("ckvMap");


        long startTime = System.nanoTime();

        int count = 0;
        for(String cookieId: map.keySet()){
            Map<Integer, KeyValueTs > ckvMap = map.get(cookieId);
            for(int key : ckvMap.keySet()){
                KeyValueTs mKeyValueTs = ckvMap.get(key);
                byte[] rowKey = Bytes.toBytes(cookieId);
                //byte[] columnFamily =  Bytes.toBytes("ckvMap");
                byte[] columnQualifierkeyId = Bytes.toBytes("keyId");
                byte[] columnQualifierValue = Bytes.toBytes("value");
                byte[] columnQualifierLastPixelTs = Bytes.toBytes("lastPixelTs");

                InsertTable.execute(table, rowKey, columnFaimilyName, columnQualifierkeyId, Bytes.toBytes(mKeyValueTs.getKeyId()));
                InsertTable.execute(table, rowKey, columnFaimilyName, columnQualifierValue, Bytes.toBytes(mKeyValueTs.getValue()));
                InsertTable.execute(table, rowKey, columnFaimilyName, columnQualifierLastPixelTs, Bytes.toBytes(mKeyValueTs.getLastPixelTs().getTime()));

            }

            count ++;
            System.out.println("count:" + count);

        }
        long endTime = System.nanoTime();

        long duration = (endTime - startTime)/1000000; // in milliseconds
        System.out.println("duration:" + duration + " milliseconds ,with count:" + count); // duration:2824 milliseconds ,with count:2154
    }
}
