package com.adara.newcache;

import com.adara.newcache.aerospikecode.AerospikeConnection;
import com.adara.newcache.gcloudcode.bigtable.BigTableConnection;
import com.adara.newcache.gcloudcode.bigtable.CreateTable;
import com.adara.newcache.gcloudcode.bigtable.InsertTable;
import com.adara.newcache.gcloudcode.bigtable.ReadTable;
import com.opinmind.ssc.KeyValueTs;
import com.adara.newcache.udcuv2code.ProcessCkvData;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jettison.json.JSONArray;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
    static byte[] columnFaimilyName = "columnFaimilyName".getBytes();
    static String bigtableTableName = "table10";

    public static void main(String[] args) throws Exception{
        //String cookieId = args[0];
        ProcessCkvData.readThenWrite(map, "/Users/yzhao/Desktop/20170712-004428.ps101-lax1.0000000000010309020.csv");
        //writeToBigTable( );
        readBigTable();

    }

    public static void writeToAerospkie(){
        AerospikeConnection.connection(map, "106879103115"); // duration:2824 milliseconds ,with count:2154, 1.31104921 ms per request
    }


    public static void writeToBigTable( ){
        try {
            byte[] tableName = Bytes.toBytes(bigtableTableName);

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


        long startTime = System.nanoTime();

        int count = 0;
        for(String cookieId: map.keySet()){
            Map<Integer, KeyValueTs > ckvMap = map.get(cookieId);
            JSONArray jsonArray = new JSONArray();
            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(byteOut);
            out.writeObject(ckvMap);
            byte[] rowKey = "rowKey".getBytes();

            byte[] columnQualifier = Bytes.toBytes(cookieId);
            InsertTable.execute(table, rowKey, columnFaimilyName, columnQualifier,byteOut.toByteArray());
/*

            System.out.println("cookie:" + cookieId);
            byte[] result = ReadTable.execute(table, columnFaimilyName, columnQualifier);
            // Parse byte array to Map
            ByteArrayInputStream byteIn = new ByteArrayInputStream(result);
            ObjectInputStream in = new ObjectInputStream(byteIn);
            Map<Integer, String> data2 = (Map<Integer, String>) in.readObject();
            System.out.println("cookie:" + cookieId + " ,ckvMap:" + data2.toString());
*/

            count ++;
           // System.out.println("count:" + count);

        }
        long endTime = System.nanoTime();

        long duration = (endTime - startTime)/1000000; // in milliseconds
        System.out.println("total time used for writing:" + duration + " milliseconds ,with count:" + count);
    }

    public static void readBigTable() throws Exception{
        Connection connection = BigTableConnection.getConnection();
        byte[] tableName = Bytes.toBytes(bigtableTableName);
        byte[] rowKey = "rowKey".getBytes();
        Table table = connection.getTable(TableName.valueOf(tableName));
        int count = 0;
        long startTime = System.nanoTime();
        for(String cookieId: map.keySet()) {
            byte[] columnQualifier = Bytes.toBytes(cookieId);
             ReadTable.executeReadingArowByItsKey(table, rowKey, columnFaimilyName, columnQualifier);
            // Parse byte array to Map
           // ByteArrayInputStream byteIn = new ByteArrayInputStream(result);
            //ObjectInputStream in = new ObjectInputStream(byteIn);
            //Map<Integer, String> data2 = (Map<Integer, String>) in.readObject();
            //System.out.println("cookie:" + cookieId + " ,ckvMap:" + data2.toString());
            count ++;
            //System.out.println(count);
        }
        long endTime = System.nanoTime();

        long duration = (endTime - startTime)/1000000; // in milliseconds
        System.out.println("total time used for reading:" + duration + " milliseconds ,with count:" + count);
    }
}
