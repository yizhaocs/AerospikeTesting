package com.adara.newcache.gcloudcode.bigtable;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Map;

/**
 * Created by yzhao on 7/17/17.
 */
public class ReadTable {
    public static void executeReadingArowByItsKey(Table table, byte[] rowkey, byte[] COLUMN_FAMILY_NAME, byte[] COLUMN_NAME) throws Exception{
        //Scan scan = new Scan();
        // StringBuilder result = new StringBuilder();
        //ResultScanner scanner = table.getScanner(scan);
        long startTime = System.nanoTime();
        Result getResult = table.get(new Get(rowkey));
        byte[] valueBytes = getResult.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME);
        print(valueBytes);
        long endTime = System.nanoTime();

        long duration = (endTime - startTime)/1000000; // in milliseconds
        System.out.println("total time used for reading one bye one:" + duration + " milliseconds");
/*
        ByteArrayInputStream byteIn = new ByteArrayInputStream(getResult.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME));
        ObjectInputStream in = new ObjectInputStream(byteIn);
        Map<Integer, String> data2 = (Map<Integer, String>) in.readObject();
        */
        //System.out.println( " ,ckvMap:" + data2.toString());
        //  System.out.println(Bytes.toString(getResult.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME)));
       /* byte[] valueBytes = null;
        for (Result row : scanner) {
            valueBytes = row.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME);
        }*/
    }


    public static void executeScanningAllTableRows(Table table, byte[] rowkey, byte[] COLUMN_FAMILY_NAME, byte[] COLUMN_NAME) throws Exception{
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);

/*
       byte[] valueBytes = null;
        for (Result row : scanner) {
            valueBytes = row.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME);
        }

        ByteArrayInputStream byteIn = new ByteArrayInputStream(valueBytes);
        ObjectInputStream in = new ObjectInputStream(byteIn);
        Map<Integer, String> data2 = (Map<Integer, String>) in.readObject();
        System.out.println("ckvMap:" + data2.toString());*/
    }

    private static void print(byte[] valueBytes) throws Exception{
        ByteArrayInputStream byteIn = new ByteArrayInputStream(valueBytes);
        ObjectInputStream in = new ObjectInputStream(byteIn);
        Map<Integer, String> data2 = (Map<Integer, String>) in.readObject();
        System.out.println("valueBytes:" + data2.toString());
    }
}
