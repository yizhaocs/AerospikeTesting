package com.adara.newcache.gcloudcode;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by yzhao on 7/17/17.
 */
public class InsertTable {
    public static void execute(Table table, byte[] rowKey, byte[] columnFamilyName, byte[] columnQualifier, byte[] value ) {
        try {
            Put put = new Put(rowKey);
            put.addColumn(columnFamilyName, columnQualifier, value);
            table.put(put);
        } catch (Exception e) {
        }
    }
}
