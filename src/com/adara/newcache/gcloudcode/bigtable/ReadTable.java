package com.adara.newcache.gcloudcode.bigtable;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by yzhao on 7/17/17.
 */
public class ReadTable {
    public static byte[] execute(Table table, byte[] COLUMN_FAMILY_NAME, byte[] COLUMN_NAME) throws Exception{
        Scan scan = new Scan();
        StringBuilder result = new StringBuilder();
        ResultScanner scanner = table.getScanner(scan);
        byte[] valueBytes = null;
        for (Result row : scanner) {
            valueBytes = row.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME);
        }
        return valueBytes;
    }
}
