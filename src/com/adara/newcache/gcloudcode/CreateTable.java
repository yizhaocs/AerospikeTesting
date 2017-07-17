package com.adara.newcache.gcloudcode;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by yzhao on 7/17/17.
 */
public class CreateTable {
    public static String create(Connection connection, String tableName, String columnFamilyName) {
        try {
            // The admin API lets us create, manage and delete tables
            Admin admin = connection.getAdmin();
            // [END connecting_to_bigtable]

            // [START creating_a_table]
            // Create a table with a single column family
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            descriptor.addFamily(new HColumnDescriptor(columnFamilyName));

            admin.createTable(descriptor);
            // [END creating_a_table]
        } catch (IOException e) {
            return "Table exists.";
        }
        return "Create table " + tableName;
    }
}
