package com.adara.newcache.gcloudcode;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.opinmind.ssc.KeyValueTs;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

/**
 * Created by yzhao on 7/17/17.
 */
public class BigtableMain {
    static byte[] tableName = Bytes.toBytes("table1");
    static byte[] columnFaimilyName = Bytes.toBytes("columnFamily1");
    public static void main(String[] args) throws Exception{
        Connection connection = BigTableConnection.getConnection();
        //System.out.println(BigtableHelloWorld.create(connection));
        System.out.println(CreateTable.execute(connection, tableName, columnFaimilyName));
        Table table = connection.getTable(TableName.valueOf(tableName));
    }



}
