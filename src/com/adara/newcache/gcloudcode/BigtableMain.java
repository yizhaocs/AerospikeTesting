package com.adara.newcache.gcloudcode;

import org.apache.hadoop.hbase.client.Connection;

/**
 * Created by yzhao on 7/17/17.
 */
public class BigtableMain {
    public static void main(String[] args){
        Connection connection = BigtableHelper.getConnection();
        //System.out.println(BigtableHelloWorld.create(connection));
        System.out.println(CreateTable.create(connection, "test1", ));
    }
}
