package com.adara.newcache.gcloudcode.bigtable;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.hadoop.hbase.client.Connection;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;


/**
 * gcloud init
 * gcloud auth application-default login
 */
public class BigTableConnection {
    private static final String PROJECT_ID = "adara-bigtable1";
    private static final String INSTANCE_ID = "adara-bigtable1";

    public static void main(String[] args){
        getConnection();
    }

    private static Connection connection = null;

    public static Connection getConnection() {
        try {
            connection = BigtableConfiguration.connect(PROJECT_ID, INSTANCE_ID);
        }catch(Exception e){
            e.printStackTrace();
        }
        return connection;
    }
}