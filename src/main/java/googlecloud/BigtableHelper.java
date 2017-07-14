package main.java.googlecloud;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class BigtableHelper {
    private static final String PROJECT_ID = "YOUR_PROJECT_ID";
    private static final String INSTANCE_ID = "YOUR_INSTANCE_ID";

    private static Connection connection = null;

    public static void connect() throws IOException {
        connection = BigtableConfiguration.connect(PROJECT_ID, INSTANCE_ID);
    }
}