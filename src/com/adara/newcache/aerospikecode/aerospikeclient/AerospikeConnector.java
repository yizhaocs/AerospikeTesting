package com.adara.newcache.aerospikecode.AerospikeClient;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.policy.ClientPolicy;

/**
 * @author yzhao
 */
public class AerospikeConnector {
    private static final ClientPolicy policy = new ClientPolicy();
    private static String aeroServer1;
    private static String aeroServer2;
    private static String aeroServer3;
    private static int port;

    private static final AerospikeClient instance = new AerospikeClient(policy, new Host(aeroServer1, port), new Host(aeroServer2, port), new Host(aeroServer3, port));

    protected AerospikeConnector() {
    }

    // Runtime initialization
    // By defualt ThreadSafe
    public static AerospikeClient getInstance() {
        return instance;
    }

    public void setAeroServer1(String aeroServer1) {
        this.aeroServer1 = aeroServer1;
    }

    public void setAeroServer2(String aeroServer2) {
        this.aeroServer2 = aeroServer2;
    }

    public void setAeroServer3(String aeroServer3) {
        this.aeroServer3 = aeroServer3;
    }

    public void setPort(int port) {
        this.port = port;
    }
}