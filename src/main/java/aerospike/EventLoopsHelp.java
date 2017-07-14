package main.java.aerospike;

import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.NioEventLoops;

/**
 * Created by yzhao on 7/14/17.
 */
public class EventLoopsHelp {
    public static EventLoops eventLoops =  new NioEventLoops(1000);
}
