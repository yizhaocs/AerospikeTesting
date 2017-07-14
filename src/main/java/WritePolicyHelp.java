package main.java;

import com.aerospike.client.policy.WritePolicy;

/**
 * Created by yzhao on 7/14/17.
 */
public class WritePolicyHelp {
    /**
     * https://discuss.aerospike.com/t/understanding-timeout-errors/2852
     * In WritePolicy, if you have maxRetries(3), sleepBetweenRetries(500ms) and timeout(0), you will try the operation 3 times, with a 500ms wait between each try.
     * If you have not been successful after 1.5 seconds you will get an exception.
     */
    public static WritePolicy wp = new WritePolicy();
    static {
        wp.maxRetries = 3;
        wp.sleepBetweenRetries = 500;
        wp.setTimeout(10000);
    }
}
