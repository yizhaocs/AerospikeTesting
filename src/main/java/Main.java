import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

/**
 * Created by yzhao on 7/12/17.
 */
public class Main {
    public static void main(String[] args){
        connection();
    }


    public static void connection(){
        ClientPolicy policy = null;
        AerospikeClient client = null;

        try{
            policy = new ClientPolicy();
            client = new AerospikeClient(policy, "172.28.128.3", 3000);

        }finally {
            if(client != null){
                client.close();
            }
        }
    }


    public static void getPutOperations(ClientPolicy policy, AerospikeClient client){
        Key key = new Key("test", "test_set", "putgetkey");
        Bin bin1 = new Bin("bin1", "value1");
        Bin bin2 = new Bin("bin2", "value2");

        client.put(new WritePolicy(), key, bin1, bin2);
        Record record = client.get(new Policy(), key);
    }
}
