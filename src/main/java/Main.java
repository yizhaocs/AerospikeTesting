import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import model.CookieData;

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
            getPutOperations(client);
            getPutOperationsAdara(client);
        }finally {
            if(client != null){
                client.close();
            }
        }
    }


    public static void getPutOperations(AerospikeClient client){
        Key key = new Key("test", "CookieData", "putgetkey");
        // Key key = new Key("adara", "CookieData", "putgetkey");
        Bin bin1 = new Bin("bin1", "value1");
        Bin bin2 = new Bin("bin2", "value2");

        client.put(null, key, bin1, bin2);
        Record record = client.get(null, key);
        System.out.println(record.toString());
    }

    public static void getPutOperationsAdara(AerospikeClient client){
        Key key = new Key("adara", "CookieData", "putgetkey");
        CookieData mCookieData = new CookieData();
        Bin bin1 = new Bin("bin1", mCookieData);
        Bin bin2 = new Bin("bin2", mCookieData);

        client.put(null, key, bin1, bin2);
        Record record = client.get(null, key);
        System.out.println(record.toString());
    }
}
