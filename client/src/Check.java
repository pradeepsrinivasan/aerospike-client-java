import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.CustomRouting;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by pradeep on 22/09/15.
 */
public class Check {

    public static void main(String args[]) {
        ClientPolicy policy = new ClientPolicy();
        policy.maxThreads = 10;
        policy.requestProleReplicas = true;

        Host[] hosts = new Host[] {
                new Host("10.33.90.160", 3000),
                new Host("10.33.110.122", 3000),
                new Host("10.33.18.132", 3000),
                new Host("10.33.65.19", 3000),
                new Host("10.33.46.154", 3000),
                new Host("10.32.126.118", 3000),
                new Host("10.32.41.78", 3000),
                new Host("10.33.50.157", 3000),
                new Host("10.32.58.110", 3000),
                new Host("10.32.14.104", 3000),
                new Host("10.32.78.0", 3000),
                new Host("10.32.69.85", 3000),
                new Host("10.32.105.81", 3000),
                new Host("10.32.22.110", 3000)
        };



        AerospikeClient client = new AerospikeClient(policy, hosts);
//        client.cluster.printPartitionMap();

//        String in = "LSTJEAE9PP95NDTGGYVM8Z5C9,LSTTROE9PJCJEU7VZ6RO7DURE,LSTTROE9BR3JVQR2WEGQ2YYPQ,LSTTKPEA59EZR2GHSNNU9YFEQ,LSTTKPEA59EYMJPMYZRNTXERM,LSTSRTEA59EFPNA7R6G0VIWLQ,LSTSRTE99ANE3VFGKBG7HMPGU,LSTSRTEA7GXTTETRBBEBMRPTP,LSTTKPEA59ECE7SAHAZL4XBSI,LSTSRTE9ZVRUUV2HVJVWFSEMR,LSTTROE8ZB6HFKSDN7CMZI7L3,LSTSRTEA59DWEUCZBES1O1ZA2,LSTJEAEAYYT8YDSUSFKSPGX7L,LSTTKPE8VZHUZKYHGZZSETF6I,LSTTKPE9ZPHBSYXVMPA3XBSYJ,LSTSRTEA59D6WT5GSZZCYHYAD,LSTJEAE96X6GRRVGYEMXPAJNA,LSTJEAE9FG7TJKVN2RMCDCCVZ,LSTTROEA59DRRZ9SPTMBNWOTN,LSTTROE9PJC5MTF5NCUDN6FD7";

        Scanner sc = new Scanner(System.in);
        Map<Node, AtomicInteger> summary = new HashMap<Node, AtomicInteger>();
        try {
            while ( sc.hasNextLine() ) {
                Key[] keys = getKeys(sc.next());
                Map<Node, HashSet<Key>> result = CustomRouting.prepareBatchNodes(client.cluster, keys);
                for (Map.Entry<Node, HashSet<Key>> entry: result.entrySet()) {
                    if ( ! summary.containsKey(entry.getKey()) ) {
                        summary.put(entry.getKey(), new AtomicInteger(0));
                    }
                    summary.get(entry.getKey()).addAndGet(entry.getValue().size());
                }
            }
        } catch ( java.util.NoSuchElementException e) {

        }


        for ( Map.Entry<Node, AtomicInteger> entry : summary.entrySet()) {
            System.out.println(entry.getKey().getHost() + "-------" + entry.getValue());
        }


    }

    private static Key[] getKeys(String in) {
        String[] ids = in.split(",");
        List<Key> keys = new ArrayList<Key>();
        for ( String id : ids) {
            if ( id != "") {
                Key key = new Key("dsp-santa", "olm", id);
                keys.add(key);
            }
        }

        return keys.toArray(new Key[]{});
    }
}
