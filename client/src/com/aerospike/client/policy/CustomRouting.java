package com.aerospike.client.policy;

import com.aerospike.client.Key;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Created by pradeep on 23/09/15.
 */
public class CustomRouting {

    public static Map<Node, HashSet<Key>> prepareBatchNodes(Cluster cluster, Key[] keys) {
        Map<Key, HashSet<Node>> key2NodesMap = new HashMap<Key, HashSet<Node>>();
        Map<Node, HashSet<Key>> node2KeysMap = new HashMap<Node, HashSet<Key>>();

        for ( int i = 0;i < keys.length; i++) {
            Key ky = keys[i];
            Node[] nodes = cluster.getAllNodes(new Partition(ky));
            key2NodesMap.put(ky, toHashSet(nodes));
            for ( Node node : nodes) {
                if ( ! node2KeysMap.containsKey(node) ) {
                    node2KeysMap.put(node, new HashSet<Key>());
                }
                node2KeysMap.get(node).add(ky);
            }
        }

//        print(key2NodesMap, node2KeysMap);

        Map<Node, HashSet<Key>> result = new HashMap<Node, HashSet<Key>>();
        algo(node2KeysMap, result);

        return result;
    }

    private static void algo(Map<Node, HashSet<Key>> node2KeysMap, Map<Node, HashSet<Key>> result) {
        int maxKeyCount = 0;
        Node maxNode = null;

        for ( Node node : node2KeysMap.keySet()) {
            HashSet<Key> keys = node2KeysMap.get(node);
            if ( keys.size() > maxKeyCount ) {
                maxKeyCount = keys.size();
                maxNode = node;
            }
        }

        if ( maxKeyCount == 0) { return; }

        result.put(maxNode, node2KeysMap.get(maxNode));
        HashSet<Key> keysToRm = node2KeysMap.get(maxNode);
        node2KeysMap.remove(maxNode);
        /* remove all keys */
        for ( Map.Entry<Node, HashSet<Key>> entry : node2KeysMap.entrySet()) {
            entry.getValue().removeAll(keysToRm);
        }

        algo(node2KeysMap, result);
    }

    private static void print(Map<Key, HashSet<Node>> key2NodesMap, Map<Node, HashSet<Key>> node2KeysMap) {
//        System.out.println("KEYS----ADJ LIST");
//        for ( Key ky : key2NodesMap.keySet()) {
//            HashSet<Node> val = key2NodesMap.get(ky);
//            StringBuilder sb = new StringBuilder(ky.userKey + "::[");
//            for ( Node node : val) {
//                sb.append(node.getHost() + ",");
//            }
//            sb.append("]"+  "["+val.size()+"]");
//            System.out.println(sb.toString());
//        }
//
//        System.out.println("NODES----ADJ LIST");
        for ( Node node : node2KeysMap.keySet()) {
            HashSet<Key> keys = node2KeysMap.get(node);
//            StringBuilder sb = new StringBuilder(node.getHost() + "::[");
//            for ( Key key : keys) {
//                sb.append(key.userKey + ",");
//            }
//            sb.append("]"+ "["+keys.size()+"]");
//            System.out.println(sb.toString());
            System.out.println(node.getHost() + "-" + keys.size());
        }
    }

    private static HashSet<Node> toHashSet(Node[] nodes) {
        HashSet<Node> nodeSet = new HashSet<Node>();
        for ( Node node: nodes) {
            nodeSet.add(node);
        }
        return nodeSet;
    }

}
