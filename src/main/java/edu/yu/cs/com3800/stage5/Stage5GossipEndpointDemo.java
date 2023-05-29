package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Stage5GossipEndpointDemo {

    private final int NUM_SERVERS = 5;
    private final int PORT_RANGE_START = 8000;
    private ArrayList<ZooKeeperPeerServer> servers;

    public Stage5GossipEndpointDemo() throws Exception {
        cleanLogs();

        // step 1: create servers
        createServers();
        // step2: wait for servers to get started
    }

    private void cleanLogs() {
        File dir = new File("./logs");
        for (File file : dir.listFiles())
            if (!file.isDirectory())
                file.delete();
    }



    private void createServers() throws IOException {
        // create IDs and addresses
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>();
        for (int i = 0; i < NUM_SERVERS; i++) {
            peerIDtoAddress.put(Integer.valueOf(i).longValue(),
                    new InetSocketAddress("localhost", PORT_RANGE_START + i * 10));
        }

        // create servers
        this.servers = new ArrayList<>();

        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            HashMap<Long, InetSocketAddress> map = new HashMap<>(peerIDtoAddress);
            map.remove(entry.getKey());
            ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(),
                    map, -1L, 0);
            this.servers.add(server);
            server.start();
        }

    }

    public static void main(String[] args) throws Exception {
        new Stage5GossipEndpointDemo();
    }

}
