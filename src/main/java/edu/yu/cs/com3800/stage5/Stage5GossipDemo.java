package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Stage5GossipDemo {

    private final int NUM_SERVERS = 9;
    private final int PORT_RANGE_START = 8000;
    private final int gatewayHTTPPort = PORT_RANGE_START + NUM_SERVERS * 10;
    private final int gatewayUDPPort = PORT_RANGE_START + NUM_SERVERS * 10 + 2;
    private final long gatewayID = NUM_SERVERS;
    private ArrayList<ZooKeeperPeerServer> servers;

    private GatewayServer gateway;

    public Stage5GossipDemo() throws Exception {
        cleanLogs();

        // step 1: create servers
        createServers();
        // step2: wait for servers to get started
        Thread.sleep(10_000);

        for (int i = 1; NUM_SERVERS - i > 3; i++) {

            System.out.println("Shutting down server " + servers.get((NUM_SERVERS -
                    i)).getServerId());
            servers.get((NUM_SERVERS - i)).shutdown();

            Thread.sleep(60_000); // Wait for servers to notice that it is dead
            System.out.println("*********************");
        }

        // step 5: stop servers
        stopServers();
    }

    private void cleanLogs() {
        File dir = new File("./logs");
        for (File file : dir.listFiles())
            if (!file.isDirectory())
                file.delete();
    }

    private void stopServers() {
        for (ZooKeeperPeerServer server : this.servers) {
            System.out.println("Shutting down server " + server.getServerId());
            server.shutdown();
        }
        System.out.println("Shutting down gateway");
        this.gateway.shutdown();
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

        // Create gateway
        this.gateway = new GatewayServer(this.gatewayHTTPPort, this.gatewayUDPPort, this.gatewayID,
                new HashMap<>(peerIDtoAddress));

        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            HashMap<Long, InetSocketAddress> map = new HashMap<>(peerIDtoAddress);
            map.remove(entry.getKey());
            map.put(this.gatewayID, new InetSocketAddress("localhost", this.gatewayUDPPort));
            ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(),
                    map, this.gatewayID, 1);
            this.servers.add(server);
            server.start();
        }

        peerIDtoAddress.put(this.gatewayID, new InetSocketAddress("localhost", this.gatewayUDPPort));
        this.servers.add(gateway.gatewayPeerServer);

    }

    public static void main(String[] args) throws Exception {
        new Stage5GossipDemo();
    }

}
