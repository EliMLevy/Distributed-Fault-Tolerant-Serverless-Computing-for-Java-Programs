package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Message.MessageType;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class Stage5CompletedAndCachedWorkDemo {

    private final int NUM_SERVERS = 5;
    private final int PORT_RANGE_START = 8000;
    private final int gatewayHTTPPort = PORT_RANGE_START + NUM_SERVERS * 10;
    private final int gatewayUDPPort = PORT_RANGE_START + NUM_SERVERS * 10 + 2;
    private final long gatewayID = NUM_SERVERS;
    private ArrayList<ZooKeeperPeerServer> servers;

    private GatewayServer gateway;

    private String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";

    public Stage5CompletedAndCachedWorkDemo() throws Exception {
        cleanLogs();

        // step 1: create servers
        createServers();
        // step2: wait for servers to get started
        Thread.sleep(10_000);
        // printLeaders();

        // for(int i = 1; NUM_SERVERS - i > 3; i++) {

        // System.out.println("Shutting down server " + servers.get((NUM_SERVERS -
        // i)).getServerId());
        // servers.get((NUM_SERVERS - i)).shutdown();

        // Thread.sleep(60_000); //Wait for servers to notice that it is dead
        // System.out.println("*********************");
        // // printLeaders();
        // }

        System.out.println("Shutting down server " + servers.get(NUM_SERVERS - 1).getServerId());
        servers.get(NUM_SERVERS - 1).shutdown();

        // Start sending TCP requests to NUM_SERVERS-2
        new Thread(() -> {
            long wordId = 0;
            while (true) {
                int destPort = PORT_RANGE_START + (NUM_SERVERS - 2) * 10;
                Message msg = new Message(MessageType.WORK, validClass.replace("world!", "world! from code version " + 0).getBytes(), "localhost", 9000, "localhost",
                        destPort, wordId++);
                try (Socket socket = new Socket("localhost", destPort + 2);) {
                    socket.getOutputStream().write(msg.getNetworkPayload());
                    Message response = new Message(Util.readAllBytesFromNetwork(socket.getInputStream()));
                    System.out.println(new String(response.getMessageContents()));
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                }
            }
        }).start();

        new Thread(() -> {
            long wordId = 1_000;
            while (true) {
                int destPort = PORT_RANGE_START + (NUM_SERVERS - 3) * 10;
                Message msg = new Message(MessageType.WORK, validClass.replace("world!", "world! from code version " + 1).getBytes(), "localhost", 9010, "localhost",
                        destPort, wordId++);
                try (Socket socket = new Socket("localhost", destPort + 2);) {
                    socket.getOutputStream().write(msg.getNetworkPayload());
                    Message response = new Message(Util.readAllBytesFromNetwork(socket.getInputStream()));
                    System.out.println(new String(response.getMessageContents()));
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                }
            }
        }).start();

        new Thread(() -> {
            long wordId = 1_000_000;
            while (true) {
                int destPort = PORT_RANGE_START + (NUM_SERVERS - 4) * 10;
                Message msg = new Message(MessageType.WORK, validClass.replace("world!", "world! from code version " + 2).getBytes(), "localhost", 9020, "localhost",
                        destPort, wordId++);
                try (Socket socket = new Socket("localhost", destPort + 2);) {
                    socket.getOutputStream().write(msg.getNetworkPayload());
                    Message response = new Message(Util.readAllBytesFromNetwork(socket.getInputStream()));
                    System.out.println(new String(response.getMessageContents()));
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                }
            }
        }).start();

        Thread.sleep(60_000); // Wait for servers to notice that it is dead
        // printLeaders();

        // // step 5: stop servers
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
        new Stage5CompletedAndCachedWorkDemo();
    }

}
