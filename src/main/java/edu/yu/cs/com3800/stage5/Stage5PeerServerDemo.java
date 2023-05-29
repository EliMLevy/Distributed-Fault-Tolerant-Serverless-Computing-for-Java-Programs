package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Stage5PeerServerDemo {
    private String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";

    private final int NUM_SERVERS = 5;
    private final int PORT_RANGE_START = 8000;
    private final int gatewayHTTPPort = PORT_RANGE_START + NUM_SERVERS * 10;
    private final int gatewayUDPPort = PORT_RANGE_START + NUM_SERVERS * 10 + 2;
    private final long gatewayID = NUM_SERVERS;
    private ArrayList<ZooKeeperPeerServer> servers;

    private GatewayServer gateway;

    public Stage5PeerServerDemo() throws Exception {

        cleanLogs();
        // step 1: create servers
        createServers();
        // step2: wait for servers to get started
        Thread.sleep(30_000);

        printLeaders();
        // step 3: send requests to the gatewayPort
        List<Future<String>> responses = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        for (int i = 0; i < NUM_SERVERS; i++) {
            final String code = this.validClass.replace("world!", "world! from code version " + i);
            responses.add(executor.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return sendMessage("localhost", gatewayHTTPPort, code);
                }
            }));
            // System.out.println(sendMessage( "localhost", gatewayHTTPPort, code));
        }

        // step 4: validate responses from leader

        printResponses(responses);

        System.out.println("Shutting down server " + servers.get(0).getServerId());
        servers.get(0).shutdown();

        Thread.sleep(10_000);
        responses = new ArrayList<>();
        for (int i = 0; i < NUM_SERVERS * 3; i++) {
            final String code = this.validClass.replace("world!", "world! from code version " + i);
            responses.add(executor.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return sendMessage("localhost", gatewayHTTPPort, code);
                }
            }));
            // System.out.println(sendMessage( "localhost", gatewayHTTPPort, code));
        }

        // step 4: validate responses from leader

        printResponses(responses);

        System.out.println("Got all responses!");
        Thread.sleep(30_000);

        // step 5: stop servers
        executor.shutdown();
        stopServers();
    }

    private void cleanLogs() {
        File dir = new File("./logs");
        for (File file : dir.listFiles())
            if (!file.isDirectory())
                file.delete();
    }

    private void printLeaders() {
        for (ZooKeeperPeerServer server : this.servers) {
            Vote leader = server.getCurrentLeader();
            if (leader != null) {
                System.out.println("Server on port " + server.getAddress().getPort() + " whose ID is "
                        + server.getServerId() + " has the following ID as its leader: " + leader.getProposedLeaderID()
                        + " and its state is " + server.getPeerState().name());
            }
        }
    }

    private void stopServers() {
        for (ZooKeeperPeerServer server : this.servers) {
            System.out.println("Shutting down server " + server.getServerId());
            server.shutdown();
        }
        System.out.println("Shutting down gateway");
        this.gateway.shutdown();

    }

    private void printResponses(List<Future<String>> responses) throws Exception {
        for (int i = 0; i < responses.size(); i++) {
            System.out.println("Response to request " + i + ":\n" + responses.get(i).get() + "\n\n");
        }
    }

    private String sendMessage(String hostName, int hostPort, String code) throws IOException, InterruptedException {
        System.out.println(String.format("Sending to %s:%d", hostName, hostPort));
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(code))
                .uri(URI.create("http://" + hostName + ":" + hostPort + "/compileandrun"))
                .header("Content-Type", "text/x-java-source")
                .build();
        HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
        String resBody = new String(response.body());
        return String.format("Code: %d; Body: %s", response.statusCode(), resBody);
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
        new Stage5PeerServerDemo();
    }
}