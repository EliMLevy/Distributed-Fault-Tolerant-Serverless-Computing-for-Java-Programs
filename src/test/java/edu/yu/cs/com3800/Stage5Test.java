package edu.yu.cs.com3800;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState;
import edu.yu.cs.com3800.stage5.GatewayPeerServerImpl;
import edu.yu.cs.com3800.stage5.GatewayServer;
import edu.yu.cs.com3800.stage5.Gossiper;
import edu.yu.cs.com3800.stage5.MockTCPServer;
import edu.yu.cs.com3800.stage5.TCPServer;
import edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl;
import edu.yu.cs.com3800.stage5.Gossiper.HeartbeatValue;
import edu.yu.cs.com3800.stage5.TCPServer.TCPMessage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Stage5Test {

    @BeforeEach
    public void init() {
        cleanLogs();
    }

    @Nested
    public class Stage4Demo {
        /*
         * Code for this demo is <em>largely</em> based on the demo code provided by
         * Professor Diament
         * in stage 4.
         */
        private String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";

        private final int NUM_SERVERS = 5;
        private final int PORT_RANGE_START = 8000;
        private final int gatewayHTTPPort = PORT_RANGE_START + NUM_SERVERS * 10;
        private final int gatewayUDPPort = PORT_RANGE_START + NUM_SERVERS * 10 + 2;
        private final long gatewayID = NUM_SERVERS;
        private final long expectedLeaderID = NUM_SERVERS - 1;
        private ArrayList<ZooKeeperPeerServer> servers;

        private GatewayServer gateway;

        // @Test
        public void runDemo() throws Exception {
            // create servers
            createServers();

            // wait for servers to get started
            Thread.sleep(10000);

            // Validate leaders
            printLeaders();

            // send requests to the gatewayPort
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
            }

            // validate responses responses
            printResponses(responses);

            // initiate shutdown
            executor.shutdown();
            stopServers();
        }

        private void printLeaders() {
            for (ZooKeeperPeerServer server : this.servers) {
                Vote leader = server.getCurrentLeader();
                if (leader != null) {
                    System.out.println("Server on port " + server.getAddress().getPort() + " whose ID is "
                            + server.getServerId() + " has the following ID as its leader: "
                            + leader.getProposedLeaderID()
                            + " and its state is " + server.getPeerState().name());

                    // assertEquals(expectedLeaderID, leader.getProposedLeaderID(), String.format(
                    // "Server %d had the wrong leader: %d", server.getServerId(),
                    // leader.getProposedLeaderID()));
                } else {
                    System.out.println(String.format("Server %d did not find leader", server.getServerId()));
                    // throw new AssertionError(String.format("Server %d did not find leader",
                    // server.getServerId()));
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
                String response = responses.get(i).get();
                Pattern p = Pattern.compile(".*" + i);
                Matcher m = p.matcher(response);
                System.out.println("Response to request " + i + ":\n" + response + "\n\n");
                assertTrue(m.matches(), String.format("Expected: %s; But was: %s", ".*" + i, response));
            }
        }

        private String sendMessage(String hostName, int hostPort, String code)
                throws IOException, InterruptedException {
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
                ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0,
                        entry.getKey(), map, this.gatewayID, 1);
                this.servers.add(server);
                server.start();
            }
            peerIDtoAddress.put(this.gatewayID, new InetSocketAddress("localhost", this.gatewayUDPPort));
            this.servers.add(gateway.gatewayPeerServer);

        }
    }

    @Nested
    public class gatewayPeerServerImplTest {

        private GatewayPeerServerImpl gatewayPeer;

        @BeforeEach
        public void init() {
            Map<Long, InetSocketAddress> peers = new HashMap<>();
            peers.put(10L, new InetSocketAddress("localhost", 3000));
            peers.put(11L, new InetSocketAddress("localhost", 3002));
            peers.put(12L, new InetSocketAddress("localhost", 3004));
            this.gatewayPeer = new GatewayPeerServerImpl(3050, 0, 3L, peers, 1);

        }

        @AfterEach
        public void cleanup() {
            this.gatewayPeer.shutdown();
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Test
        public void testStateImmutability() {
            // Make sure his state is immutable
            assertEquals(ServerState.OBSERVER, gatewayPeer.getPeerState());
            gatewayPeer.setPeerState(ServerState.LOOKING);
            assertEquals(ServerState.OBSERVER, gatewayPeer.getPeerState());
            gatewayPeer.setPeerState(ServerState.LEADING);
            assertEquals(ServerState.OBSERVER, gatewayPeer.getPeerState());
            gatewayPeer.setPeerState(ServerState.FOLLOWING);
            assertEquals(ServerState.OBSERVER, gatewayPeer.getPeerState());
        }

        @Test
        public void testSingleDelegateWork() {
            try {
                this.gatewayPeer.setCurrentLeader(new Vote(10L, 0));
            } catch (IOException e1) {
                e1.printStackTrace();
                throw new AssertionError("Set leader should not throw error");
            }

            String input = "Testing delegate work";
            LinkedBlockingQueue<Message> sentMessages = new LinkedBlockingQueue<>();
            MockTCPServer mockTCPServer = new MockTCPServer(3004, sentMessages);
            try {
                Message res = this.gatewayPeer.delegateWork(input, mockTCPServer, 1L);
                assertArrayEquals(("Response: " + input).getBytes(), res.getMessageContents());
                assertEquals(1, sentMessages.size());
                Message msg = sentMessages.poll();
                assertEquals(input, new String(msg.getMessageContents()));
                assertEquals(3052, msg.getSenderPort());
                assertEquals("localhost", msg.getSenderHost());
                assertEquals(3002, msg.getReceiverPort());
                assertEquals("localhost", msg.getReceiverHost());
                assertEquals(MessageType.WORK, msg.getMessageType());
            } catch (IOException e) {
                e.printStackTrace();
                throw new AssertionError("Delegate work should not throw an error");
            }
        }

        @Test
        public void testParralelDelegateWork() {
            final int NUM_CALLS = 10;

            try {
                this.gatewayPeer.setCurrentLeader(new Vote(10L, 0));
            } catch (IOException e1) {
                e1.printStackTrace();
                throw new AssertionError("Set leader should not throw error");
            }

            String input = "Testing delegate work #";
            LinkedBlockingQueue<Message> sentMessages = new LinkedBlockingQueue<>();
            MockTCPServer mockTCPServer = new MockTCPServer(3004, sentMessages);

            List<Future<String>> responses = new LinkedList<>();
            ExecutorService executorService = Executors.newFixedThreadPool(5);
            for (int i = 0; i < NUM_CALLS; i++) {
                final int index = i;
                responses.add(executorService.submit(() -> {
                    try {
                        Message res = this.gatewayPeer.delegateWork(input + index, mockTCPServer, 1L);
                        assertArrayEquals(("Response: " + input + index).getBytes(), res.getMessageContents());
                        // assertEquals(1, sentMessages.size());
                        Message msg = sentMessages.poll();
                        // assertEquals(input + index, new String(msg.getMessageContents()));
                        assertEquals(3052, msg.getSenderPort());
                        assertEquals("localhost", msg.getSenderHost());
                        assertEquals(3002, msg.getReceiverPort());
                        assertEquals("localhost", msg.getReceiverHost());
                        assertEquals(MessageType.WORK, msg.getMessageType());

                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new AssertionError("Delegate work should not throw an error");
                    }

                    return "Success from task #" + index;
                }));
            }

            for (int i = 0; i < responses.size(); i++) {
                Future<String> f = responses.get(i);
                try {
                    String result = f.get();
                    System.out.println(result);
                    assertEquals("Success from task #" + i, result);

                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                    throw new AssertionError("Task threw exceptions");
                }
            }

        }

    }

    @Nested
    public class GossiperTest {

        @Test
        public void testSerializeDeserializeHeartbeatMap() {
            Map<Long, HeartbeatValue> mapA = new HashMap<>();
            mapA.put(1L, new HeartbeatValue(1L, 5L, 9L));
            mapA.put(2L, new HeartbeatValue(2L, 6L, 10L));
            mapA.put(3L, new HeartbeatValue(3L, 7L, 11L));
            mapA.put(4L, new HeartbeatValue(4L, 8L, 12L));

            byte[] bytesOfA = Gossiper.heartbeatMapToByteArr(mapA);

            Map<Long, Long> mapB = Gossiper.byteArrToHeartbeatMap(bytesOfA);

            for (Map.Entry<Long, HeartbeatValue> entry : mapA.entrySet()) {
                assertTrue(mapB.containsKey(entry.getKey()), "Deserialized map is missing key " + entry.getKey());
                assertEquals(entry.getValue().counter, mapB.get(entry.getKey()),
                        String.format("Expected %d but found %d", entry.getValue().counter, mapB.get(entry.getKey())));
            }

            for (Map.Entry<Long, Long> entry : mapB.entrySet()) {
                assertTrue(mapA.containsKey(entry.getKey()), "Dseerialized map has extra key " + entry.getKey());
                assertEquals(entry.getValue(), mapA.get(entry.getKey()).counter,
                        String.format("Expected %d but found %d", entry.getValue(), mapA.get(entry.getKey()).counter));
            }
        }

        @Test
        public void testHandleGossipMessage() {

            Map<Long, InetSocketAddress> peerIdsToAddress = new HashMap<>();
            peerIdsToAddress.put(1L, new InetSocketAddress("localhost", 8000));
            peerIdsToAddress.put(2L, new InetSocketAddress("localhost", 8010));
            peerIdsToAddress.put(3L, new InetSocketAddress("localhost", 8020));
            peerIdsToAddress.put(4L, new InetSocketAddress("localhost", 8030));
            ZooKeeperPeerServerImpl peerServer = new ZooKeeperPeerServerImpl(5000, 0, 0L, peerIdsToAddress, null, 0);
            Gossiper gossiper = new Gossiper(peerServer, peerIdsToAddress, null, null, new ReentrantLock(), 5050);

            // Map with starting heartbeat counters
            Map<Long, HeartbeatValue> start = new HashMap<>(gossiper.getHeartbeatMap());

            Map<Long, HeartbeatValue> mapToMerge = new HashMap<>();
            mapToMerge.put(1L, new HeartbeatValue(1L, 10L, System.currentTimeMillis()));
            mapToMerge.put(2L, new HeartbeatValue(1L, Long.MIN_VALUE, System.currentTimeMillis()));
            mapToMerge.put(3L, new HeartbeatValue(1L, 10L, System.currentTimeMillis()));
            mapToMerge.put(4L, new HeartbeatValue(1L, Long.MIN_VALUE, System.currentTimeMillis()));

            gossiper.handleGossipMessage(0L, Gossiper.heartbeatMapToByteArr(mapToMerge), System.currentTimeMillis());

            Map<Long, HeartbeatValue> mergedMap = gossiper.getHeartbeatMap();

            assertEquals(10L, mergedMap.get(1L).counter);
            assertEquals(10L, mergedMap.get(3L).counter);

            assertEquals(start.get(2L).counter, mergedMap.get(2L).counter);
            assertEquals(start.get(4L).counter, mergedMap.get(4L).counter);

            gossiper.shutdown();
            peerServer.shutdown();

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

        @Test
        public void testMergeNewHeartbeats() {
            Map<Long, InetSocketAddress> peerIdsToAddress = new HashMap<>();
            peerIdsToAddress.put(1L, new InetSocketAddress("localhost", 8000));
            peerIdsToAddress.put(2L, new InetSocketAddress("localhost", 8010));
            peerIdsToAddress.put(3L, new InetSocketAddress("localhost", 8020));
            peerIdsToAddress.put(4L, new InetSocketAddress("localhost", 8030));

            LinkedBlockingQueue<Message> incomingMessages = new LinkedBlockingQueue<>();
            incomingMessages.add(randomGossipMessage());
            incomingMessages.add(randomWorkMessage());
            incomingMessages.add(randomGossipMessage());
            incomingMessages.add(randomGossipMessage());
            incomingMessages.add(randomWorkCompletedMessage());
            incomingMessages.add(randomGossipMessage());
            incomingMessages.add(randomElectionMessage());
            incomingMessages.add(randomGossipMessage());

            ZooKeeperPeerServerImpl peerServer = new ZooKeeperPeerServerImpl(5000, 0, 0L, peerIdsToAddress, null, 0);
            Gossiper gossiper = new Gossiper(peerServer, peerIdsToAddress, null, incomingMessages, new ReentrantLock(),
                    5050);

            assertEquals(8, incomingMessages.size());
            gossiper.mergeNewHeartbeats(incomingMessages, System.currentTimeMillis());
            assertEquals(3, incomingMessages.size());

            int workMessages = 1;
            int completedWork = 1;
            int election = 1;

            for (Message msg : incomingMessages) {
                if (msg.getMessageType() == MessageType.COMPLETED_WORK)
                    completedWork--;
                if (msg.getMessageType() == MessageType.WORK)
                    workMessages--;
                if (msg.getMessageType() == MessageType.ELECTION)
                    election--;
            }

            assertEquals(0, workMessages);
            assertEquals(0, completedWork);
            assertEquals(0, election);

            gossiper.shutdown();
            peerServer.shutdown();

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

        @Test
        public void testGossipToRandomPeer() {
            // Ensure that a peer is being gossiped to, I have no idea how to test if it is
            // at all random

            Map<Long, InetSocketAddress> peerIdsToAddress = new HashMap<>();
            peerIdsToAddress.put(1L, new InetSocketAddress("localhost", 8000));
            peerIdsToAddress.put(2L, new InetSocketAddress("localhost", 8010));
            peerIdsToAddress.put(3L, new InetSocketAddress("localhost", 8020));
            peerIdsToAddress.put(4L, new InetSocketAddress("localhost", 8030));

            LinkedBlockingQueue<Message> outgoingMessages = new LinkedBlockingQueue<>();
            InetSocketAddress myAddress = new InetSocketAddress("localhost", 9000);
            ZooKeeperPeerServerImpl peerServer = new ZooKeeperPeerServerImpl(9000, 0, 0L, peerIdsToAddress, null, 0);
            Gossiper gossiper = new Gossiper(peerServer, peerIdsToAddress, outgoingMessages, null, new ReentrantLock(),
                    9050);

            assertEquals(0, outgoingMessages.size());
            gossiper.gossipToRandomPeer();
            assertEquals(1, outgoingMessages.size());

            Map<Long, HeartbeatValue> heartbeatMap = gossiper.getHeartbeatMap();
            Message msg = outgoingMessages.poll();
            assertEquals("localhost", msg.getSenderHost());
            assertEquals(9000, msg.getSenderPort());
            assertTrue(msg.getReceiverPort() >= 8000 && msg.getReceiverPort() <= 8030);
            assertEquals("localhost", msg.getReceiverHost());

            gossiper.shutdown();
            peerServer.shutdown();

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

    }

    @Nested
    public class Stage5Demos {

        @Nested
        public class GossipFailureDetection {

            private final int NUM_SERVERS = 5;
            private final int PORT_RANGE_START = 8000;
            private final int gatewayHTTPPort = PORT_RANGE_START + NUM_SERVERS * 10;
            private final int gatewayUDPPort = PORT_RANGE_START + NUM_SERVERS * 10 + 2;
            private final long gatewayID = NUM_SERVERS;
            private ArrayList<ZooKeeperPeerServerImpl> servers;
            private GatewayServer gateway;

            // @Test
            @DisplayName("Ensure that Gossip is exchanged between all nodes and gateway and that dead failures are recognized as failed")
            public void testGossipFailureDetection() throws Exception {
                // step 1: create servers
                createServers();
                // step2: wait for servers to get started
                Thread.sleep(20_000);

                printLeaders(NUM_SERVERS - 1);

                for (int i = 1; NUM_SERVERS - i > 3; i++) {

                    System.out.println("Shutting down server " + servers.get((NUM_SERVERS -
                            i)).getServerId());
                    servers.get((NUM_SERVERS - i)).shutdown();

                    Thread.sleep(40_000); // Wait for servers to notice that it is dead
                    Thread.sleep(20_000); // Wait for servers to elect new leader
                    printLeaders((NUM_SERVERS - i) - 1);

                    System.out.println("*********************");
                }

                // step 5: stop servers
                stopServers();
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
                    ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0,
                            entry.getKey(),
                            map, this.gatewayID, 1);
                    this.servers.add(server);
                    server.start();
                }

                peerIDtoAddress.put(this.gatewayID, new InetSocketAddress("localhost", this.gatewayUDPPort));
                this.servers.add(gateway.gatewayPeerServer);

            }

            private void printLeaders(long expectedLeaderID) {
                for (ZooKeeperPeerServerImpl server : this.servers) {
                    Vote leader = server.getCurrentLeader();
                    if (leader != null && server.getState() != Thread.State.TERMINATED) {
                        System.out.println("Server on port " + server.getAddress().getPort() + " whose ID is "
                                + server.getServerId() + " has the following ID as its leader: "
                                + leader.getProposedLeaderID()
                                + " and its state is " + server.getPeerState().name());

                        // assertEquals(expectedLeaderID, leader.getProposedLeaderID(), String.format(
                        // "Server %d had the wrong leader: %d", server.getServerId(),
                        // leader.getProposedLeaderID()));
                    } else {
                        // throw new AssertionError(String.format("Server %d did not find leader",
                        // server.getServerId()));
                        System.out.println(String.format("Server %d did not find leader", server.getServerId()));
                    }
                }
            }
        }

        @Nested
        public class Stage5MainDemo {
            private String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";

            private final int NUM_SERVERS = 5;
            private final int PORT_RANGE_START = 8000;
            private final int gatewayHTTPPort = PORT_RANGE_START + NUM_SERVERS * 10;
            private final int gatewayUDPPort = PORT_RANGE_START + NUM_SERVERS * 10 + 2;
            private final long gatewayID = NUM_SERVERS;
            private ArrayList<ZooKeeperPeerServer> servers;

            private GatewayServer gateway;

            // @Test
            @DisplayName("Ensure that gateway sends work to correct leader. Ensure that followers elect the correct leader and correctly assume leadership.")
            public void Stage5PeerServerDemoLeaderDeath() throws Exception {

                cleanLogs();
                // step 1: create servers
                createServers();

                List<Future<String>> responses = new ArrayList<>();
                ExecutorService executor = Executors.newFixedThreadPool(10);
                for (int i = 0; i < NUM_SERVERS * 2; i++) {
                    final String code = this.validClass.replace("world!", "world! from code version " + i);
                    responses.add(executor.submit(new Callable<String>() {
                        @Override
                        public String call() throws Exception {
                            return sendMessage("localhost", gatewayHTTPPort, code);
                        }
                    }));
                    // System.out.println(sendMessage( "localhost", gatewayHTTPPort, code));
                }
                // step2: wait for servers to get started
                printResponses(responses);

                Thread.sleep(15_000);

                printLeaders();
                // step 3: send requests to the gatewayPort
                responses = new ArrayList<>();
                for (int i = 0; i < NUM_SERVERS * 2; i++) {
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

                System.out.println("Shutting down server " + servers.get(4).getServerId());
                servers.get(4).shutdown();

                Thread.sleep(1_000);
                responses = new ArrayList<>();
                for (int i = 0; i < NUM_SERVERS * 2; i++) {
                    final String code = this.validClass.replace("world!", "world! from code version " + i);
                    responses.add(executor.submit(new Callable<String>() {
                        @Override
                        public String call() throws Exception {
                            return sendMessage("localhost", gatewayHTTPPort, code);
                        }
                    }));
                }

                // step 4: validate responses from leader

                printResponses(responses);

                System.out.println("Got all responses!");

                responses = new ArrayList<>();
                for (int i = 0; i < NUM_SERVERS * 2; i++) {
                    final String code = this.validClass.replace("world!", "world! from code version " + i);
                    responses.add(executor.submit(new Callable<String>() {
                        @Override
                        public String call() throws Exception {
                            return sendMessage("localhost", gatewayHTTPPort, code);
                        }
                    }));
                }

                // // step 4: validate responses from leader

                // printResponses(responses);

                System.out.println("Got all responses!");

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
                                + server.getServerId() + " has the following ID as its leader: "
                                + leader.getProposedLeaderID()
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

            private String sendMessage(String hostName, int hostPort, String code)
                    throws IOException, InterruptedException {
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
                    ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0,
                            entry.getKey(),
                            map, this.gatewayID, 1);
                    this.servers.add(server);
                    server.start();
                }
                peerIDtoAddress.put(this.gatewayID, new InetSocketAddress("localhost", this.gatewayUDPPort));
                this.servers.add(gateway.gatewayPeerServer);

            }

        }

    }

    @Nested

    public class TCPServerTest {

        @Nested
        public class AsyncServerTests {

            private final int serverPort = 5000;
            private TCPServer tcpServer;

            @BeforeEach
            public void init() {
                tcpServer = new TCPServer(serverPort);
                new Thread(tcpServer).start();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            @AfterEach
            public void cleanup() {

                tcpServer.interrupt();
                try {
                    tcpServer.sendTcpMessage(null, "localhost", serverPort, false);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }

            @Test
            public void oneMsgTest() {
                // Once the server is started up, it should start accepting tcp requests
                try (Socket socket = new Socket("localhost", serverPort);) {
                    Message msg = new Message(MessageType.WORK, "Hello there! Msg 1".getBytes(), "localhost", 4000,
                            "localhost", serverPort);
                    socket.getOutputStream().write(msg.getNetworkPayload());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                    TCPMessage rcvdMsg = tcpServer.msgsFromClient.peek();
                    assertEquals(1, tcpServer.msgsFromClient.size(), "TCPServer should have one msg");
                    assertArrayEquals("Hello there! Msg 1".getBytes(), rcvdMsg.msg.getMessageContents());

                    Message response = new Message(MessageType.WORK, "Server response!".getBytes(), "localhost",
                            serverPort,
                            "localhost", 4000);
                    tcpServer.respondToClient(rcvdMsg.id, response);

                    Message rcvdResponse = new Message(Util.readAllBytesFromNetwork(socket.getInputStream()));
                    assertArrayEquals("Server response!".getBytes(), rcvdResponse.getMessageContents());

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Test
            public void testConcurrentConversations() {

                ExecutorService es = Executors.newFixedThreadPool(5);
                CompletionService<Boolean> completionService = new ExecutorCompletionService<>(es);

                // Phaser to synchronize sending, checking, sending, checking
                Phaser phaser = new Phaser(1);

                for (int i = 0; i < 5; i++) {
                    final int id = i;
                    final String msgContents = "Hello there! Msg " + id;
                    final String responseContents = "Server response!" + msgContents;
                    phaser.register();
                    completionService.submit(() -> {
                        try (Socket socket = new Socket("localhost", serverPort);) {
                            Message msg = new Message(MessageType.WORK, msgContents.getBytes(), "localhost", 4000,
                                    "localhost", serverPort);
                            socket.getOutputStream().write(msg.getNetworkPayload());
                            // Wait until all threads have sent the messages
                            phaser.arriveAndAwaitAdvance();

                            Message rcvdResponse = new Message(Util.readAllBytesFromNetwork(socket.getInputStream()));
                            assertArrayEquals(responseContents.getBytes(), rcvdResponse.getMessageContents());

                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        return true;
                    });
                }

                // Wait for all threads to have sent their msgs
                phaser.arriveAndAwaitAdvance();
                // Give the server some time to process all the connections
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                }
                assertEquals(5, tcpServer.msgsFromClient.size());
                for (TCPMessage msg : tcpServer.msgsFromClient) {
                    byte[] responseBody = ("Server response!" + new String(msg.msg.getMessageContents())).getBytes();
                    Message response = new Message(MessageType.WORK, responseBody, "localhost", serverPort, "localhost",
                            4000);
                    tcpServer.respondToClient(msg.id, response);
                }
                for (int i = 0; i < 5; i++) {
                    try {
                        assertTrue(completionService.take().get());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                }

            }
        }

    }

    public byte[] buildMsgContent(ElectionNotification notification) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 3 + 2);
        buffer.putLong(notification.getProposedLeaderID());
        buffer.putChar(notification.getState().getChar());
        buffer.putLong(notification.getSenderID());
        buffer.putLong(notification.getPeerEpoch());
        return buffer.array();
    }

    public Message randomGossipMessage() {
        Map<Long, HeartbeatValue> contents = new HashMap<>();

        contents.put(1L, new HeartbeatValue(1L, 10L, System.currentTimeMillis()));
        contents.put(2L, new HeartbeatValue(1L, -10L, System.currentTimeMillis()));
        contents.put(3L, new HeartbeatValue(1L, 10L, System.currentTimeMillis()));
        contents.put(4L, new HeartbeatValue(1L, -10L, System.currentTimeMillis()));

        return new Message(MessageType.GOSSIP, Gossiper.heartbeatMapToByteArr(contents), "localhost", 8000, "localhost",
                8000);
    }

    public Message randomWorkMessage() {
        String contents = "to be or not to be that is the questions";
        return new Message(MessageType.WORK, contents.getBytes(), "localhost", 8000, "localhost", 8000);
    }

    public Message randomWorkCompletedMessage() {
        String contents = "to be or not to be that is the questions";
        return new Message(MessageType.COMPLETED_WORK, contents.getBytes(), "localhost", 8000, "localhost", 8000);
    }

    public Message randomElectionMessage() {
        String contents = "to be or not to be that is the questions";
        return new Message(MessageType.ELECTION, contents.getBytes(), "localhost", 8000, "localhost", 8000);
    }

    private void cleanLogs() {
        File dir = new File("./logs");
        for (File file : dir.listFiles())
            if (!file.isDirectory())
                file.delete();
    }

}
