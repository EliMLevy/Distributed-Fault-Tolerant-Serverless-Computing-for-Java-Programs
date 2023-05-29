package edu.yu.cs.com3800.stage5;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class Gossiper extends Thread implements LoggingServer {

    private final Map<Long, HeartbeatValue> heartbeatMap;
    private long heartbeatCounter = 0;
    static final int GOSSIP = 3000;
    static final int FAIL = GOSSIP * 10;
    static final int CLEANUP = FAIL * 2;

    private final LinkedBlockingQueue<Message> incomingMessages;
    private final LinkedBlockingQueue<Message> outgoingMessages;

    private final Set<Long> markedForCleanup;
    public Set<Long> failedServers;
    private final Map<Long, InetSocketAddress> peerIDtoAddress;
    public final Map<InetSocketAddress, Long> addressToPeerID;

    private final Logger summarylogger;
    private final Logger verboselogger;

    private final ZooKeeperPeerServerImpl myPeerServer;

    private final HttpServer httpserver;
    private final int httpPort;
    private final ExecutorService httpthreadPool;
    private final Lock incomingMessagesLock;

    public Gossiper(ZooKeeperPeerServerImpl myPeerServer, Map<Long, InetSocketAddress> peerIDtoAddress,
            LinkedBlockingQueue<Message> outgoingMessages,
            LinkedBlockingQueue<Message> incomingMessages, Lock incomingMessagesLock, int peerServerPort) {

        this.myPeerServer = myPeerServer;
        this.peerIDtoAddress = peerIDtoAddress;
        this.incomingMessagesLock = incomingMessagesLock;
        addressToPeerID = new HashMap<>();
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            addressToPeerID.put(entry.getValue(), entry.getKey());
        }
        this.outgoingMessages = outgoingMessages;
        this.incomingMessages = incomingMessages;

        this.heartbeatMap = new HashMap<>();
        for (long id : peerIDtoAddress.keySet()) {
            this.heartbeatMap.put(id, new HeartbeatValue(id, heartbeatCounter, System.currentTimeMillis()));
        }

        // Only accessed by this object in this thread so doesnt need to be thread safe
        this.markedForCleanup = new HashSet<>();
        // This is accessed by the peerServerImpl to check for failures so it does need
        // to be thread safe
        this.failedServers = ConcurrentHashMap.newKeySet();

        String summaryLoggerName = "edu.yu.cs.com3800.stage5.GossipSummaryLog-UDPPort-" + peerServerPort;
        try {
            this.summarylogger = initializeLogging(summaryLoggerName);
        } catch (IOException e) {
            throw new RuntimeException("Uncaught", e);
        }

        String verboseLoggerName = "edu.yu.cs.com3800.stage5.GossipVerboseLog-UDPPort-" + peerServerPort;
        try {
            this.verboselogger = initializeLogging(verboseLoggerName);
        } catch (IOException e) {
            throw new RuntimeException("Uncaught", e);
        }

        this.httpPort = peerServerPort + 1;
        try {
            this.httpserver = HttpServer.create(new InetSocketAddress(this.httpPort), 0);
            String logDir = System.getProperty("logdir", "./logs/");

            this.httpserver.createContext("/logsummary",
                    new LogFileHttpHandler(logDir + summaryLoggerName + ".log"));
            this.httpserver.createContext("/logdetails",
                    new LogFileHttpHandler(logDir + verboseLoggerName + ".log"));
            this.httpserver.createContext("/stateinfo", (exchange) -> {
                char responseBytes = this.myPeerServer.getPeerState().getChar();
                exchange.sendResponseHeaders(200, 1);
                OutputStream os = exchange.getResponseBody();
                os.write(responseBytes);
                os.close();
            });
            this.httpthreadPool = Executors.newFixedThreadPool(1, new FixedDaemonThreadFactory());

            this.httpserver.setExecutor(this.httpthreadPool);
            this.httpserver.start();
        } catch (IOException e) {
            throw new RuntimeException("Uncaught", e);
        }
    }

    public void shutdown() {
        this.httpserver.stop(1);
        this.httpthreadPool.shutdownNow();
    }

    /*
     * on each iteration of your gossiper's run loop it should:
     * 1) merge in to its records all new heartbeats  / gossip info that the UDP
     * receiver has
     * 2) check for failures, using the records it has
     * 3) clean up old failures that have reached cleanup time
     * 4) gossip to a random peer
     * 5) sleep for the heartbeat/gossip interval
     */
    @Override
    public void run() {

        while (!this.isInterrupted()) {
            this.heartbeatCounter++;
            long timeNow = System.currentTimeMillis();

            this.mergeNewHeartbeats(this.incomingMessages, timeNow);

            this.checkForFailures(timeNow);

            this.cleanUpOldFailures(timeNow);

            this.gossipToRandomPeer(this.heartbeatMap, this.peerIDtoAddress, this.outgoingMessages,
                    this.myPeerServer.getAddress());

            try {
                Thread.sleep(Gossiper.GOSSIP);
            } catch (InterruptedException e) {
                break;
            }

        }
    }

    public void gossipToRandomPeer() {
        this.gossipToRandomPeer(this.heartbeatMap, this.peerIDtoAddress, this.outgoingMessages,
                this.myPeerServer.getAddress());
    }

    public void stateChange(ServerState oldState, ServerState newState) {
        String logMsg = String.format("[%d]: switching from [%s] to [%s]", this.myPeerServer.getServerId(), oldState,
                newState);
        this.summarylogger.info(logMsg);
    }

    public void gossipToRandomPeer(Map<Long, HeartbeatValue> heartbeatMap,
            Map<Long, InetSocketAddress> peerIDtoAddress,
            LinkedBlockingQueue<Message> outgoingMessages,
            InetSocketAddress myAddress) {
        // Choose a random id to gossip to
        Map<Long, HeartbeatValue> livingPeers = this.heartbeatMap.entrySet().stream()
                .filter(e -> !this.markedForCleanup.contains(e.getKey()))
                .filter(e -> !this.failedServers.contains(e.getKey()))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        List<Long> candidateIds = livingPeers.keySet().stream()
                .collect(Collectors.toList());
        if (candidateIds.size() == 0) {
            this.myPeerServer.logger.warning("NO LIVING PEERS!!!");
        }
        long id = chooseRandomElement(candidateIds);
        InetSocketAddress to = peerIDtoAddress.get(id);

        // Convert the heartbeat map to a byte array
        byte[] data = Gossiper.heartbeatMapToByteArr(livingPeers);

        // Send a UDP message to that
        Message outgoing = new Message(MessageType.GOSSIP, data,
                myAddress.getHostName(), myAddress.getPort(),
                to.getHostName(), to.getPort());

        // Check if the message was sent successfully
        if (!outgoingMessages.offer(outgoing)) {
            this.myPeerServer.logger.warning("Failed to send gossip message to server " + id);
        }
    }

    private long chooseRandomElement(List<Long> candidateIds) {
        Random rndm = new Random();
        int rndmNumber = rndm.nextInt(candidateIds.size());
        return candidateIds.get(rndmNumber);
    }

    public int cleanUpOldFailures(long timeNow) {
        Set<Long> toRemove = this.markedForCleanup.stream()
                .filter(e -> this.heartbeatMap.get(e) != null)
                .filter(e -> timeNow - this.heartbeatMap.get(e).timestamp > FAIL + CLEANUP)
                .collect(Collectors.toSet());
        toRemove.forEach(this.markedForCleanup::remove);
        return toRemove.size();
    }

    public void checkForFailures(long timeNow) {
        // check for failures, using the records it has
        this.heartbeatMap.entrySet().stream()
                .filter(entry -> timeNow - entry.getValue().timestamp > FAIL)
                .filter(entry -> !this.failedServers.contains(entry.getKey()))
                .forEach(entry -> {
                    this.markedForCleanup.add(entry.getKey());
                    this.failedServers.add(entry.getKey());
                    this.myPeerServer.newFailure(entry.getKey());
                    String logMsg = String.format("[%d]: no heartbeat from server [%d] - server failed",
                            this.myPeerServer.getServerId(), entry.getKey());
                    this.summarylogger.info(logMsg);
                    System.out.println(logMsg);
                });

    }

    public void mergeNewHeartbeats(LinkedBlockingQueue<Message> incomingMessages, long timeNow) {
        if (this.incomingMessages.size() == 0)
            return;
        byte[] poisinPillBytes = "GOSSIP POISIN PILL".getBytes();
        Message poisonPill = new Message(MessageType.GOSSIP, poisinPillBytes, null, 0, null, 0);

        // This method was contending for the the queue with the election so I needed to
        // add some synchronization here
        this.incomingMessagesLock.lock();
        incomingMessages.offer(poisonPill);
        int messagesProcessed = 0;
        // We have locked the queue so we really shouldnt get stuck in an infinite loop
        // but lets be safe
        int MAX_MESSAGES_TO_PROCESS = (incomingMessages.size() + 1) * 4;
        while (messagesProcessed < MAX_MESSAGES_TO_PROCESS) {
            Message msg = incomingMessages.poll();
            if (msg != null) {
                if (msg.getMessageType() == MessageType.GOSSIP) {
                    byte[] contents = msg.getMessageContents();
                    // If this is the poisin pill, break
                    if (Arrays.equals(contents, poisinPillBytes))
                        break;
                    InetSocketAddress senderAddress = new InetSocketAddress(msg.getSenderHost(), msg.getSenderPort());
                    long senderId = this.addressToPeerID.get(senderAddress);
                    if (this.markedForCleanup.contains(senderId) || this.failedServers.contains(senderId))
                        continue;
                    this.receivedMsgFrom(msg.getSenderHost(), msg.getSenderPort(), timeNow);
                    this.handleGossipMessage(senderId, contents, timeNow);
                } else {
                    // If its not gossip put it back on the end
                    if (!incomingMessages.offer(msg)) {
                        this.myPeerServer.logger
                                .warning("Failed to put non gossip message back on queue. " + msg.toString());
                    }
                }
            }
            messagesProcessed++;
        }
        this.incomingMessagesLock.unlock();
    }

    public void receivedMsgFrom(String host, int port, long timeNow) {
        InetSocketAddress sender = new InetSocketAddress(host, port);
        this.heartbeatMap.put(addressToPeerID.get(sender),
                new HeartbeatValue(addressToPeerID.get(sender), this.heartbeatCounter, timeNow));
    }

    public void handleGossipMessage(long senderId, byte[] contents, long timeNow) {
        Map<Long, Long> rcvdMap = Gossiper.byteArrToHeartbeatMap(contents);
        this.verboselogger.info(String.format("From: %d; Received at: %d; Contents: %s", senderId, timeNow, rcvdMap));

        rcvdMap.entrySet().stream()
                .filter(e -> this.heartbeatMap.containsKey(e.getKey())) // Eliminate gossip about servers that were
                                                                        // cleaned up
                .filter(e -> !this.markedForCleanup.contains(e.getKey())) // Eliminate gossip about servers that are
                                                                          // marked for cleanup
                .filter(e -> e.getValue() > this.heartbeatMap.get(e.getKey()).counter) // Get the entries that contain
                                                                                       // useful info
                .forEach(entry -> {
                    String logMsg = String.format(
                            "[%d]: updated [%d]'s heartbeat sequence to [%d] based on message from [%d] at node time [%d]",
                            this.myPeerServer.getServerId(), entry.getKey(), entry.getValue(), senderId, timeNow);
                    this.summarylogger.info(logMsg);
                    this.heartbeatMap.put(entry.getKey(),
                            new HeartbeatValue(entry.getKey(), entry.getValue(), timeNow));
                });
    }

    public static byte[] heartbeatMapToByteArr(Map<Long, HeartbeatValue> map) {
        // Serialize our heartbeat map
        ByteBuffer contents = ByteBuffer.allocate(map.size() * 2 * Long.BYTES);
        for (Map.Entry<Long, HeartbeatValue> entry : map.entrySet()) {
            contents.putLong(entry.getKey());
            contents.putLong(entry.getValue().counter);
        }

        return contents.array();
    }

    public static Map<Long, Long> byteArrToHeartbeatMap(byte[] arr) {
        // Deserialize our heartbeat map
        ByteBuffer contents = ByteBuffer.wrap(arr);
        Map<Long, Long> map = new HashMap<>();
        while (contents.hasRemaining()) {
            map.put(contents.getLong(), contents.getLong());
        }

        return map;
    }

    public Map<Long, HeartbeatValue> getHeartbeatMap() {
        return new HashMap<>(this.heartbeatMap);
    }

    public static class HeartbeatValue {

        public Long id;
        public Long counter;
        public Long timestamp;

        public HeartbeatValue(Long id, Long counter, Long timestamp) {
            this.id = id;
            this.counter = counter;
            this.timestamp = timestamp;
        }

        public String toString() {
            return String.format("{id:%d; Counter:%d; Timestamp:%d}", this.id, this.counter, this.timestamp);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof HeartbeatValue))
                return false;

            HeartbeatValue other = (HeartbeatValue) o;

            return this.id == other.id && this.counter == other.counter && this.timestamp == other.timestamp;
        }
    }

    public class LogFileHttpHandler implements HttpHandler {

        private String path;

        public LogFileHttpHandler(String path) {
            this.path = path;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            StringBuilder logFileContents = new StringBuilder();

            try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    logFileContents.append(line).append("\n");
                }
            } catch (IOException e) {
                logFileContents.append("Error reading log file: " + e.getMessage());
            }

            byte[] responseBytes = logFileContents.toString().getBytes();
            exchange.sendResponseHeaders(200, responseBytes.length);
            OutputStream os = exchange.getResponseBody();
            os.write(responseBytes);
            os.close();

        }

    }
}
