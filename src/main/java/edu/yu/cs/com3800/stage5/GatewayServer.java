package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import edu.yu.cs.com3800.LoggingServer;

import com.sun.net.httpserver.HttpServer;

public class GatewayServer implements LoggingServer {

    private final int httpPort;
    private final HttpServer server;
    private final ExecutorService threadPool;
    private final Logger logger;
    public final GatewayPeerServerImpl gatewayPeerServer;

    private final AtomicLong nextWorkId = new AtomicLong(0);

    public GatewayServer(int httpPort, int udpPort, long id, Map<Long, InetSocketAddress> peerIdToAddress) {
        this.httpPort = httpPort;

        // Startup the peer server than will observe the cluster and keep track of the
        // leader
        gatewayPeerServer = new GatewayPeerServerImpl(udpPort, 0, id, peerIdToAddress, 1);
        gatewayPeerServer.setName("GatewayPeerServerMaster");
        gatewayPeerServer.start();

        try {
            this.logger = initializeLogging(
                    "edu.yu.cs.com3800.stage5.GatewayServer-id-" + id + "-httpPort-" + httpPort);
        } catch (IOException e) {
            throw new RuntimeException("Uncaught", e);
        }

        // Startup the HTTP server listening on the port passed into the constructor
        try {
            this.logger.info("Creating and starting server on " + this.httpPort);
            this.server = HttpServer.create(new InetSocketAddress(this.httpPort), 0);
            this.server.createContext("/compileandrun", new CompileAndRunHandler(this.gatewayPeerServer, this.logger, this.nextWorkId));
            this.server.createContext("/getcluster", new GetClusterHandler(peerIdToAddress, gatewayPeerServer));
            this.threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                    new FixedDaemonThreadFactory());
            this.server.setExecutor(this.threadPool);
            this.server.start();
        } catch (IOException e) {
            throw new RuntimeException("Uncaught", e);
        }

    }

    public void shutdown() {
        this.logger.info("Gateway shutting down");
        this.gatewayPeerServer.shutdown();
        this.server.stop(1);
        this.threadPool.shutdownNow();
    }

    
public static void main(String[] args) {

    /*
     * User input will look like:
     * java GatewayServer.java httpPort udpPort id NUM_SERVERS id1 hostname port id2
     * hostname port ...
     */

    final int NUM_SERVERS = Integer.parseInt(args[3]);
    HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>();
    for (int i = 0; i < NUM_SERVERS; i++) {
        long id = Long.parseLong(args[i * 3 + 4]);
        String hostname = args[i * 3 + 5];
        int port = Integer.parseInt(args[i * 3 + 6]);
        peerIDtoAddress.put(id, new InetSocketAddress(hostname, port));
    }

    int gatewayHTTPPort = Integer.parseInt(args[0]);
    int gatewayUDPPort = Integer.parseInt(args[1]);
    Long gatewayID = Long.parseLong(args[2]);

    new GatewayServer(gatewayHTTPPort, gatewayUDPPort, gatewayID, new HashMap<>(peerIDtoAddress));

}

}
