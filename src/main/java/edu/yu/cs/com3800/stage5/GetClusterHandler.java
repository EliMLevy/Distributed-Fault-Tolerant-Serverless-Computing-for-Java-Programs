package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState;

public class GetClusterHandler implements HttpHandler {

    private final Map<Long, InetSocketAddress> peerIdToAddress;
    private final GatewayPeerServerImpl peerServer;

    public GetClusterHandler(Map<Long, InetSocketAddress> peerIdToAddress, GatewayPeerServerImpl peerServer) {
        this.peerIdToAddress = peerIdToAddress;
        this.peerServer = peerServer;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {

        if (this.peerServer.getCurrentLeader() == null
                || this.peerServer.isFailed(this.peerServer.getCurrentLeader().getProposedLeaderID())) {
            byte[] responseBytes = "Gateway does not have a leader yet".getBytes();
            exchange.sendResponseHeaders(200, responseBytes.length);
            OutputStream os = exchange.getResponseBody();
            os.write(responseBytes);
            os.close();
        }

        Map<Long, String> states = new ConcurrentHashMap<>();
        states.put(this.peerServer.getServerId(), this.peerServer.getPeerState().toString());
        ExecutorService httpPool = Executors.newFixedThreadPool(peerIdToAddress.size());
        CountDownLatch finished = new CountDownLatch(peerIdToAddress.size());
        for (Long peerid : peerIdToAddress.keySet()) {
            if (this.peerServer.isFailed(peerid)) {
                // states.put(peerid, "FAILED");
                finished.countDown();
            } else {
                InetSocketAddress addr = peerIdToAddress.get(peerid);
                httpPool.submit(() -> {
                    HttpClient client = HttpClient.newHttpClient();
                    HttpRequest request = HttpRequest.newBuilder()
                            .GET()
                            .uri(URI.create("http://" + addr.getHostName() + ":" + (addr.getPort() + 1) + "/stateinfo"))
                            .build();
                    try {
                        HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
                        String resBody = new String(response.body());
                        char stateChar = resBody.charAt(0);
                        ServerState state = ServerState.getServerState(stateChar);
                        states.put(peerid, state.toString());
                    } catch (IOException | InterruptedException e) {
                        // states.put(peerid,e.getMessage());
                    }
                    finished.countDown();
                });
            }
        }

        try {
            finished.await();
        } catch (InterruptedException e) {
            throw new RuntimeException("Uncaught", e);
        }

        httpPool.shutdownNow();

        String response = states.toString();
        byte[] responseBytes = response.getBytes();

        exchange.sendResponseHeaders(200, responseBytes.length);
        OutputStream os = exchange.getResponseBody();
        os.write(responseBytes);
        os.close();
    }
}
