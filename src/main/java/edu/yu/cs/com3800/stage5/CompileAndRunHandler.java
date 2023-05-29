package edu.yu.cs.com3800.stage5;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import edu.yu.cs.com3800.Message;


public class CompileAndRunHandler implements HttpHandler {

    private final GatewayPeerServerImpl peerServer;
    private final Logger gatewayLogger;

    private final int RETRY_TIME = 2_000;
    private final AtomicLong nextWorkId;

    public CompileAndRunHandler(GatewayPeerServerImpl peerServer, Logger gatewayLogger, AtomicLong nextWorkId) {
        this.peerServer = peerServer;
        this.gatewayLogger = gatewayLogger;
        this.nextWorkId = nextWorkId;
    }

    @Override
    public void handle(HttpExchange exch) throws IOException {
        this.gatewayLogger.info("Received request");

        String response = "Default response";
        int code = 500;
        if (exch.getRequestMethod().toUpperCase().equals("POST")) {

            Map<String, String> headerMap = extractHeaders(exch);
            boolean validHeaders = validateHeaders(headerMap);
            if (!validHeaders) {
                sendResponse(exch, 400, "Bad Request");
            }

            InputStream is = exch.getRequestBody();
            String input = new String(is.readAllBytes());

            try {
                while (this.peerServer.isInElection()) {
                    this.gatewayLogger.info("In election! Sleeping!");
                    try {
                        Thread.sleep(RETRY_TIME);
                    } catch (InterruptedException e) {
                    }
                }
                final long workId = nextWorkId.getAndIncrement();
                Message rawResponse = this.peerServer.delegateWork(input, workId);
                while (rawResponse == null) {
                    try {
                        Thread.sleep(RETRY_TIME);
                    } catch (InterruptedException e) {
                    }
                    // we got null because the leader changed mid processing.
                    this.gatewayLogger.info("Got null response. Retrying");
                    rawResponse = this.peerServer.delegateWork(input, workId);

                }
                try {
                    response = new String(rawResponse.getMessageContents());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                code = rawResponse.getErrorOccurred() ? 400 : 200;
            } catch (IOException e) {
                code = 500;
                response = "Internal server error";
                this.gatewayLogger.log(Level.WARNING, "Request threw exception:", e);
            } catch (Exception e1) {
                e1.printStackTrace();
            } finally {
                this.sendResponse(exch, code, response);
            }
        } else {
            this.sendResponse(exch, 405, "Method not allowed");
        }

    }

    private boolean validateHeaders(Map<String, String> headerMap) {
        String contentType = headerMap.get("CONTENT-TYPE");
        if (contentType == null) {
            return false;
        }
        if (!contentType.equals("TEXT/X-JAVA-SOURCE")) {
            return false;
        }
        return true;
    }

    private Map<String, String> extractHeaders(HttpExchange exch) {
        Map<String, String> headerMap = new HashMap<>();
        for (String key : exch.getRequestHeaders().keySet()) {
            String upperKey = key.toUpperCase();
            String upperValue = exch.getRequestHeaders().get(key).get(0).toUpperCase();
            headerMap.put(upperKey, upperValue);
        }
        return headerMap;
    }

    /**
     * @param exchange The HTTPExchange object that holds the OutputStream to write
     *                 to
     * @param code     The status code to be returned to client
     * @param body     The response body to be returned
     * @throws IOException If there is an error writing to output stream
     */
    private void sendResponse(HttpExchange exchange, int code, String body) throws IOException {
        this.gatewayLogger.info("Final response: " + code + " " + body);
        exchange.sendResponseHeaders(code, body.length());
        OutputStream os = exchange.getResponseBody();
        os.write(body.getBytes());
        os.close();
    }
}
