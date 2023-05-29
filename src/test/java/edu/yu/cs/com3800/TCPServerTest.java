package edu.yu.cs.com3800;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.stage5.TCPServer;
import edu.yu.cs.com3800.stage5.TCPServer.TCPMessage;

public class TCPServerTest {

    // The TCP Server has two modes: Sync and Async mode

    // In Sync mode the user calls the blockTillConnection() method which starts
    // up a serversocket and returns the socket when someone connects.
    // It is up to the user to read the message, respond, and close the socket
    // Additionally, the user can call the sendTcpMessage method which acts
    // as a tcpClient and retuns the result of connecting, sending the message,
    // and reading all available bytes from the network

    @Nested
    public class AsyncServerTests {
        // In Async mode the tcp server is started as a thread and it starts up a
        // serversocket
        // on the port that it is given in the constructor
        // Whenever it receves a connection, it reads a message from the socket,
        // associates the socket with an ID and places the message on a queue.
        // It also stores the socket in a mapping from ID -> Socket.
        // The user can start up the TCPServer and poll the queue. When it receives a
        // message
        // it can handle the request and response by providing a message and the id
        // of the socket.

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

                Message response = new Message(MessageType.WORK, "Server response!".getBytes(), "localhost", serverPort,
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
                Message response = new Message(MessageType.WORK, responseBody, "localhost", serverPort, "localhost", 4000);
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
