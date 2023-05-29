package edu.yu.cs.com3800.stage5;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Phaser;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Vote;
import edu.yu.cs.com3800.ZooKeeperPeerServer;
import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.stage5.TCPServer.TCPMessage;

public class JavaRunnerFollower extends Thread {

    private final ZooKeeperPeerServerImpl myPeerServer;
    private final JavaRunner javaRunner;
    private final Logger logger;
    private final TCPServer tcpSenderReceiver;

    private final Queue<Message> waitingToBeSent;
    private final Phaser electionBlocker;

    public JavaRunnerFollower(ZooKeeperPeerServerImpl myPeerServer, Phaser electionBlocker, TCPServer tcpServer) {
        this.myPeerServer = myPeerServer;
        this.electionBlocker = electionBlocker;

        this.tcpSenderReceiver = tcpServer;
        this.waitingToBeSent = new LinkedList<>();

        try {
            this.javaRunner = new JavaRunner();
        } catch (Exception e) {
            throw new RuntimeException("Uncaught", e);
        }

        try {
            this.logger = this.myPeerServer.initializeLogging("edu.yu.cs.com3800.stage5.JavaRunnerFollower-id-"
                    + this.myPeerServer.getServerId() + "-TcpPort-" + this.myPeerServer.getTcpPort());
        } catch (IOException e) {
            throw new RuntimeException("Uncaught", e);
        }

    }

    @Override
    public void run() {
        this.startAcceptingWork();
    }

    /**
     * This methods enters a loop as long as the server is not interrupted and is in
     * the FOLLOWING state.
     * The server will:
     * - act as a tcp server and wait for a connection on its tcp port
     * - extract the request body from the message it receives
     * - compile and run the request body
     * - send back a message with the response as the body
     */
    public void startAcceptingWork() {
        this.electionBlocker.register();
        this.logger.info("Starting to accept work. Listening on port " + this.myPeerServer.getTcpPort());
        while (this.myPeerServer.getPeerState() == ZooKeeperPeerServer.ServerState.FOLLOWING
                || this.myPeerServer.getPeerState() == ZooKeeperPeerServer.ServerState.LOOKING) {
            this.electionBlocker.arriveAndAwaitAdvance();
            if (this.myPeerServer.getPeerState() != ZooKeeperPeerServer.ServerState.FOLLOWING
                    && this.myPeerServer.getPeerState() != ZooKeeperPeerServer.ServerState.LOOKING) {
                if (this.myPeerServer.getCurrentLeader() != null && this.waitingToBeSent.size() > 0) {
                    sendWaitingResponses();
                }
                break;
            }
            try {
                // This will give us the ID of the dialogue and the Message object
                TCPMessage msg = this.tcpSenderReceiver.poll(500);

                // If we have a leader and we have results waiting to be sent. Send them
                if (this.myPeerServer.getCurrentLeader() != null && this.waitingToBeSent.size() > 0) {
                    sendWaitingResponses();
                }

                if (msg != null) {
                    this.logger.info(String.format("Message from port %d received on port %d", msg.msg.getSenderPort(),
                            msg.msg.getReceiverPort()));

                    InetSocketAddress fromAddress = new InetSocketAddress(msg.msg.getSenderHost(),
                            msg.msg.getSenderPort() - 2);
                    // Ignore messages from dead peers
                    if (this.myPeerServer.isFailed(fromAddress)) {
                        this.tcpSenderReceiver.respondToClient(msg.id, null);
                    } else {
                        MessageType msgType = msg.msg.getMessageType();
                        if (msgType == MessageType.WORK) {
                            this.logger.info("Received work message. DialogueID: " + msg.id + ". WorkID: "
                                    + msg.msg.getRequestID());
                            // Process the work request
                            final Vote currentLeader = this.myPeerServer.getCurrentLeader();
                            Message response = this.processWorkRequest(msg.msg, this.myPeerServer.getAddress());
                            
                            if(currentLeader == null && this.myPeerServer.getCurrentLeader() != null) {
                                this.tcpSenderReceiver.respondToClient(msg.id, response);
                            }
                            
                            // If leader has died or changed place it on queue
                            if (this.myPeerServer.getCurrentLeader() == null ||
                                    this.myPeerServer.isFailed(currentLeader.getProposedLeaderID())
                                    || this.myPeerServer.getCurrentLeader().getProposedLeaderID() != currentLeader
                                            .getProposedLeaderID()) {
                                this.waitingToBeSent.add(response);
                                this.tcpSenderReceiver.respondToClient(msg.id, null);
                            } else {
                                // Else, Send it to the leader
                                this.tcpSenderReceiver.respondToClient(msg.id, response);
                            }
                        } else if (msgType == MessageType.COMPLETED_WORK
                                || msgType == MessageType.NEW_LEADER_GETTING_LAST_WORK) {
                            this.logger.info("Received message that should be going to leader. DialogueID: " + msg.id
                                    + ". WorkID: " + msg.msg.getRequestID());
                            // The only reason a worker would be receiving these is if other nodes
                            // elected this server as the leader before he elected himself the laeder.
                            // this is a precarious situation. Place these items back into the msg queue
                            // and go to sleep. Hopefully we will wake up as the leader...
                            this.tcpSenderReceiver.putBackMessage(msg);
                            Thread.sleep(1000);
                        } else if (msgType == MessageType.GOSSIP || msgType == MessageType.ELECTION) {
                            // The only reason one of these will show up in my TCP queue is if something
                            // is terribly terribly terribly terribly terribly wrong. Fail very loudly if
                            // this happens
                            this.logger.severe("Received an election or gossip message in TCP queue");
                            throw new IllegalStateException(
                                    "Election or Gossip message being sent over TCP?? What happened bro? "
                                            + msg.msg.toString());
                        } else {
                            // Let us pray that this block is never reached....
                            this.logger.severe("Foreign message type received. " + msg.msg.toString());
                            throw new IllegalStateException("Foreign message type received. " + msg.msg.toString());
                        }
                    }
                }

            } catch (InterruptedException e) {
                break;
            }
        }
        this.logger.info("Follower exitting. Goodbye! " + this.myPeerServer.getPeerState());

    }

    private void sendWaitingResponses() {
        // We are assuming that if we are in this method we have a leader
        // A more careful approach would recheck the status of the leader because maybe
        // the leader died again
        // I dont think I need to worry about leaders dying in quick succession... so I
        // wont...
        long leaderId = this.myPeerServer.getCurrentLeader().getProposedLeaderID();
        InetSocketAddress leaderAddr = this.myPeerServer.getPeerByID(leaderId);
        Message curr = this.waitingToBeSent.poll();
        while (curr != null) {
            try {
                this.logger.info("Attempting to send NLGLW workId " + curr.getRequestID() + " to leader");
                Message newMsg = new Message(MessageType.NEW_LEADER_GETTING_LAST_WORK, curr.getMessageContents(),
                        this.myPeerServer.getAddress().getHostName(), this.myPeerServer.getAddress().getPort(),
                        leaderAddr.getHostName(), leaderAddr.getPort(), curr.getRequestID(), curr.getErrorOccurred());
                this.tcpSenderReceiver.sendTcpMessage(newMsg, leaderAddr.getHostName(),
                        leaderAddr.getPort() + 2, false);
            } catch (IOException e) {
                this.logger.log(Level.INFO, "Failed to send result that was computed for last leader", e);
                this.waitingToBeSent.add(curr);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                }
            }
            curr = this.waitingToBeSent.poll();
        }
    }

    /**
     * @param msg         The Message object that contains the client request
     * @param fromAddress The adress of this server
     * @return The result of compile and run wrapped in a new Message object, ready
     *         to be sent back to leader
     */
    public Message processWorkRequest(Message msg, InetSocketAddress fromAddress) {
        long workId = msg.getRequestID();
        String response;
        boolean errorOccurred = false;
        try {
            response = this.javaRunner.compileAndRun(new ByteArrayInputStream(msg.getMessageContents()));
        } catch (Exception e) {
            StringWriter stackTraceWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stackTraceWriter);
            e.printStackTrace(printWriter);
            response = e.getMessage() + "\n" + stackTraceWriter.toString();
            errorOccurred = true;
        }
        Message outgoingMessage = new Message(MessageType.COMPLETED_WORK, response.getBytes(),
                fromAddress.getHostName(), fromAddress.getPort(), msg.getSenderHost(), msg.getSenderPort(), workId,
                errorOccurred);

        return outgoingMessage;

    }

}
