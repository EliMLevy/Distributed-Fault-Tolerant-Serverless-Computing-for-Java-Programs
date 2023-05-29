package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.logging.Logger;

import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Vote;
import edu.yu.cs.com3800.Message.MessageType;

public class GatewayPeerServerImpl extends ZooKeeperPeerServerImpl {

    private final TCPServer tcpSenderReceiver;
    private final Logger gatewayLogger;

    /**
     * @param myUdpPort
     * @param peerEpoch
     * @param id
     * @param peerIDtoAddress
     */
    public GatewayPeerServerImpl(int myUdpPort, long peerEpoch, Long id, Map<Long, InetSocketAddress> peerIDtoAddress,
            int numberOfObservers) {
        super(myUdpPort, peerEpoch, id, peerIDtoAddress, id, numberOfObservers);

        try {
            this.gatewayLogger = initializeLogging(
                    "edu.yu.cs.com3800.stage5.GatewayPeerServerImpl-id-" + id + "-tcpPort-" + this.getTcpPort());
        } catch (IOException e) {
            throw new RuntimeException("Uncaught", e);
        }

        this.tcpSenderReceiver = new TCPServer(myUdpPort + 2);
        super.setPeerState(ServerState.OBSERVER);
    }

    @Override
    public void setPeerState(ServerState state) {
    }

    /**
     * This method will always send the message to the peer who it believes to be
     * the current leader
     * 
     * @param input The string representing the client's request
     * @return The byte[] representing the response to the client
     * @throws IOException If it fails to connect to the leader an IOException will
     *                     be trown
     */
    public Message delegateWork(String input, long workId) throws IOException {
        return this.delegateWork(input, this.tcpSenderReceiver, workId);
    }

    public Message delegateWork(String input, TCPServer tcpServer, long workId) throws IOException {
        final Vote currentLeader = this.getCurrentLeader();
        // If we dont have a leader return null so the gateway rerequests after waiting a bit
        if(currentLeader == null) return null;

        InetSocketAddress from = this.getAddress();
        // A more careful implementation would make sure that we STILL have a leader and he hasnt 
        // failed in between the last two instructions.
        InetSocketAddress to = this.getPeerByID(currentLeader.getProposedLeaderID());
        
        Message outgoingMessage = new Message(MessageType.WORK, input.getBytes(),
                from.getHostName(), from.getPort() + 2, to.getHostName(), to.getPort() + 2, workId);

        this.gatewayLogger.fine("Delegating workID " + workId + " to: " + to.getPort());
        try {
            long leaderId = currentLeader.getProposedLeaderID();
            byte[] rawResponse = tcpServer.sendTcpMessage(outgoingMessage, to.getHostName(),
                    to.getPort() + 2, true);
            // If the leader changed/died then this response is invalid so we can throw it out
            if (this.getCurrentLeader() == null || leaderId != this.getCurrentLeader().getProposedLeaderID() || this.isFailed(leaderId)) {
                this.gatewayLogger
                        .info("Leader changed or died while work was being handled. New leader: " + this.getCurrentLeader());
                return null;
            }
            this.gatewayLogger.info("Returning response to workID " + workId);
            try {
                return new Message(rawResponse);
            } catch (Exception e) {
                return null;
            }
        } catch (IOException e) {
            this.gatewayLogger.info(String.format("Failed to delegate workId %d from: %s:%d To: %s:%d error",
                    workId, from.getHostName(), from.getPort(),
                    to.getHostName(), to.getPort()));
            return null;
        }
    }

}
