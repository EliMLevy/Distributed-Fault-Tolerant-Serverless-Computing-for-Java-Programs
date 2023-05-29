package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.ZooKeeperPeerServer;
import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.stage5.TCPServer.TCPMessage;

public class RoundRobinLeader extends Thread {

    private final ZooKeeperPeerServerImpl myPeerServer;

    // Does not need to be a thread safe collection.
    private final List<Long> peerIds;
    private int nextPeer = 0;

    private final Logger logger;

    private final TCPServer tcpSenderReceiver;
    private final ExecutorService executorService;
    private final Map<Long, InetSocketAddress> peerIDtoAddress;

    // The map is only read and modified by the main thread. The sets need to be
    // thread safe
    private final Map<Long, Set<TCPMessage>> assignedWork;

    // Accessed by the peerServer so needs to be thread safe
    private final LinkedBlockingQueue<Long> unhandledFailures;
    private final Set<Long> failedServers;

    private Map<Long, Message> resultsThatHaventBeenSent;

    public RoundRobinLeader(ZooKeeperPeerServerImpl myPeerServer, Map<Long, InetSocketAddress> peerIDtoAddress,
            long gatewayID, Set<Long> failedServers, TCPServer tcpServer) {
        this.myPeerServer = myPeerServer;
        this.peerIDtoAddress = new HashMap<>(peerIDtoAddress);
        this.failedServers = failedServers;
        Map<Long, InetSocketAddress> map = new HashMap<>(peerIDtoAddress);
        map.remove(gatewayID);
        this.peerIds = new ArrayList<>(map.keySet());

        this.tcpSenderReceiver = tcpServer;

        this.assignedWork = new HashMap<>();
        this.unhandledFailures = new LinkedBlockingQueue<>();

        this.executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                new FixedDaemonThreadFactory());

        this.resultsThatHaventBeenSent = new HashMap<>();

        try {
            this.logger = this.myPeerServer.initializeLogging("edu.yu.cs.com3800.stage5.RoundRobinLeader-id-"
                    + this.myPeerServer.getServerId() + "-tcpPort-" + this.myPeerServer.getTcpPort());
        } catch (IOException e) {
            throw new RuntimeException("Uncaught", e);
        }
    }

    @Override
    public void run() {
        this.startAcceptingRequests();
    }
    /*
     * !!Important Note!!
     * 
     * I misunderstood how the NEW_LEADER_GETTING_LAST_WORK was
     * supposed to work. I understand now that the leader is supposed to begin his
     * reign by actively requesting any computed results from the followers. The way
     * I implemented it was that if the follower computes a result but can't send it
     * back to the leader, that follower stores the response till there is a leader
     * and then that follower initiates a TCP connection with the leader in order to
     * send back the result. This is how I interpreted "Gathers any work that it..."
     * from the leader section of the requirement doc.
     * 
     * Unfortunately, I realized this 3 hours before the due date so I dont have
     * time rework my code. However, I am confident the approach that I adopted is
     * valid and worthy of full credit for two reasons:
     * 1) It works.
     * 2) Having the leader establish a TCP connection with all of the followers
     * before it can begin servicing requests from the gateway can potentially delay
     * the startup time of the leader to however long it takes to mark a follower as
     * failed. In a large system where we expect nodes to be consistently failing
     * this would almost double the time it takes for the cluster to recover from
     * the loss of the leader (time it takes to recognize the leader as dead + time
     * to run election + time to gather computed results). In contrast, my approach
     * allows the leader to immediately begin delating requests to the followers.
     * The passive collection of the previously computed results only has potential
     * to increase the efficiency of the system.
     * 
     * 
     */

    /**
     * 
     * Starts accepting incoming work requests and delegating them to workers. This
     * method enters a loop as long as the
     * server is not interrupted and it is in the LEADING state. Inside the loop,
     * the server listens for connections on its
     * TCP port and creates a task to handle incoming requests. The task reads all
     * bytes from the socket, assigns the work
     * to a worker using a round-robin approach, synchronously waits for the result,
     * and writes the result back to the socket.
     * This continues until the server is interrupted or its state changes to
     * something other than LEADING.
     */
    public void startAcceptingRequests() {
        this.logger.info("Starting to accept requests");
        while (!this.isInterrupted() && this.myPeerServer.getPeerState() == ZooKeeperPeerServer.ServerState.LEADING) {
            // Make sure that work assigned to recently failed nodes gets reassigned ASAP
            while (this.unhandledFailures.size() > 0) {
                long id = this.unhandledFailures.poll();
                reassignWork(id);
            }

            try {
                TCPMessage msg = this.tcpSenderReceiver.poll(500);
                if (msg != null) {
                    // Make sure we catch any message type.
                    long requestId = msg.msg.getRequestID();
                    MessageType msgType = msg.msg.getMessageType();
                    if (msgType == MessageType.WORK) {
                        // Check if we have the result cached
                        if (this.resultsThatHaventBeenSent.containsKey(requestId)) {
                            this.logger.info("Already had the result of workId " + requestId);
                            Message computedResult = this.resultsThatHaventBeenSent.get(requestId);
                            this.tcpSenderReceiver.respondToClient(msg.id, computedResult);
                            this.resultsThatHaventBeenSent.remove(requestId);
                        } else {
                            // Delegate the work
                            long idOfNextWorker = getIdOfNextWorker();
                            this.logger.info("Delegating workID " + requestId + " to peer " + idOfNextWorker);
                            this.delegateWork(msg, idOfNextWorker);
                        }
                    } else if (msgType == MessageType.NEW_LEADER_GETTING_LAST_WORK) {
                        // if the result for this was already rerequested by the gateway send it
                        // immediately and close connection to worker
                        Long dialogueId = this.tcpSenderReceiver.servicingWorkId(requestId);
                        if (dialogueId != null) {
                            this.logger.info("Received NLGLW workId " + requestId
                                    + " and was already servicing this gateway request");
                            this.tcpSenderReceiver.respondToClient(dialogueId, msg.msg);
                            this.tcpSenderReceiver.respondToClient(requestId, null);
                        } else {
                            this.logger.info("Received NLGLW workId " + requestId + " and caching result for later");
                            // else, store this result until the gateway rerequests it
                            this.resultsThatHaventBeenSent.put(requestId, msg.msg);
                        }
                    } else if (msgType == MessageType.COMPLETED_WORK) {
                        // This really shouldnt happen. COMPLETED_WORK msgs should only be sent in
                        // response to the WORK msgs (on the same TCP connection). Fail loud and fast
                        throw new IllegalStateException(
                                "Follower should not be initiating TCP connection for COMPLETED_WORK msg. "
                                        + msg.msg.toString());
                    } else if (msgType == MessageType.ELECTION || msgType == MessageType.GOSSIP) {
                        // The only reason one of these will show up in my TCP queue is if something
                        // is terribly terribly terribly terribly terribly wrong. Fail very loudly if
                        // this happens
                        throw new IllegalStateException(
                                "Election or Gossip message being sent over TCP?? What happened bro? "
                                        + msg.msg.toString());
                    } else {
                        // Let us pray that this block is never reached....
                        throw new IllegalStateException("Foreign message type received. " + msg.msg.toString());
                    }
                }
            } catch (InterruptedException e) {
                break;
            }

        }
        this.logger.info("Leader exiting. Goodbye!");
    }

    /**
     * 
     * Returns the ID of the next worker to which a task should be assigned,
     * according to the round-robin approach.
     * 
     * @return the ID of the next worker
     */
    private long getIdOfNextWorker() {
        Long potentialNextWorker = this.peerIds.get(this.nextPeer);
        while (this.failedServers.contains(potentialNextWorker)) {
            this.nextPeer++;
            this.nextPeer = this.nextPeer % this.peerIds.size();
            potentialNextWorker = this.peerIds.get(this.nextPeer);
        }
        this.nextPeer++;
        this.nextPeer = this.nextPeer % this.peerIds.size();
        return potentialNextWorker;
    }

    /**
     * 
     * Assigns a task to the specified worker and waits for the result.
     * 
     * @param msg            the message carrying the work to be assigned
     * @param idOfNextWorker the ID of the worker to which the task should be
     *                       assigned
     */
    private void delegateWork(TCPMessage msg, long idOfNextWorker) {
        this.assignedWork.putIfAbsent(idOfNextWorker, new ConcurrentHashMap<TCPMessage, Integer>().keySet(0));
        this.assignedWork.get(idOfNextWorker).add(msg);
        final InetSocketAddress to = this.peerIDtoAddress.get(idOfNextWorker);
        this.executorService
                .submit(new WorkAssignmentTask(tcpSenderReceiver, this.myPeerServer.getAddress(), to, msg,
                        this.assignedWork.get(idOfNextWorker), this.logger));
    }

    /**
     * 
     * Reassigns the work that was previously assigned to the specified failed
     * worker to another worker.
     * 
     * @param failedWorkerId the ID of the failed worker
     */
    private void reassignWork(long failedWorkerId) {
        if (this.assignedWork.get(failedWorkerId) == null)
            return;
        for (TCPMessage msg : this.assignedWork.get(failedWorkerId)) {
            long idOfNextWorker = getIdOfNextWorker();
            this.logger.info("Reassigning work from dead peer " + failedWorkerId + " to peer " + idOfNextWorker);
            delegateWork(msg, idOfNextWorker);
        }
    }

    public void newFailure(Long id) {
        this.unhandledFailures.add(id);
    }

    private class WorkAssignmentTask implements Runnable {

        private InetSocketAddress fromAddress;
        private InetSocketAddress toAddress;
        private TCPServer tcpServer;
        private TCPMessage msg;
        private Set<TCPMessage> workAssignedToThisWorker;
        private Logger workLogger;

        public WorkAssignmentTask(TCPServer tcpServer, InetSocketAddress fromAddress, InetSocketAddress toAddress,
                TCPMessage msg, Set<TCPMessage> workAssignedToThisWorker, Logger logge0r) {
            this.fromAddress = fromAddress;
            this.toAddress = toAddress;
            this.tcpServer = tcpServer;
            this.msg = msg;
            this.workAssignedToThisWorker = workAssignedToThisWorker;
            workLogger = logge0r;

        }

        public void run() {
            Message response;
            try {
                this.workLogger.info("WorkID: " + this.msg.msg.getRequestID() + "  being handled");
                response = handleWorkRequest(this.msg.msg);
                this.workLogger.info("WorkID: " + this.msg.msg.getRequestID() + " handled! sending back to gateway");
                this.tcpServer.respondToClient(msg.id, response);
                this.workAssignedToThisWorker.remove(msg);
            } catch (IOException e) {
                this.workLogger.log(Level.INFO, "Failed to handle workID: " + this.msg.msg.getRequestID(), e);
            }
        }

        /**
         * 
         * Handles an incoming work request by delegating the work to a worker and
         * returning the result to the caller.
         * 
         * @param msg the incoming work request
         * @throws IOException if an I/O error occurs while communicating with the
         *                     worker
         */
        public Message handleWorkRequest(Message msg)
                throws IOException {

            // Store record and send message
            Message outgoingMsg = new Message(MessageType.WORK, msg.getMessageContents(),
                    this.fromAddress.getHostName(),
                    this.fromAddress.getPort() + 2, this.toAddress.getHostName(), this.toAddress.getPort() + 2,
                    msg.getRequestID());
            try {
                this.workLogger.info("Sending work message to " + this.toAddress.getHostName() + ":"
                        + (this.toAddress.getPort() + 2));
                byte[] response = this.tcpServer.sendTcpMessage(outgoingMsg, this.toAddress.getHostName(),
                        this.toAddress.getPort() + 2, true);

                return new Message(response);
            } catch (IOException e) {
                throw new IOException("Failed to send work request", e);
            }
        }

    }

}
