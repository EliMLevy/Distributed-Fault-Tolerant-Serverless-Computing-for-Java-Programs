package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Message.MessageType;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ZooKeeperPeerServerImpl extends Thread implements ZooKeeperPeerServer {
    private final InetSocketAddress myAddress;
    private final int myPort;
    private final Long id;
    private ServerState state = ServerState.LOOKING;
    private long peerEpoch;

    private volatile Vote currentLeader;
    private volatile boolean shutdown;

    protected LinkedBlockingQueue<Message> outgoingMessages;
    protected LinkedBlockingQueue<Message> incomingMessages;
    private Map<Long, InetSocketAddress> peerIDtoAddress;

    private Lock incomingMessagesLock;

    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;
    private Gossiper gossipWorker;

    private JavaRunnerFollower followerThread;
    private RoundRobinLeader leaderThread;

    public Logger logger;
    public final Long gatewayID;
    private final int numberOfObservers;

    private TCPServer tcpServer;

    private Phaser followerElectionBlocker;

    public ZooKeeperPeerServerImpl(int udpPort, long peerEpoch, Long serverID,
            Map<Long, InetSocketAddress> peerIDtoAddress, Long gatewayID, int numberOfObservers) {
        this.myPort = udpPort;
        this.peerEpoch = peerEpoch;
        this.id = serverID;
        this.peerIDtoAddress = peerIDtoAddress;
        this.gatewayID = gatewayID;
        this.numberOfObservers = numberOfObservers;
        this.myAddress = new InetSocketAddress("localhost", myPort);

        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();

        this.incomingMessagesLock = new ReentrantLock();

        this.followerElectionBlocker = new Phaser();

        this.tcpServer = new TCPServer(this.getTcpPort());

        this.senderWorker = new UDPMessageSender(this.outgoingMessages, this.myPort);
        try {
            this.receiverWorker = new UDPMessageReceiver(this.incomingMessages, this.myAddress, this.myPort, this);
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        this.gossipWorker = new Gossiper(this, peerIDtoAddress, outgoingMessages,
                incomingMessages, incomingMessagesLock, this.myPort);

        try {
            this.logger = initializeLogging(
                    "edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl-id" + this.id + "udpPort-" + udpPort);
            this.logger.setLevel(Level.FINE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void shutdown() {
        this.shutdown = true;
        if (this.senderWorker != null) {
            this.senderWorker.interrupt();
            this.logger.info("Sender interrupt");
        }
        if (this.receiverWorker != null) {
            this.receiverWorker.interrupt();
            this.logger.info("receiver interrupt");

        }
        if (this.followerThread != null) {
            this.followerThread.interrupt();
            this.logger.info("follower thread interrupt");
        }
        if (this.leaderThread != null) {
            this.leaderThread.interrupt();
            this.logger.info("leader thread interrupt");
        }

        if (this.gossipWorker != null) {
            this.gossipWorker.interrupt();
            this.gossipWorker.shutdown();
            this.logger.info("Gossip thread interrupt");
        }

        if (this.tcpServer != null) {
            this.tcpServer.interrupt();
            try {
                this.tcpServer.sendTcpMessage(null, "localhost", this.getTcpPort(), false);
            } catch (IOException e) {
            }
        }
        this.logger.info("Shut down...");
        this.interrupt();
    }

    @Override
    public void run() {
        this.logger.info("Entering run...");
        // step 1: create and run thread that sends broadcast messages
        senderWorker.start();
        // step 2: create and run thread that listens for messages sent to this server
        receiverWorker.start();
        
        // These two threads dont stop until this node is shut down. Even during
        // elections or state transitions.
        Util.startAsDaemon(this.gossipWorker, "PeerServer-" + this.id + "-Gossiper");
        Util.startAsDaemon(this.tcpServer, this.id + "-TCPServer-Port" + this.getTcpPort());
        
        // step 3: main server loop
        enterMainServerLoop();

    }

    public void enterMainServerLoop() {
        this.logger.info("Entering main servrer loop from serverimple. State: " + this.getPeerState().toString());
        while (!this.shutdown && !this.isInterrupted()) {

            if (this.getPeerState() == ServerState.LOOKING) {
                // If we are in the looking state. Make the follower stop tending to requests.
                this.followerElectionBlocker.register();

                // run the elction algorothm (Just call look for leader)
                this.logger.info("STARTING ELECTIONS. QUORUM SIZE IS: " + this.getQuorumSize());
                ZooKeeperLeaderElection zeelection = new ZooKeeperLeaderElection(this, this.incomingMessages);
                zeelection.incomingMessagesLock = this.incomingMessagesLock;
                Vote v = zeelection.lookForLeader();
                this.logger.info("FINISHED! " + v.getProposedLeaderID() + ". Starting gossiper");

                // Increment epoch and remove election blocker
                this.peerEpoch++;
                this.followerElectionBlocker.arriveAndDeregister();
            } else if (this.getPeerState() == ServerState.LEADING) {
                // If we are in the leading state: (we can be sure this is the first time are
                // entering leading)
                // start the leader thread and stop the follower thread if we havent already
                if (this.leaderThread == null) {
                    this.logger.info("STARTING LEADING");
                    this.leaderThread = new RoundRobinLeader(this, this.peerIDtoAddress, this.gatewayID,
                            this.gossipWorker.failedServers, this.tcpServer);
                    Util.startAsDaemon(leaderThread, "Leader-" + this.id);
                }
            } else if (this.getPeerState() == ServerState.FOLLOWING) {
                // If we are in the following state:
                // if our leader is dead:
                if (this.leaderIsDead()) {
                    // go into the looking state
                    this.currentLeader = null;
                    this.peerEpoch++;
                    this.setPeerState(ServerState.LOOKING);
                } else {
                    // if we dont have a running follower: (follower is null or follower.stopped)
                    if (this.followerThread == null) {
                        // start a follower thread
                        this.followerThread = new JavaRunnerFollower(this, this.followerElectionBlocker,
                                this.tcpServer);
                        Util.startAsDaemon(followerThread, "Follower-" + this.id);
                    }
                }
                // If we are in the observer state:
            } else if (this.getPeerState() == ServerState.OBSERVER) {
                // If our leader is dead:
                if (this.leaderIsDead()) {
                    // run the election algorithm
                    this.logger.info("Observer entering election. Old leader: " + this.getCurrentLeader());
                    this.currentLeader = null;
                    this.peerEpoch++;
                    this.followerElectionBlocker.register();
                    ZooKeeperLeaderElection observerElection = new ZooKeeperLeaderElection(this, this.incomingMessages);
                    observerElection.incomingMessagesLock = this.incomingMessagesLock;
                    observerElection.lookForLeader();
                    this.followerElectionBlocker.arriveAndDeregister();
                }

            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
        this.logger.info("Exiting main server loop. Goodbye!");
    }

    private boolean leaderIsDead() {
        return this.currentLeader == null
                || this.gossipWorker.failedServers.contains(this.getCurrentLeader().getProposedLeaderID());
    }

    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        this.logger.info("Setting leader to " + v.getProposedLeaderID());
        this.currentLeader = v;

    }

    @Override
    public Vote getCurrentLeader() {
        return this.currentLeader;
    }

    @Override
    public void sendMessage(MessageType type, byte[] messageContents, InetSocketAddress target)
            throws IllegalArgumentException {
        this.outgoingMessages.add(new Message(type, messageContents, this.myAddress.getHostName(), this.myPort,
                target.getAddress().getHostName(), target.getPort()));
    }

    @Override
    public void sendBroadcast(MessageType type, byte[] messageContents) {
        for (Map.Entry<Long, InetSocketAddress> entry : this.peerIDtoAddress.entrySet()) {
            if (this.gossipWorker == null || !this.gossipWorker.failedServers.contains(entry.getKey())) {
                this.sendMessage(type, messageContents, entry.getValue());
            }
        }
    }

    @Override
    public synchronized ServerState getPeerState() {
        return this.state;
    }

    @Override
    public synchronized void setPeerState(ServerState newState) {
        if (newState != ServerState.OBSERVER) {
            String logMsg = String.format("[%d]: switching from [%s] to [%s]", this.id, this.state, newState);
            this.logger.info(logMsg);
            this.gossipWorker.stateChange(this.state, newState);
            System.out.println(logMsg);
        }
        if (this.state == ServerState.OBSERVER)
            return;
        this.state = newState;
    }

    @Override
    public Long getServerId() {
        return this.id;
    }

    @Override
    public long getPeerEpoch() {
        return this.peerEpoch;
    }

    @Override
    public InetSocketAddress getAddress() {
        return this.myAddress;
    }

    @Override
    public int getUdpPort() {
        return this.myPort;
    }

    public int getTcpPort() {
        return this.myPort + 2;
    }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        if (this.id == peerId)
            return this.myAddress;
        else
            return this.peerIDtoAddress.get(peerId);
    }

    @Override
    public int getQuorumSize() {
        int numServers = this.peerIDtoAddress.keySet().size() + 1;
        numServers -= this.numberOfObservers;
        if (this.gossipWorker != null) {
            numServers -= this.gossipWorker.failedServers.size();
        }
        int half = numServers / 2;
        return half + 1;
    }

    public void newFailure(Long id) {
        // If we are leading:
        // reassign any client request work it had given the dead node to a different
        // node
        if (this.state == ServerState.LEADING) {
            this.leaderThread.newFailure(id);
        }
    }

    public boolean isFailed(long id) {
        return this.gossipWorker.failedServers.contains(id);
    }

    public boolean isFailed(InetSocketAddress address) {
        if (!this.gossipWorker.addressToPeerID.containsKey(address)) {
            return true;
        }
        return this.gossipWorker.failedServers.contains(this.gossipWorker.addressToPeerID.get(address));
    }

    public boolean isInElection() {
        return this.currentLeader == null || this.getPeerState() == ServerState.LOOKING;
    }

    public static void main(String[] args) {

        /*
         * The user will specify the id -> address mapping as follows:
         * java ZookeeperPeerServerImpl myId myPort gatewayId NUM_OBSERVERS NUM_SERVERS
         * id1 host1 port1 id2 host2 port2 ...
         */

        final int NUM_SERVERS = Integer.parseInt(args[4]);
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>();
        for (int i = 0; i < NUM_SERVERS; i++) {
            long id = Long.parseLong(args[i * 3 + 5]);
            String hostname = args[i * 3 + 6];
            int port = Integer.parseInt(args[i * 3 + 7]);
            peerIDtoAddress.put(id, new InetSocketAddress(hostname, port));
        }

        long myId = Long.parseLong(args[0]);
        int myPort = Integer.parseInt(args[1]);
        long gatewayId = Long.parseLong(args[2]);
        int NUM_OBSERVERS = Integer.parseInt(args[3]);

        ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(myPort, 0, myId, peerIDtoAddress, gatewayId,
                NUM_OBSERVERS);
        server.start();

    }
}
