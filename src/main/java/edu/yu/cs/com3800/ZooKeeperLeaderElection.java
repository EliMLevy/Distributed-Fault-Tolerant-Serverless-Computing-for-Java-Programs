package edu.yu.cs.com3800;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState;
import edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl;

public class ZooKeeperLeaderElection {
    /**
     * time to wait once we believe we've reached the end of leader election.
     */
    private final static int finalizeWait = 4000;

    /**
     * Upper bound on the amount of time between two consecutive notification
     * checks.
     * This impacts the amount of time to get the system up again after long
     * partitions. Currently 60 seconds.
     */
    private final static int maxNotificationInterval = 60000;
    private final int QUORUM_SIZE;

    private LinkedBlockingQueue<Message> incomingMessages;
    private ZooKeeperPeerServerImpl myPeerServer;

    private long proposedLeader;
    private long proposedEpoch;

    private Map<Long, ElectionNotification> voteRecords;

    public Lock incomingMessagesLock;

    public ZooKeeperLeaderElection(ZooKeeperPeerServer server, LinkedBlockingQueue<Message> incomingMessages) {
        this.incomingMessages = incomingMessages;
        this.incomingMessagesLock = new ReentrantLock();
        this.myPeerServer = (ZooKeeperPeerServerImpl) server;
        if (server.getPeerState() != ServerState.OBSERVER) {
            this.proposedLeader = this.myPeerServer.getServerId();
            this.proposedEpoch = this.myPeerServer.getPeerEpoch();
        } else {
            this.proposedLeader = Long.MIN_VALUE;
            this.proposedEpoch = 0;
        }

        this.QUORUM_SIZE = this.myPeerServer.getQuorumSize();

        this.voteRecords = new HashMap<>();

    }

    public Vote lookForLeader() {
        // send initial notifications to other peers to get things started
        this.myPeerServer.logger.info("Entering look for leader! State: " + this.myPeerServer.getPeerState().toString());
        this.voteRecords.put(this.myPeerServer.getServerId(),
                new ElectionNotification(this.proposedLeader, this.myPeerServer.getPeerState(),
                        this.myPeerServer.getServerId(), this.proposedEpoch));
        sendNotifications();
        // Loop, exchanging notifications with other servers until we find a leader
        while (this.myPeerServer.getPeerState() == ZooKeeperPeerServer.ServerState.LOOKING
                || this.myPeerServer.getCurrentLeader() == null) {
            Message nextMsg = retrieveNextMessage();
            if (nextMsg == null) {
                this.myPeerServer.logger.info("Didnt find another message... Exiting election");
                return getCurrentVote();
            }
            
            ElectionNotification nextNotif = getNotificationFromMessage(nextMsg);
            this.myPeerServer.logger.fine(String.format("Received election notification from:%d, with state:%s",
                    nextNotif.getSenderID(), nextNotif.getState().toString()));
            if(cameFromInValidPeer(nextNotif)) {
                this.myPeerServer.logger.info("Invalid peer: " + nextNotif.getSenderID());
                continue;
            }
            if(nextNotif.getPeerEpoch() < this.proposedEpoch) continue;
            if (nextNotif.getState() == ServerState.LOOKING) {
                if (supersedesCurrentVote(nextNotif.getProposedLeaderID(), nextNotif.getPeerEpoch())) {
                    adoptNewProposal(nextNotif);
                }
                this.voteRecords.put(nextNotif.getSenderID(), nextNotif);
                if (haveEnoughVotes(this.voteRecords, getCurrentVote())) {
                    this.myPeerServer.logger.info("Starting eletion last call");
                    if (electionLastCall()) {
                        this.myPeerServer.logger.info("Exiting eletion last call");
                        acceptElectionWinner(nextNotif);
                    }
                }
            } else if (nextNotif.getState() == ServerState.LEADING
                    || nextNotif.getState() == ServerState.FOLLOWING) {
                this.voteRecords.put(nextNotif.getSenderID(), nextNotif);
                if (haveEnoughVotes(this.voteRecords, nextNotif)) {
                    acceptElectionWinner(nextNotif);
                } else {
                }
            }
        }
        this.myPeerServer.logger.info("Done with election loop. Current leeder: "
                + this.myPeerServer.getCurrentLeader().getProposedLeaderID());
        return this.getCurrentVote();

    }

    private boolean cameFromInValidPeer(ElectionNotification nextNotif) {
        return this.myPeerServer.isFailed(nextNotif.getSenderID());
    }
    
    private synchronized Vote getCurrentVote() {
        return new Vote(this.proposedLeader, this.proposedEpoch);
    }

    /*
     * Attempts to retrieve the next message of the incoming message queue.
     * The initital timeout used is 200ms. If no message is found,
     * sendNotifications()
     * is re-invoked, the timeout length is doubled and we attempt to read from the
     * queue again.
     * If the timeout length exceeds the maxNotificationInterval then the method
     * returns null.
     */
    private Message retrieveNextMessage() {
        int currentBackOff = 100;
        Queue<Message> nonElectionMsgs = new LinkedList<>();
        this.incomingMessagesLock.lock();
        try {
            while (currentBackOff <= ZooKeeperLeaderElection.maxNotificationInterval) {
                Message result = this.incomingMessages.poll(currentBackOff, TimeUnit.MILLISECONDS);
                if (result != null) {
                    if (result.getMessageType() != MessageType.ELECTION) {
                        nonElectionMsgs.add(result);
                    } else {
                        // If this message does not provide new information then diregard it
                        ElectionNotification notif = getNotificationFromMessage(result);
                        ElectionNotification storedNotif = this.voteRecords.get(notif.getSenderID());
                        if(storedNotif == null || storedNotif.getProposedLeaderID() != notif.getProposedLeaderID() || storedNotif.getPeerEpoch() != notif.getPeerEpoch()) {
                            return result;
                        }
                    }
                } else {
                    sendNotifications();
                    currentBackOff *= 2;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            for(Message m : nonElectionMsgs) {
                this.incomingMessages.add(m);
            }
            this.incomingMessagesLock.unlock();
        }
        return null;
    }

    /*
     * A precondition to calling acceptElectionWinner. We must go through any
     * remaining messages on the queue
     * to ensure that there are no superior votes. The timeout for each poll is
     * finalizeWait.
     * If there is a superior vote on the queue then the vote is adopted and the
     * method returns false.
     * If there is no superior vote then the method returns true.
     * 
     * @TODO the state of the senders is ignored. Even messages from LEADING or
     * FOLLOWING servers are recorded
     * in voteRecrods. This is inconsistent with how it is implemented in the main
     * loop.
     */
    private boolean electionLastCall() {
        // TODO this might be more involved than how i implemented
        Queue<Message> nonElectionMsgs = new LinkedList<>();
        this.incomingMessagesLock.lock();
        try {
            Thread.sleep(finalizeWait);
            Message newMsg = this.incomingMessages.poll();
            while (newMsg != null) {
                if(newMsg.getMessageType() != MessageType.ELECTION) {
                    nonElectionMsgs.add(newMsg);
                } else {
                    ElectionNotification newNotif = getNotificationFromMessage(newMsg);
                    this.voteRecords.put(newNotif.getSenderID(), newNotif);
                    if (supersedesCurrentVote(newNotif.getProposedLeaderID(), newNotif.getPeerEpoch())) {
                        adoptNewProposal(newNotif);
                        return false;
                    }
                }
                newMsg = this.incomingMessages.poll();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            Message nonElectionMsg = nonElectionMsgs.poll();
            while(nonElectionMsg != null) {
                this.incomingMessages.add(nonElectionMsg);
                nonElectionMsg = nonElectionMsgs.poll();
            }
            this.incomingMessagesLock.unlock();
        }
        return true;

    }

    /*
     * Invokes the sendBroadcast method of myPeerServer, notifying other servers of
     * the current vote.
     */
    private void sendNotifications() {

        ElectionNotification msg = new ElectionNotification(this.proposedLeader, this.myPeerServer.getPeerState(),
                this.myPeerServer.getServerId(), this.proposedEpoch);
        this.myPeerServer.logger.fine("Broadcasting: " + msg.toString() + " with state: " + msg.getState().toString());
        this.myPeerServer.sendBroadcast(MessageType.ELECTION, buildMsgContent(msg));

    }

    /*
     * Sets the state and leader of myPeerServer. Also clears the incoming messages
     * queue.
     */
    private Vote acceptElectionWinner(ElectionNotification n) {
        this.myPeerServer.logger.fine("Accepting winner: " + n.toString());
        try {
            this.myPeerServer.setCurrentLeader(n);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (n.getProposedLeaderID() == this.myPeerServer.getServerId()) {
            this.myPeerServer.setPeerState(ServerState.LEADING);
        } else {
            this.myPeerServer.setPeerState(ServerState.FOLLOWING);
        }
        
        return n;
    }

    /*
     * We return true if one of the following three cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     */
    protected boolean supersedesCurrentVote(long newId, long newEpoch) {
        return (newEpoch > this.proposedEpoch) || ((newEpoch == this.proposedEpoch) && (newId > this.proposedLeader));
    }

    /**
     * Termination predicate. Given a set of votes, determines if have sufficient
     * support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one
     * current vote
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification> votes, Vote proposal) {
        // TODO check if every server has a vote
        // is the number of votes for the proposal > the size of my peer server’s
        // quorum?
        int count = 0;
        for (ElectionNotification vote : votes.values()) {
            if (vote.getProposedLeaderID() == proposal.getProposedLeaderID()) {
                count++;
            }
        }
        this.myPeerServer.logger.fine(proposal.toString() + " has " + count + " votes");
        return count >= this.QUORUM_SIZE;
    }

    public static byte[] buildMsgContent(ElectionNotification notification) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 3 + 2);
        buffer.putLong(notification.getProposedLeaderID());
        buffer.putChar(notification.getState().getChar());
        buffer.putLong(notification.getSenderID());
        buffer.putLong(notification.getPeerEpoch());
        return buffer.array();
    }

    public static ElectionNotification getNotificationFromMessage(Message received) {
        final ByteBuffer msgBytes = ByteBuffer.wrap(received.getMessageContents());
        final long leader = msgBytes.getLong();
        final char stateChar = msgBytes.getChar();
        final long senderID = msgBytes.getLong();
        final long peerEpoch = msgBytes.getLong();
        return new ElectionNotification(leader, ZooKeeperPeerServer.ServerState.getServerState(stateChar), senderID,
                peerEpoch);
    }

    /*
     * Changes the proposed leader of this server, stores that in the voteRecords,
     * and invokes sendNotifications()
     */
    private void adoptNewProposal(ElectionNotification vote) {
        this.myPeerServer.logger.fine("Adopting new proposal: " + vote.toString());
        this.proposedEpoch = vote.getPeerEpoch();
        this.proposedLeader = vote.getProposedLeaderID();
        this.voteRecords.put(this.myPeerServer.getServerId(), vote);
        sendNotifications();
    }
}
