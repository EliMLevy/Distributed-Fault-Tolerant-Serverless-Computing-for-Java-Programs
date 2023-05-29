package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.LinkedBlockingQueue;

import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Vote;
import edu.yu.cs.com3800.ZooKeeperPeerServer;

public class MockZooKeeperPeerServerImpl extends Thread implements ZooKeeperPeerServer {


    public LinkedBlockingQueue<Message> outgoingMessages;
    private String hostname;
    private int port;
    private InetSocketAddress address;

    public MockZooKeeperPeerServerImpl(LinkedBlockingQueue<Message> outgoingMessages, String hostname, int port) {
        this.outgoingMessages = outgoingMessages;
        this.hostname = hostname;
        this.port = port;


        this.address = new InetSocketAddress(this.hostname, this.port);

    }

    @Override
    public void shutdown() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public Vote getCurrentLeader() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void sendMessage(MessageType type, byte[] messageContents, InetSocketAddress target)
            throws IllegalArgumentException {
        outgoingMessages.add(new Message(type, messageContents, this.hostname, this.port, target.getHostName(), target.getPort()));
        
    }

    @Override
    public void sendBroadcast(MessageType type, byte[] messageContents) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public ServerState getPeerState() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setPeerState(ServerState newState) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public Long getServerId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long getPeerEpoch() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public InetSocketAddress getAddress() {
        return this.address;
    }

    @Override
    public int getUdpPort() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getQuorumSize() {
        // TODO Auto-generated method stub
        return 0;
    }
    
}
