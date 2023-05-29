package edu.yu.cs.com3800.stage5;

import java.util.concurrent.LinkedBlockingQueue;

import edu.yu.cs.com3800.Message;

public class MockTCPServer extends TCPServer {

    private LinkedBlockingQueue<Message> sentMessages;

    public MockTCPServer(int port, LinkedBlockingQueue<Message> sentMessages) {
        super(port);
        this.sentMessages = sentMessages;
    }

    @Override
    public byte[] sendTcpMessage(Message msg, String hostName, int port, boolean waitForReply) {
        this.sentMessages.add(msg);
        String res = "Response: " + new String(msg.getMessageContents());
        
        return new Message(msg.getMessageType(), res.getBytes(), hostName, port, hostName, port).getNetworkPayload();
    }
    
}
