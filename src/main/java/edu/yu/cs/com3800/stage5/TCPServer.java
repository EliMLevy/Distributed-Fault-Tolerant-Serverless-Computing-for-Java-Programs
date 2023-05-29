package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.Message.MessageType;

public class TCPServer extends Thread implements LoggingServer {

    private final int port;

    public final LinkedBlockingQueue<TCPMessage> msgsFromClient;
    private long dialogueId = 0;

    // Map request ids to the dialogueIds
    private final Map<Long, Map<MessageType, Long>> workIdsBeingServiced;
    // Map dialogueIds to sockets
    private final Map<Long, Socket> openDialogues;

    public final Logger logger;

    public TCPServer(int port) {
        this.port = port;
        this.openDialogues = new ConcurrentHashMap<>();
        this.msgsFromClient = new LinkedBlockingQueue<>();
        this.workIdsBeingServiced = new ConcurrentHashMap<>();

        try {
            this.logger = initializeLogging("edu.yu.cs.com3800.stage5.TCPServer-port" + this.port);
        } catch (IOException e) {
            throw new RuntimeException("Unhandled");
        }
    }

    public class TCPMessage {
        public long id;
        public Message msg;

        public TCPMessage(long id, Message msg) {
            this.id = id;
            this.msg = msg;
        }

        @Override
        public int hashCode() {
            return (int) this.id;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof TCPMessage))
                return false;

            TCPMessage other = (TCPMessage) o;
            return this.id == other.id;
        }
    }

    @Override
    public void run() {
        try (ServerSocket serverSocket = new ServerSocket(this.port);) {
            while (!this.isInterrupted()) {
                this.logger.info("TCPServer: waiting to receive connection");
                Socket socket = serverSocket.accept();
                if (this.isInterrupted())
                    break;
                Message msg = new Message(Util.readAllBytesFromNetwork(socket.getInputStream()));
                this.logger.info("TCPServer: received connection. id " + msg.getRequestID());
                this.workIdsBeingServiced.putIfAbsent(msg.getRequestID(), new ConcurrentHashMap<>());
                this.workIdsBeingServiced.get(msg.getRequestID()).put(msg.getMessageType(), dialogueId);
                this.openDialogues.put(dialogueId, socket);
                this.msgsFromClient.add(new TCPMessage(dialogueId, msg));
                this.dialogueId++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (Socket s : this.openDialogues.values()) {
            if (!s.isClosed()) {
                try (s) {
                    s.close();
                } catch (IOException e) {
                }
            }
        }
    }

    public Long servicingWorkId(long id) {
        if (this.workIdsBeingServiced.containsKey(id)
                && this.workIdsBeingServiced.get(id).containsKey(MessageType.WORK)) {
            return this.workIdsBeingServiced.get(id).get(MessageType.WORK);
        }
        return null;
    }

    public TCPMessage poll(long msTimeout) throws InterruptedException {
        TCPMessage msg = this.msgsFromClient.poll(msTimeout, TimeUnit.MILLISECONDS);
        return msg;
    }

    public void putBackMessage(TCPMessage msg) {
        this.msgsFromClient.offer(msg);
    }

    public void respondToClient(long id, Message msg) {
        this.logger.info("Fetching dialogueId: " + id + ".\n" + this.openDialogues.toString() + "\n"
                + this.workIdsBeingServiced.toString());

        try (Socket socket = this.openDialogues.get(id);) {
            if (socket == null) {
                this.logger.info("Socket with dialogue ID " + id + " was already responded to");
                return;
            }
            if (!socket.isClosed()) {
                if (msg != null) {
                    socket.getOutputStream().write(msg.getNetworkPayload());
                } else {
                    socket.getOutputStream().write("No data".getBytes());
                }
            } else {
                this.logger.warning("Trying to respond to socket that is closed. Message: " + msg.toString());
            }
        } catch (IOException e) {
            this.logger.log(Level.WARNING, "Failed to respond to client", e);
        }
        this.logger.info("Disposing of dialogueId: " + id);
        if (msg != null) {
            this.workIdsBeingServiced.remove(msg.getRequestID());
        }
        this.disposeOfDialogue(id);
    }

    /**
     * Sends a message and synchronously returns the response
     * 
     * @param msg          The message object to be sent
     * @param receiverHost The hostname of the receipient
     * @param receiverPort The port of the recipient
     * @return The response of the recipient
     * @throws IOException
     */
    public byte[] sendTcpMessage(Message msg, String receiverHost, int receiverPort, boolean waitForReply)
            throws IOException {
        try (Socket socket = new Socket(receiverHost, receiverPort);) {
            if (msg != null) {
                socket.getOutputStream().write(msg.getNetworkPayload());
            } else {
                return new byte[0];
            }
            if (waitForReply) {
                return Util.readAllBytesFromNetwork(socket.getInputStream());
            } else {
                return null;
            }
        } catch (IOException e) {
            throw new IOException(e);
        }
    }

    private void disposeOfDialogue(long id) {
        Socket socket = this.openDialogues.get(id);
        if (socket == null)
            return;
        if (!socket.isClosed()) {
            try (socket) {
                socket.close();
            } catch (IOException e) {
            }
        }
        this.openDialogues.remove(id);
    }

}
