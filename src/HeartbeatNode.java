import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class HeartbeatNode {
    private static final int HEARTBEAT_PORT = 8888;
    private static final int MESSAGE_PORT = 8889;
    private static final int MULTICAST_PORT = 8890;
    private static final String MULTICAST_GROUP = "230.0.0.1";
    private static final long HEARTBEAT_INTERVAL = 3000;
    private static final long TIMEOUT_THRESHOLD = 10000;

    private String nodeId;
    private Map<String, NodeInfo> nodes;
    private AtomicBoolean running;
    private MulticastSocket multicastSocket;
    private InetAddress multicastGroup;
    private ServerSocket messageServerSocket;
    private int actualMessagePort;

    private class NodeInfo {
        String nodeId;
        InetAddress address;
        int messagePort;
        long lastSeen;
        String uniqueId;

        NodeInfo(String nodeId, InetAddress address, int messagePort) {
            this.nodeId = nodeId;
            this.address = address;
            this.messagePort = messagePort;
            this.lastSeen = System.currentTimeMillis();
            this.uniqueId = nodeId + "@" + address.getHostAddress() + ":" + messagePort;
        }
    }

    public HeartbeatNode(String nodeId) {
        this.nodeId = nodeId;
        this.nodes = new ConcurrentHashMap<>();
        this.running = new AtomicBoolean(true);
        this.actualMessagePort = MESSAGE_PORT;

        try {
            this.multicastGroup = InetAddress.getByName(MULTICAST_GROUP);
            this.multicastSocket = new MulticastSocket(MULTICAST_PORT);
            this.multicastSocket.joinGroup(multicastGroup);

            // Încearcă să pornească serverul pe portul principal
            try {
                this.messageServerSocket = new ServerSocket(MESSAGE_PORT);
                this.actualMessagePort = MESSAGE_PORT;
            } catch (IOException e) {
                // Dacă portul este ocupat, găsește un port liber automat
                this.messageServerSocket = new ServerSocket(0); // Port liber
                this.actualMessagePort = messageServerSocket.getLocalPort();
                System.out.println("Note: Message port " + MESSAGE_PORT + " was busy, using port " + actualMessagePort);
            }
        } catch (IOException e) {
            System.err.println("Error initializing node: " + e.getMessage());
            System.exit(1);
        }
    }

    public void start() {
        System.out.println("=== HEARTBEAT NODE " + nodeId + " ===");
        try {
            System.out.println("Local Address: " + InetAddress.getLocalHost().getHostAddress());
        } catch (UnknownHostException e) {
            System.out.println("Local Address: Unknown");
        }
        System.out.println("Message Port: " + actualMessagePort);
        System.out.println("Multicast Group: " + MULTICAST_GROUP + ":" + MULTICAST_PORT);
        System.out.println("Commands: 'send <nodeId> <message>', 'list', 'quit', 'help'");
        System.out.println("----------------------------------------");

        new Thread(this::startMulticastSender).start();
        new Thread(this::startMulticastReceiver).start();
        new Thread(this::startMessageServer).start();
        new Thread(this::startMonitor).start();
        startConsoleReader();
    }

    private void startMulticastSender() {
        try {
            while (running.get()) {
                // Trimite informații despre nod prin multicast
                String multicastMsg = "HEARTBEAT:" + nodeId + ":" + actualMessagePort;
                byte[] buffer = multicastMsg.getBytes();
                DatagramPacket packet = new DatagramPacket(
                        buffer, buffer.length, multicastGroup, MULTICAST_PORT
                );
                multicastSocket.send(packet);

                Thread.sleep(HEARTBEAT_INTERVAL);
            }
        } catch (IOException | InterruptedException e) {
            if (running.get()) {
                System.err.println("Multicast sender error: " + e.getMessage());
            }
        }
    }

    private void startMulticastReceiver() {
        try {
            byte[] buffer = new byte[1024];

            while (running.get()) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                multicastSocket.setSoTimeout(1000);

                try {
                    multicastSocket.receive(packet);
                    String message = new String(packet.getData(), 0, packet.getLength());
                    processMulticastMessage(message, packet.getAddress());
                } catch (SocketTimeoutException e) {
                    // Continue checking
                }
            }
        } catch (IOException e) {
            if (running.get()) {
                System.err.println("Multicast receiver error: " + e.getMessage());
            }
        }
    }

    private void processMulticastMessage(String message, InetAddress sourceAddress) {
        if (message.startsWith("HEARTBEAT:")) {
            String[] parts = message.split(":");
            if (parts.length >= 3) {
                String sourceNode = parts[1];
                int sourceMessagePort = Integer.parseInt(parts[2]);

                // Verifică dacă nu este același nod (prin adresă sau port)
                boolean isSameNode = sourceNode.equals(nodeId) &&
                        sourceAddress.equals(multicastGroup);

                if (!isSameNode) {
                    updateNodeStatus(sourceNode, sourceAddress, sourceMessagePort);
                }
            }
        }
    }

    private void startMessageServer() {
        try {
            messageServerSocket.setSoTimeout(1000);

            while (running.get()) {
                try {
                    Socket clientSocket = messageServerSocket.accept();
                    new Thread(() -> handleMessage(clientSocket)).start();
                } catch (SocketTimeoutException e) {
                    // Continue checking
                }
            }
        } catch (IOException e) {
            if (running.get()) {
                System.err.println("Message server error: " + e.getMessage());
            }
        }
    }

    private void handleMessage(Socket clientSocket) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(clientSocket.getInputStream()))) {

            String message = reader.readLine();
            if (message != null) {
                System.out.println("RECEIVED MESSAGE: " + message);
                System.out.print("> ");
            }
        } catch (IOException e) {
            System.err.println("Error handling message: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                // Ignore close errors
            }
        }
    }

    private void startMonitor() {
        while (running.get()) {
            long currentTime = System.currentTimeMillis();
            List<String> deadNodes = new ArrayList<>();

            for (Map.Entry<String, NodeInfo> entry : nodes.entrySet()) {
                if (currentTime - entry.getValue().lastSeen > TIMEOUT_THRESHOLD) {
                    deadNodes.add(entry.getKey());
                }
            }

            for (String deadNode : deadNodes) {
                nodes.remove(deadNode);
                System.out.println("ALERT: Node " + deadNode + " is DOWN!");
                System.out.print("> ");
            }

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private void updateNodeStatus(String nodeId, InetAddress address, int messagePort) {
        long currentTime = System.currentTimeMillis();
        String uniqueId = nodeId + "@" + address.getHostAddress() + ":" + messagePort;

        boolean isNew = !nodes.containsKey(uniqueId);
        NodeInfo nodeInfo = new NodeInfo(nodeId, address, messagePort);
        nodeInfo.lastSeen = currentTime;
        nodes.put(uniqueId, nodeInfo);

        if (isNew) {
            System.out.println("NEW NODE DETECTED: " + nodeId + " (" + address.getHostAddress() + ":" + messagePort + ")");
            System.out.print("> ");
        } else {
            // Actualizează timestamp-ul pentru nodul existent
            nodes.get(uniqueId).lastSeen = currentTime;
        }
    }

    private void startConsoleReader() {
        try (BufferedReader consoleReader = new BufferedReader(new InputStreamReader(System.in))) {
            while (running.get()) {
                System.out.print("> ");
                String input = consoleReader.readLine();

                if (input == null) continue;

                if (input.equalsIgnoreCase("quit")) {
                    stop();
                    break;
                } else if (input.equalsIgnoreCase("list")) {
                    listNodes();
                } else if (input.startsWith("send ")) {
                    sendMessage(input.substring(5));
                } else if (input.equalsIgnoreCase("help")) {
                    showHelp();
                } else if (!input.trim().isEmpty()) {
                    System.out.println("Unknown command. Type 'help' for available commands.");
                }
            }
        } catch (IOException e) {
            System.err.println("Console reader error: " + e.getMessage());
        }
    }

    private void sendMessage(String input) {
        String[] parts = input.split(" ", 2);
        if (parts.length < 2) {
            System.out.println("Usage: send <nodeId> <message>");
            return;
        }

        String targetNode = parts[0];
        String message = parts[1];

        // Găsește toate nodurile cu ID-ul respectiv
        List<NodeInfo> targetNodes = new ArrayList<>();
        for (NodeInfo info : nodes.values()) {
            if (info.nodeId.equals(targetNode)) {
                targetNodes.add(info);
            }
        }

        if (targetNodes.isEmpty()) {
            System.out.println("Node " + targetNode + " not found or offline");
            return;
        }

        // Încearcă să trimită mesajul la primul nod online
        boolean sent = false;
        for (NodeInfo targetInfo : targetNodes) {
            try (Socket socket = new Socket(targetInfo.address, targetInfo.messagePort);
                 PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {

                writer.println("From " + nodeId + ": " + message);
                System.out.println("Message sent to " + targetNode + " (" + targetInfo.address.getHostAddress() + ")");
                sent = true;
                break;
            } catch (IOException e) {
                // Continue to next node
            }
        }

        if (!sent) {
            System.out.println("Failed to send message to " + targetNode + " - all nodes unreachable");
        }
    }

    private void listNodes() {
        System.out.println("=== ACTIVE NODES ===");
        long currentTime = System.currentTimeMillis();

        if (nodes.isEmpty()) {
            System.out.println("No other nodes detected");
        } else {
            // Grupează nodurile după nodeId pentru afișare mai clară
            Map<String, List<NodeInfo>> groupedNodes = new HashMap<>();
            for (NodeInfo info : nodes.values()) {
                groupedNodes.computeIfAbsent(info.nodeId, k -> new ArrayList<>()).add(info);
            }

            for (Map.Entry<String, List<NodeInfo>> entry : groupedNodes.entrySet()) {
                String nodeId = entry.getKey();
                List<NodeInfo> instances = entry.getValue();

                for (NodeInfo info : instances) {
                    String status = (currentTime - info.lastSeen < TIMEOUT_THRESHOLD) ? "ONLINE" : "OFFLINE";
                    String location = info.address.isAnyLocalAddress() ? "local" : info.address.getHostAddress();
                    System.out.printf("%s - %s (%s:%d) - Last seen: %dms ago%n",
                            nodeId, status, location, info.messagePort, currentTime - info.lastSeen);
                }
            }
        }
        System.out.println("====================");
    }

    private void showHelp() {
        System.out.println("=== AVAILABLE COMMANDS ===");
        System.out.println("list - Show all known nodes");
        System.out.println("send <nodeId> <message> - Send message to node");
        System.out.println("quit - Stop this node");
        System.out.println("help - Show this help");
        System.out.println("===========================");
    }

    public void stop() {
        running.set(false);
        if (multicastSocket != null) {
            try {
                multicastSocket.leaveGroup(multicastGroup);
                multicastSocket.close();
            } catch (IOException e) {
                // Ignore close errors
            }
        }
        if (messageServerSocket != null) {
            try {
                messageServerSocket.close();
            } catch (IOException e) {
                // Ignore close errors
            }
        }
        System.out.println("Heartbeat node " + nodeId + " stopped.");
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java HeartbeatNode <nodeId>");
            System.out.println("Examples:");
            System.out.println("  On same computer:");
            System.out.println("    Terminal 1: java HeartbeatNode node1");
            System.out.println("    Terminal 2: java HeartbeatNode node2");
            System.out.println("    Terminal 3: java HeartbeatNode node3");
            System.out.println("  On different computers:");
            System.out.println("    Computer 1: java HeartbeatNode node1");
            System.out.println("    Computer 2: java HeartbeatNode node2");
            System.out.println("    Computer 3: java HeartbeatNode node3");
            return;
        }

        String nodeId = args[0];

        HeartbeatNode node = new HeartbeatNode(nodeId);

        Runtime.getRuntime().addShutdownHook(new Thread(node::stop));

        node.start();
    }
}