package pdc;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.*;

public class Worker {
    private Socket socket;
    private String id;
    private ExecutorService threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private boolean active = false;

    public void joinCluster(String host, int port) {
        try {
            socket = new Socket();
            socket.connect(new InetSocketAddress(host, port), 2000);
            active = true;
            id = System.getenv("WORKER_ID") != null ? System.getenv("WORKER_ID") : "W-" + System.nanoTime();
            
            send(new Message("REGISTER", id, null));

            // Failure Detection: Active Heartbeat pulse
            new Thread(() -> {
                while (active) {
                    try {
                        Thread.sleep(3000);
                        send(new Message("HEARTBEAT", id, null));
                    } catch (Exception e) { active = false; }
                }
            }).start();

            try (InputStream in = socket.getInputStream()) {
                while (active) {
                    Message msg = Message.receive(in);
                    if (msg == null) break;
                    handleRpc(msg);
                }
            }
        } catch (IOException e) {
            System.err.println("Worker connection failed.");
        }
    }

    private void handleRpc(Message msg) {
        if ("TASK".equals(msg.messageType)) {
            threadPool.submit(() -> {
                try {
                    String[] parts = new String(msg.payload).split("\\|");
                    int[][] matrixB = MatrixGenerator.parse(parts[1]);
                    String[] rowHeader = parts[0].split(";");
                    int rowIdx = Integer.parseInt(rowHeader[0]);
                    String[] rowVals = rowHeader[1].split(",");
                    
                    int[] resRow = new int[matrixB[0].length];
                    for (int j = 0; j < matrixB[0].length; j++) {
                        for (int k = 0; k < rowVals.length; k++) {
                            resRow[j] += Integer.parseInt(rowVals[k]) * matrixB[k][j];
                        }
                    }
                    
                    StringBuilder sb = new StringBuilder().append(rowIdx).append(";");
                    for (int i = 0; i < resRow.length; i++) sb.append(resRow[i]).append(i < resRow.length - 1 ? "," : "");
                    
                    Message response = new Message("RESULT", id, sb.toString().getBytes());
                    send(response);
                } catch (Exception e) {}
            });
        }
    }

    private void send(Message m) {
        try {
            synchronized(socket) {
                if (!socket.isClosed()) {
                    socket.getOutputStream().write(m.serialize());
                    socket.getOutputStream().flush();
                }
            }
        } catch (IOException e) { active = false; }
    }

    public void execute() { System.out.println("Worker thread initiated."); }
}