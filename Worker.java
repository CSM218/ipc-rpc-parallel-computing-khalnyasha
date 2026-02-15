package pdc;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Worker {

    private Socket socket;
    private boolean isRunning = false;
    private String workerId;
    private String studentId;
    private final Object lock = new Object();
    private ExecutorService threadPool;

    public Worker() {
        int cores = Runtime.getRuntime().availableProcessors();
        this.threadPool = Executors.newFixedThreadPool(cores);
    }

    public void joinCluster(String masterHost, int port) {
        try {
            socket = new Socket(masterHost, port);
            isRunning = true;

            // Environment Variables check
            workerId = System.getenv("WORKER_ID");
            if (workerId == null) workerId = "Worker-" + System.nanoTime();
            
            studentId = System.getenv("STUDENT_ID");
            if (studentId == null) studentId = "Unknown";

            Message reg = new Message("REGISTER", workerId, null);
            reg.studentId = studentId;
            send(reg);

            // Heartbeat Thread
            new Thread(() -> {
                while (isRunning) {
                    try {
                        Thread.sleep(2000);
                        Message hb = new Message("HEARTBEAT", workerId, null);
                        hb.studentId = studentId;
                        send(hb);
                    } catch (Exception e) { break; }
                }
            }).start();

            // Main Loop
            while (isRunning) {
                Message msg = Message.receive(socket.getInputStream());
                if (msg == null) break;

                if ("TASK".equals(msg.messageType)) {
                    threadPool.submit(() -> handleTask(msg));
                } 
            }
        } catch (IOException e) {}
    }

    private void handleTask(Message msg) {
        try {
            String text = new String(msg.payload);
            String[] parts = text.split("\\|");
            
            String[] header = parts[0].split(";");
            int rowIndex = Integer.parseInt(header[0]);
            String[] rowStr = header[1].split(",");
            
            int[] rowA = new int[rowStr.length];
            for(int i=0; i<rowStr.length; i++) rowA[i] = Integer.parseInt(rowStr[i]);

            String[] rowsB = parts[1].split("\\\\");
            int cols = rowsB[0].split(",").length;
            int[][] matrixB = new int[rowsB.length][cols];
            
            for(int i=0; i<rowsB.length; i++) {
                String[] vals = rowsB[i].split(",");
                for(int j=0; j<vals.length; j++) {
                    matrixB[i][j] = Integer.parseInt(vals[j]);
                }
            }

            int[] resultRow = new int[cols];
            for(int j=0; j<cols; j++) {
                int sum = 0;
                for(int k=0; k<rowA.length; k++) {
                    sum += rowA[k] * matrixB[k][j];
                }
                resultRow[j] = sum;
            }

            StringBuilder sb = new StringBuilder();
            sb.append(rowIndex).append(";");
            for(int i=0; i<resultRow.length; i++) {
                sb.append(resultRow[i]).append(i < resultRow.length-1 ? "," : "");
            }

            Message res = new Message("RESULT", workerId, sb.toString().getBytes());
            res.studentId = studentId;
            send(res);

        } catch (Exception e) {}
    }

    private void send(Message msg) {
        synchronized (lock) {
            try {
                if(socket != null && !socket.isClosed()) {
                    socket.getOutputStream().write(msg.pack());
                    socket.getOutputStream().flush();
                }
            } catch (IOException e) {}
        }
    }

    public void execute() {}
}