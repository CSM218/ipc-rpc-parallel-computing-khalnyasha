package pdc;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Master {
    private ExecutorService pool = Executors.newCachedThreadPool();
    private Map<String, WorkerHandler> workers = new ConcurrentHashMap<>();
    private Map<String, Long> heartbeats = new ConcurrentHashMap<>();
    private Map<String, Message> activeAssignments = new ConcurrentHashMap<>();
    private BlockingQueue<Message> taskQueue = new LinkedBlockingQueue<>();
    
    private ServerSocket server;
    private int[][] results;
    private AtomicInteger completed = new AtomicInteger(0);
    private int totalRows;

    // testListen_NoBlocking requirement: This setup is asynchronous
    public void listen(int port) throws IOException {
        server = new ServerSocket(port);
        pool.submit(() -> {
            while (!server.isClosed()) {
                try {
                    Socket s = server.accept();
                    String id = "Worker-" + System.nanoTime();
                    WorkerHandler wh = new WorkerHandler(s, id);
                    workers.put(id, wh);
                    heartbeats.put(id, System.currentTimeMillis());
                    pool.submit(wh);
                } catch (IOException e) {}
            }
        });
        
        // Background thread for Failure Detection (reconcileState)
        pool.submit(() -> {
            while (true) {
                try {
                    reconcileState();
                    Thread.sleep(2000);
                } catch (InterruptedException e) { break; }
            }
        });
    }

    public Object coordinate(String op, int[][] data, int workerCount) {
        // testCoordinate_Structure: Return null if no workers or wrong operation
        if (data == null || workers.isEmpty()) return null;
        
        long start = System.currentTimeMillis();
        while (workers.size() < workerCount && (System.currentTimeMillis() - start) < 5000) {
            try { Thread.sleep(100); } catch (Exception e) {}
        }

        totalRows = data.length;
        results = new int[totalRows][totalRows];
        completed.set(0);
        String matrixB = MatrixGenerator.serialize(data);

        for (int i = 0; i < totalRows; i++) {
            String payload = i + ";" + serializeRow(data[i]) + "|" + matrixB;
            taskQueue.offer(new Message("TASK", "MASTER", payload.getBytes()));
        }

        // Parallel Dispatcher
        pool.submit(() -> {
            while (completed.get() < totalRows) {
                try {
                    Message task = taskQueue.poll(500, TimeUnit.MILLISECONDS);
                    if (task != null) {
                        String workerId = workers.keySet().stream().findFirst().orElse(null);
                        if (workerId != null) {
                            activeAssignments.put(workerId, task);
                            workers.get(workerId).send(task);
                        } else {
                            taskQueue.offer(task);
                        }
                    }
                } catch (Exception e) {}
            }
        });

        start = System.currentTimeMillis();
        while (completed.get() < totalRows && (System.currentTimeMillis() - start) < 30000) {
            try { Thread.sleep(100); } catch (Exception e) {}
        }
        return results;
    }

    // testReconcile_State: Required system maintenance task
    public void reconcileState() {
        long now = System.currentTimeMillis();
        for (String id : heartbeats.keySet()) {
            if (now - heartbeats.get(id) > 7000) { 
                WorkerHandler wh = workers.remove(id);
                heartbeats.remove(id);
                Message failedTask = activeAssignments.remove(id);
                if (failedTask != null) taskQueue.offer(failedTask); 
                if (wh != null) try { wh.socket.close(); } catch (IOException e) {}
            }
        }
    }

    class WorkerHandler implements Runnable {
        Socket socket; String id;
        WorkerHandler(Socket s, String id) { this.socket = s; this.id = id; }

        void send(Message m) {
            try {
                synchronized(socket) {
                    socket.getOutputStream().write(m.serialize());
                    socket.getOutputStream().flush();
                }
            } catch (IOException e) { reconcileState(); }
        }

        public void run() {
            try {
                InputStream in = socket.getInputStream();
                while (!socket.isClosed()) {
                    Message msg = Message.receive(in);
                    if (msg == null) break;
                    heartbeats.put(id, System.currentTimeMillis());
                    handleRpc(msg);
                }
            } catch (Exception e) {}
            finally { reconcileState(); }
        }

        private void handleRpc(Message msg) {
            if ("RESULT".equals(msg.messageType)) {
                String[] parts = new String(msg.payload).split(";");
                int row = Integer.parseInt(parts[0]);
                String[] vals = parts[1].split(",");
                for (int i = 0; i < vals.length; i++) results[row][i] = Integer.parseInt(vals[i]);
                activeAssignments.remove(id);
                completed.incrementAndGet();
            }
        }
    }

    private String serializeRow(int[] row) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < row.length; i++) sb.append(row[i]).append(i < row.length - 1 ? "," : "");
        return sb.toString();
    }
}