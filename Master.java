package pdc;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Master {
    private ExecutorService systemThreads = Executors.newCachedThreadPool();
    private Map<String, WorkerHandler> workers = new ConcurrentHashMap<>();
    private Map<String, Long> lastHeartbeat = new ConcurrentHashMap<>();
    private BlockingQueue<Message> taskQueue = new LinkedBlockingQueue<>();
    private Map<String, Message> assignedTasks = new ConcurrentHashMap<>();

    private boolean isRunning = true;
    private ServerSocket serverSocket;
    private int[][] finalResult;
    private AtomicInteger tasksDone = new AtomicInteger(0);
    private int totalTasks = 0;

    public void listen(int port) throws IOException {
        serverSocket = new ServerSocket(port);
        systemThreads.submit(() -> {
            while (isRunning && !serverSocket.isClosed()) {
                try {
                    Socket s = serverSocket.accept();
                    String id = "W-" + System.nanoTime();
                    WorkerHandler wh = new WorkerHandler(s, id);
                    workers.put(id, wh);
                    lastHeartbeat.put(id, System.currentTimeMillis());
                    systemThreads.submit(wh);
                } catch (IOException e) {}
            }
        });
        
        // Background thread to constantly reconcile state
        systemThreads.submit(() -> {
            while (isRunning) {
                reconcileState();
                try { Thread.sleep(2000); } catch (InterruptedException e) {}
            }
        });
    }

    public Object coordinate(String operation, int[][] data, int workerCount) {
        if (operation == null || !operation.contains("MULTIPLY") && !operation.contains("TASK")) return null;
        
        String envStudentId = System.getenv("STUDENT_ID");
        long start = System.currentTimeMillis();
        while (workers.size() < workerCount && (System.currentTimeMillis() - start) < 5000) {
            try { Thread.sleep(100); } catch (Exception e) {}
        }

        int n = data.length;
        finalResult = new int[n][n];
        totalTasks = n;
        tasksDone.set(0);

        String matrixBStr = serializeMatrix(data);
        for (int i = 0; i < n; i++) {
            Message task = new Message("TASK", "MASTER", (i + ";" + serializeRow(data[i]) + "|" + matrixBStr).getBytes());
            task.studentId = envStudentId;
            taskQueue.offer(task);
        }

        systemThreads.submit(() -> {
            while (tasksDone.get() < totalTasks) {
                try {
                    Message task = taskQueue.poll(500, TimeUnit.MILLISECONDS);
                    if (task != null) {
                        String targetId = null;
                        for (String id : workers.keySet()) {
                            if (!assignedTasks.containsKey(id)) { targetId = id; break; }
                        }
                        if (targetId == null && !workers.isEmpty()) targetId = workers.keySet().iterator().next();
                        
                        if (targetId != null) {
                            assignedTasks.put(targetId, task);
                            workers.get(targetId).send(task);
                        } else {
                            taskQueue.offer(task);
                        }
                    }
                } catch (Exception e) {}
            }
        });

        start = System.currentTimeMillis();
        while (tasksDone.get() < totalTasks && (System.currentTimeMillis() - start) < 30000) {
            try { Thread.sleep(50); } catch (Exception e) {}
        }
        return finalResult;
    }

    public void reconcileState() {
        long now = System.currentTimeMillis();
        for (String id : lastHeartbeat.keySet()) {
            if (now - lastHeartbeat.get(id) > 6000) { // 6 second timeout
                WorkerHandler wh = workers.remove(id);
                lastHeartbeat.remove(id);
                Message lostTask = assignedTasks.remove(id);
                if (lostTask != null) taskQueue.offer(lostTask);
                if (wh != null) try { wh.socket.close(); } catch (IOException e) {}
            }
        }
    }

    class WorkerHandler implements Runnable {
        Socket socket;
        String id;
        public WorkerHandler(Socket s, String id) { this.socket = s; this.id = id; }

        public void send(Message msg) {
            try {
                synchronized (socket) {
                    socket.getOutputStream().write(msg.serialize());
                    socket.getOutputStream().flush();
                }
            } catch (IOException e) {}
        }

        @Override
        public void run() {
            try {
                while (isRunning) {
                    Message msg = Message.receive(socket.getInputStream());
                    if (msg == null) break;
                    lastHeartbeat.put(id, System.currentTimeMillis());
                    if ("RESULT".equals(msg.messageType)) {
                        String[] parts = new String(msg.payload).split(";");
                        int rowIdx = Integer.parseInt(parts[0]);
                        String[] vals = parts[1].split(",");
                        for (int c = 0; c < vals.length; c++) finalResult[rowIdx][c] = Integer.parseInt(vals[c]);
                        assignedTasks.remove(id);
                        tasksDone.incrementAndGet();
                    }
                }
            } catch (Exception e) {}
            finally { reconcileState(); }
        }
    }

    private String serializeMatrix(int[][] d) {
        StringBuilder sb = new StringBuilder();
        for(int i=0; i<d.length; i++) {
            for(int j=0; j<d[i].length; j++) sb.append(d[i][j]).append(j<d[i].length-1 ? "," : "");
            if(i<d.length-1) sb.append("\\");
        }
        return sb.toString();
    }

    private String serializeRow(int[] r) {
        StringBuilder sb = new StringBuilder();
        for(int i=0; i<r.length; i++) sb.append(r[i]).append(i<r.length-1 ? "," : "");
        return sb.toString();
    }
}