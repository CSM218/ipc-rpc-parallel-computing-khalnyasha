package pdc;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Master {

    // Thread pools for system management and worker handling
    private ExecutorService systemThreads = Executors.newCachedThreadPool();
    
    // [CONCURRENT_COLLECTIONS] - Required for thread safety
    private Map<String, WorkerHandler> workers = new ConcurrentHashMap<>();
    
    // [FAILURE_DETECTION] - Map to track the last heartbeat timestamp for each worker
    private Map<String, Long> lastHeartbeat = new ConcurrentHashMap<>();
    
    // [RECOVERY_MECHANISM] - Map to track which task is assigned to which worker
    private Map<String, Message> assignedTasks = new ConcurrentHashMap<>();
    
    // [REQUEST_QUEUING] - Queue for pending tasks
    private BlockingQueue<Message> taskQueue = new LinkedBlockingQueue<>();

    private boolean isRunning = true;
    private ServerSocket serverSocket;
    
    // [ATOMIC_OPERATIONS] - Atomic counter for results
    private int[][] finalResult;
    private AtomicInteger tasksDone = new AtomicInteger(0);
    private int totalTasks = 0;

    public void listen(int port) throws IOException {
        serverSocket = new ServerSocket(port);
        
        // 1. Connection Listener Thread
        systemThreads.submit(() -> {
            while (isRunning && !serverSocket.isClosed()) {
                try {
                    Socket s = serverSocket.accept();
                    String workerId = "W-" + System.nanoTime(); 
                    WorkerHandler wh = new WorkerHandler(s, workerId);
                    
                    // Register the new worker
                    workers.put(workerId, wh);
                    lastHeartbeat.put(workerId, System.currentTimeMillis());
                    
                    systemThreads.submit(wh);
                } catch (IOException e) {
                    // Ignore socket closure errors during shutdown
                }
            }
        });
        
        // 2. [FAILURE_DETECTION] Background Monitor Thread
        // This thread runs continuously to check for dead workers
        systemThreads.submit(() -> {
            while (isRunning) {
                try {
                    reconcileState();
                    Thread.sleep(1000); // Check every second
                } catch (InterruptedException e) { break; }
            }
        });
    }

    public Object coordinate(String operation, int[][] data, int workerCount) {
        String envStudentId = System.getenv("STUDENT_ID");

        // Wait for workers to join
        long start = System.currentTimeMillis();
        while (workers.size() < workerCount && (System.currentTimeMillis() - start) < 5000) {
            try { Thread.sleep(100); } catch (Exception e) {}
        }

        if (workers.isEmpty()) return null;

        int n = data.length;
        finalResult = new int[n][n];
        totalTasks = n;
        tasksDone.set(0);

        String matrixBStr = serializeMatrix(data);

        // Fill the Queue
        for (int i = 0; i < n; i++) {
            String rowStr = serializeRow(data[i]);
            String payload = i + ";" + rowStr + "|" + matrixBStr;
            Message task = new Message("TASK", "MASTER", payload.getBytes());
            task.studentId = envStudentId;
            taskQueue.offer(task);
        }

        // 3. Dispatcher Loop
        systemThreads.submit(() -> {
            while (tasksDone.get() < totalTasks) {
                try {
                    // Poll with timeout to allow checking for system shutdown
                    Message task = taskQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (task != null) {
                        boolean assigned = false;
                        for (String wid : workers.keySet()) {
                            WorkerHandler w = workers.get(wid);
                            // Assign to first available, live worker
                            if (w != null && !w.socket.isClosed()) {
                                assignedTasks.put(wid, task); // [RECOVERY] Track assignment
                                w.send(task);
                                assigned = true;
                                break;
                            }
                        }
                        // If no worker available, put back in queue
                        if (!assigned) taskQueue.offer(task);
                    }
                } catch (Exception e) {}
            }
        });

        // Wait for results
        start = System.currentTimeMillis();
        while (tasksDone.get() < totalTasks && (System.currentTimeMillis() - start) < 30000) {
            try { Thread.sleep(50); } catch (Exception e) {}
        }

        return finalResult;
    }

    // [RECOVERY_MECHANISM] & [FAILURE_DETECTION]
    public void reconcileState() {
        long now = System.currentTimeMillis();
        long TIMEOUT = 5000; // 5 seconds timeout

        for (String id : workers.keySet()) {
            WorkerHandler w = workers.get(id);
            Long lastSeen = lastHeartbeat.get(id);
            
            // Check if worker is dead (socket closed OR timed out)
            boolean isDead = w.socket.isClosed();
            if (lastSeen != null && (now - lastSeen > TIMEOUT)) {
                isDead = true;
                // System.out.println("Worker " + id + " timed out!"); 
            }

            if (isDead) {
                // 1. Remove worker
                workers.remove(id);
                lastHeartbeat.remove(id);
                
                // 2. [RECOVERY] Check if it had an assigned task
                Message lostTask = assignedTasks.remove(id);
                if (lostTask != null) {
                    // System.out.println("Recovering task from dead worker " + id);
                    taskQueue.offer(lostTask); // Re-queue the task!
                }
                
                try { w.socket.close(); } catch (IOException e) {}
            }
        }
    }

    class WorkerHandler implements Runnable {
        Socket socket;
        String id;
        
        public WorkerHandler(Socket s, String id) { 
            this.socket = s; 
            this.id = id; 
        }

        public void send(Message msg) {
            try {
                synchronized (socket) {
                    if (!socket.isClosed()) {
                        socket.getOutputStream().write(msg.serialize());
                        socket.getOutputStream().flush();
                    }
                }
            } catch (IOException e) { 
                // If send fails, trigger immediate check
                reconcileState(); 
            }
        }

        @Override
        public void run() {
            try {
                InputStream in = socket.getInputStream();
                while (isRunning && !socket.isClosed()) {
                    Message msg = Message.receive(in);
                    if (msg == null) break;

                    // Update Heartbeat on ANY message
                    lastHeartbeat.put(id, System.currentTimeMillis());

                    if ("RESULT".equals(msg.messageType)) {
                        handleResult(msg);
                    } else if ("HEARTBEAT".equals(msg.messageType)) {
                        // Heartbeat explicitly handled by updating timestamp above
                    }
                }
            } catch (Exception e) {
                // Connection error
            } finally { 
                reconcileState(); // Trigger recovery on exit
            }
        }

        private void handleResult(Message msg) {
            String text = new String(msg.payload);
            String[] parts = text.split(";");
            int rowIdx = Integer.parseInt(parts[0]);
            String[] vals = parts[1].split(",");
            
            for (int c = 0; c < vals.length; c++) {
                finalResult[rowIdx][c] = Integer.parseInt(vals[c]);
            }
            
            // Task complete, remove from tracking
            assignedTasks.remove(id);
            tasksDone.incrementAndGet();
        }
    }

    // --- Helpers ---
    private String serializeMatrix(int[][] data) {
        StringBuilder sb = new StringBuilder();
        for(int i=0; i<data.length; i++) {
            for(int j=0; j<data[i].length; j++) {
                sb.append(data[i][j]).append(j<data[i].length-1 ? "," : "");
            }
            if(i<data.length-1) sb.append("\\");
        }
        return sb.toString();
    }

    private String serializeRow(int[] row) {
        StringBuilder sb = new StringBuilder();
        for(int i=0; i<row.length; i++) {
            sb.append(row[i]).append(i<row.length-1 ? "," : "");
        }
        return sb.toString();
    }
}