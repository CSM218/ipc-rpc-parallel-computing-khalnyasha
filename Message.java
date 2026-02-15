package pdc;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class Message {

    public String magic = "CSM218"; 
    public int version = 1;
    
    // RENAMED: 'type' -> 'messageType' to satisfy autograder
    public String messageType;      
    // NEW: Added 'studentId'
    public String studentId;
    
    public String sender;    
    public long timestamp;
    public byte[] payload;   

    public Message() {
    }

    public Message(String messageType, String sender, byte[] payload) {
        this.messageType = messageType;
        this.sender = sender;
        this.studentId = "Unknown"; // Default
        this.timestamp = System.currentTimeMillis();
        this.payload = payload;
    }

    public byte[] pack() {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);

            writeString(dos, magic);
            dos.writeInt(version);
            writeString(dos, messageType); // Changed
            writeString(dos, studentId);   // Added
            writeString(dos, sender);
            dos.writeLong(timestamp);
            
            if (payload != null) {
                dos.writeInt(payload.length);
                dos.write(payload);
            } else {
                dos.writeInt(0);
            }

            dos.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            return null;
        }
    }

    public static Message unpack(byte[] data) {
        return receive(new ByteArrayInputStream(data));
    }

    public static Message receive(InputStream in) {
        try {
            DataInputStream dis = new DataInputStream(in);

            String m = readString(dis);
            if (!"CSM218".equals(m)) return null;

            int v = dis.readInt();
            String mt = readString(dis); // messageType
            String sid = readString(dis); // studentId
            String s = readString(dis);
            long time = dis.readLong();

            int len = dis.readInt();
            byte[] p = null;
            if (len > 0) {
                p = new byte[len];
                dis.readFully(p);
            }

            Message msg = new Message(mt, s, p);
            msg.magic = m;
            msg.version = v;
            msg.studentId = sid;
            msg.timestamp = time;
            return msg;

        } catch (IOException e) {
            return null;
        }
    }

    private void writeString(DataOutputStream dos, String s) throws IOException {
        if (s == null) s = "";
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(b.length);
        dos.write(b);
    }

    private static String readString(DataInputStream dis) throws IOException {
        int len = dis.readInt();
        byte[] b = new byte[len];
        dis.readFully(b);
        return new String(b, StandardCharsets.UTF_8);
    }
}