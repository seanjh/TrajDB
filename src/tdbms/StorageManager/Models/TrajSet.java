package tdbms.StorageManager.Models;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;

public class TrajSet {
    public static final int totalHeaderBytes = Integer.BYTES;
    private LinkedList<TrajSequence> sequences;

    public TrajSet(LinkedList<TrajSequence> sequences) {
        this.sequences = sequences;

    }

    public TrajSet(ByteBuffer buff, FileChannel fc) throws IOException {
        this.sequences = new LinkedList<>();

        fc.read(buff);
        buff.flip();
        // Read the length/count of sequences in this trajectory set
        int size = buff.getInt();

        // Remaining bytes are the actual sequences
        TrajSequence seq;
        for ( ; size > 0; size--) {
            if (buff.limit() - buff.position() < TrajSequence.totalBytes) {
                refillBuffer(buff, fc);
            }
            seq = new TrajSequence(buff);
            this.sequences.add(seq);
        }
    }

    private void refillBuffer(ByteBuffer buff, FileChannel fc) throws IOException {
        // Allocate a new buffer covering the bytes already read from the input/current buffer
        ByteBuffer tmp = ByteBuffer.allocate(buff.position() - 1);
        fc.read(tmp);
        tmp.flip();

        // Shift the remaining bytes from the input buffer to the front of the buffer
        int buffByteNum = 0;
        while (buff.hasRemaining()) {
            buff.put(buffByteNum, buff.get());
            buffByteNum++;
        }

        // Make sure we don't write more that was read into the new tmp buffer
        if (tmp.limit() + buffByteNum < buff.capacity()) {
            buff.limit(tmp.limit() + buffByteNum);
        }

        // Populate the remainder of the buffer with new bytes read into tmp
        buff.position(buffByteNum);
        while (buff.hasRemaining()) {
            buff.put(tmp.get());
        }
        buff.flip();
    }

    public LinkedList<TrajSequence> getTrajSequences() { return sequences; }

    public ByteBuffer getBuffer() {
        ByteBuffer buff = ByteBuffer.allocate(getTotalBytes());
        buff.putInt(getSequenceCount());
        for (TrajSequence seq : sequences) {
            buff.put(seq.getBuffer().array());
        }
        buff.rewind();
        return buff;
    }

    public int getTotalBytes() {
        return totalHeaderBytes + (sequences.size() * TrajSequence.totalBytes);
    }

    public int getSequenceCount() {
        return sequences.size();
    }

    @Override
    public String toString() {
        StringBuilder out = new StringBuilder();
        for (TrajSequence seq : this.sequences) {
            out.append(seq.toString());
            out.append("\n");
        }
        return out.toString();
    }
}
