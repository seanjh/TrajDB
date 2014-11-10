package tdbms.StorageManager.Models;

import java.nio.ByteBuffer;

public class TrajIndex {
    public static final int totalBytes = 16;
    public static final int invalidId = -1;
    private long key;
    private long keyPosition;
    private long dataPosition;

    public TrajIndex(long key, long keyPosition, long dataPosition) {
        this.key = key;
        this.keyPosition = keyPosition;
        this.dataPosition = dataPosition;
    }

    public TrajIndex(ByteBuffer buff, long keyPosition) {
        this.key = buff.getLong();
        this.keyPosition = keyPosition;
        this.dataPosition = buff.getLong();
    }

    public ByteBuffer getBuffer() {
        ByteBuffer buff = ByteBuffer.allocate(totalBytes);
        buff.putLong(this.key);
        buff.putLong(this.dataPosition);
        buff.rewind();
        return buff;
    }

    public long getKey() { return this.key; }
    public long getKeyPosition() { return this.keyPosition; }
    public void setKeyPosition(long position) { this.keyPosition = position; }
    public long getDataPosition() { return this.dataPosition; }
    public void setDataPosition(long position) { this.dataPosition = position; }

    public boolean isDeletedIndex() {
        return this.getKey() == TrajIndex.invalidId;
    }

    @Override
    public String toString() {
        return "Key=" + Long.toString(this.key) + "," +
                "Position=" + Long.toString(this.dataPosition);
    }
}
