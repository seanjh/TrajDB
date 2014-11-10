package tdbms.StorageManager.Controllers;

import tdbms.QueryProcessor.TDBMSException;
import tdbms.StorageManager.Models.TrajIndex;
import tdbms.StorageManager.Models.TrajIndexFile;
import tdbms.StorageManager.Models.TrajSet;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

public class IndexController extends DatabaseController {

    public IndexController(String tname) throws TDBMSException {
        super(tname);
    }

    public long getMaxId() {
        long result = 0;
        try (RandomAccessFile file = new RandomAccessFile(this.indexFile, "rw");
             FileChannel fc = file.getChannel()) {
            if (fc.size() > TrajIndexFile.indexBodyPosition) {
                ByteBuffer buff = ByteBuffer.allocate(TrajIndex.totalBytes);
                FileLock lock;
                TrajIndex index;

                // Skip to the end of the index file
                fc.position(fc.size() - TrajIndex.totalBytes);
                lock = fc.lock(fc.position(), blockSize, false);
                // Read the last index
                fc.read(buff);
                buff.flip();
                index = new TrajIndex(buff, fc.position());
                lock.release();
                result = index.getKey();
            }
        } catch (IOException e) { e.printStackTrace(); }
        return result;
    }

    private TrajIndex getIndexById(long id) {

        TrajIndex index = null;
        try (RandomAccessFile file = new RandomAccessFile(this.indexFile, "rw");
             FileChannel fc = file.getChannel()) {

            if (fc.size() > TrajIndexFile.indexBodyPosition) {
                ByteBuffer buff = ByteBuffer.allocate(blockSize);
                fc.position(TrajIndexFile.indexBodyPosition);
                FileLock lock = fc.lock(fc.position(), blockSize, false);

                fc.read(buff);
                buff.flip();
                long keyPosition = fc.position() - buff.limit() + buff.position();
                index = new TrajIndex(buff, keyPosition);
                while (index.getKey() != id &&
                        (buff.position() < buff.limit() || fc.position() < fc.size())) {
                    keyPosition = fc.position() - buff.limit() + buff.position();
                    index = new TrajIndex(buff, keyPosition);
                    if (!buff.hasRemaining()) {
                        lock.release();
                        lock = fc.lock(fc.position(), blockSize, false);
                        buff.clear();
                        fc.read(buff);
                        buff.flip();
                    }
                }

                lock.release();
            }
        } catch (IOException e) { e.printStackTrace(); }

        if (index != null && index.getKey() == id)
            return index;
        return null;
    }

    public long getDataPositionById(long id) {
        TrajIndex index = getIndexById(id);
        if (index != null)
            return index.getDataPosition();
        return invalidPosition;
    }

    public void addNewIndex(long id, long dataPosition) {
        try (RandomAccessFile file = new RandomAccessFile(this.indexFile, "rw");
             FileChannel fc = file.getChannel()) {

            // Add the new index key and data position at the end of the file
            fc.position(fc.size());
            TrajIndex index = new TrajIndex(id, fc.position(), dataPosition);
            ByteBuffer buff = index.getBuffer();
            fc.write(buff);

            // Update the index key count header value
            //fc.position(TrajIndexFile.keysCountPosition);
            //buff = ByteBuffer.allocate(Long.BYTES);
            //buff.putLong(TrajIndexFile.getKeysCount(fc) + 1);
            //buff.rewind();
            //fc.write(buff);

        } catch (IOException e) { e.printStackTrace(); }
    }

    public void deleteIndex(long id) throws TDBMSException {
        TrajIndex index = getIndexById(id);
        if (index != null) {
            TrajIndex deleted = new TrajIndex(-1, index.getKeyPosition(), invalidPosition);
            try (RandomAccessFile file = new RandomAccessFile(this.indexFile, "rw");
                 FileChannel fc = file.getChannel()) {
                //System.out.printf("Deleting Id %d from index\n", id);
                fc.position(deleted.getKeyPosition());
                FileLock lock = fc.lock(fc.position(), TrajIndex.totalBytes, false);
                ByteBuffer buff = deleted.getBuffer();
                fc.write(buff);
                lock.release();

                // Update the index deleted count header value
                //fc.position(TrajIndexFile.deletedCountPosition);
                long newDeletedCount = TrajIndexFile.getDeletedCount(fc) + 1;
                buff = ByteBuffer.allocate(Long.BYTES);
                buff.putLong(newDeletedCount);
                buff.rewind();
                fc.position(TrajIndexFile.deletedCountPosition);
                fc.write(buff);

                // When deleted keys represent > 25% of the index file, clean the index
                if ((float) newDeletedCount / (float) TrajIndexFile.getKeysCount(fc) > 0.25) {
                    //cleanIndexFile();
                }
            } catch (IOException e) { e.printStackTrace(); }
        } else {
            System.out.printf("Id %d does not exist. Cannot delete this Id from index.\n", id);
        }
    }

    /*private boolean bufferHasIndexRemaining(ByteBuffer buff) {
        return !buff.hasRemaining() || (buff.limit() - 1) - buff.position() < TrajIndex.totalBytes;
    }

    private boolean hasFileRemaining(FileChannel fc, ByteBuffer buff) throws IOException {
        return fc.position() - (buff.limit() - 1 - buff.position()) - 1 < fc.size();
    }

    private void addActiveIndexToOutput(FileChannel fc, ByteBuffer buff, TrajIndex index, long dataOffset) throws IOException {
        index.setDataPosition(index.getKeyPosition() - dataOffset);
        buff.put(index.getBuffer().array());
        if (!bufferHasIndexRemaining(buff)) {
            buff.flip();
            fc.write(buff);
            buff.clear();
        }
    }

    public void cleanIndexFile() throws TDBMSException {
        // TODO: Implement
        try (RandomAccessFile file = new RandomAccessFile(this.indexFile, "rw");
             FileChannel fcIn = file.getChannel();
             FileChannel fcOut = file.getChannel()) {

            FileLock lock = fcOut.lock();
            ByteBuffer inBuff = ByteBuffer.allocate(blockSize);
            ByteBuffer outBuff = ByteBuffer.allocate(blockSize);
            refillIndexBuffer(fcIn, inBuff);
            TrajIndex index = new TrajIndex(inBuff, 0);
            long headPosition = index.getDataPosition();
            TrajIndex head = index;
            TrajIndex tailStart;
            TrajIndex tailEnd;
            long nextActiveKeyPosition = 0;
            long dataOffset = 0;
            //long indexOffset = 0;
            while (hasFileRemaining(fcIn, inBuff)) {
                if (!index.isDeletedIndex()) {
                    // Handle active index entries
                    addActiveIndexToOutput(fcOut, outBuff, index, dataOffset);
                    if (!bufferHasIndexRemaining(inBuff)) {
                        refillIndexBuffer(fcIn, inBuff);
                    }
                    head = index;
                    headPosition = index.getDataPosition();
                    // Get the next index entry from the buffer
                    index = new TrajIndex(inBuff, nextActiveKeyPosition);
                    nextActiveKeyPosition += TrajIndex.totalBytes;
                } else {
                    // Find the last byte of the active TrajSet head
                    TrajController tc = new TrajController(tableName);
                    TrajSet set = tc.getTrajSetFromPosition(headPosition);
                    headPosition += set.getTotalBytes();

                    // Handle erased index entries
                    // Continue scanning input until an active index entry is found
                    while (hasFileRemaining(fcIn, inBuff) && index.isDeletedIndex()) {
                        if (!bufferHasIndexRemaining(inBuff)) {
                            refillIndexBuffer(fcIn, inBuff);
                        }
                        index = new TrajIndex(inBuff, -1);
                    }

                    index.setDataPosition(nextActiveKeyPosition);
                    if (hasFileRemaining(fcIn, inBuff)) {
                        tailStart = index;
                        dataOffset += tailStart.getDataPosition() - headPosition;
                        nextActiveKeyPosition += TrajIndex.totalBytes;


                    } else {
                        // Last thing we saw in the file were deleted index entries
                        tc = new TrajController(tableName);
                        tc.truncateFile(index.getDataPosition());
                    }
                }
            }
            lock.release();
        } catch (IOException e) { e.printStackTrace(); }
    }

    private void refillIndexBuffer(FileChannel fc, ByteBuffer buff) throws IOException {
        if (buff.position() == 0) {
            buff.clear();
            fc.read(buff);
        } else {
            long unreadBytes = buff.limit() - buff.position() - 1;
            long newPosition = fc.position() - unreadBytes;
            fc.position(newPosition);
            buff.clear();
            fc.read(buff);
        }
        buff.flip();
    }*/
}
