package tdbms.StorageManager.Controllers;

import tdbms.QueryProcessor.TDBMSException;
import tdbms.StorageManager.Models.TrajIndex;
import tdbms.StorageManager.Models.TrajSequence;
import tdbms.StorageManager.Models.TrajSet;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.LinkedList;

public class TrajController extends DatabaseController {

    public TrajController(String tname) throws TDBMSException { super(tname); }

    public long insertSequence(LinkedList<TrajSequence> sequences) throws TDBMSException {
        long result = TrajIndex.invalidId;
        try (RandomAccessFile file = new RandomAccessFile(this.dataFile, "rw");
             FileChannel fc = file.getChannel()) {
            FileLock lock = fc.lock();

            // get new Id for this sequence
            IndexController ic = new IndexController(this.tableName);
            result = ic.getMaxId() + 1;

            // Add a index entry for the new sequence
            ic.addNewIndex(result, fc.size());

            // Append the new sequences to the file
            fc.position(fc.size());
            TrajSet set = new TrajSet(sequences);
            ByteBuffer buff = set.getBuffer();
            //System.out.printf("Adding new trajectory with %d sequences at position %d", sequences.size(), fc.position());
            fc.write(buff);

            lock.release();
        } catch (IOException e) { e.printStackTrace(); }
        return result;
    }

    private int getSetLength(long position, FileChannel fc) throws IOException {
        // Read the length of this sequence from disk
        ByteBuffer buff = ByteBuffer.allocate(Integer.BYTES);
        fc.position(position);
        fc.read(buff);
        buff.rewind();
        return buff.getInt();
    }

    private void deleteSequenceAtPosition(long position) throws TDBMSException {
        try (RandomAccessFile file = new RandomAccessFile(this.dataFile, "rw");
        FileChannel fc = file.getChannel()) {
            FileLock lock = fc.lock();

            int setLength = getSetLength(position, fc);

            //ByteBuffer buff = ByteBuffer.allocate(blockSize);
            ByteBuffer buff = ByteBuffer.allocate(TrajSequence.totalBytes * setLength + TrajSet.totalHeaderBytes);
            while (buff.hasRemaining()) {
                buff.put((byte) -1);
            }
            buff.flip();
            fc.position(position);
            fc.write(buff);

            lock.release();
        } catch (IOException e) { e.printStackTrace(); }
    }

    public long deleteSequenceById(long id) throws TDBMSException {
        long result = TrajIndex.invalidId;
        // Get position for this Id from the index
        IndexController ic = new IndexController(this.tableName);
        long position = ic.getDataPositionById(id);

        if (position != IndexController.invalidPosition) {
            // Delete the index entry for this Id
            ic.deleteIndex(id);

            // Delete data for this Id beginning at position
            deleteSequenceAtPosition(position);

            // Success
            result = id;
        }

        return result;
    }

    public TrajSet getTrajById(long id) throws TDBMSException {
        TrajSet set = null;

        // Get position for this Id from the index
        IndexController ic = new IndexController(this.tableName);
        long position = ic.getDataPositionById(id);

        // Get the trajectory sequences if the Id was located in the index
        if (position != IndexController.invalidPosition) {
            set = getTrajSetFromPosition(position);
        }

        return set;
    }

    public TrajSet getTrajSetFromPosition(long position) {
        TrajSet set = null;

        try (RandomAccessFile file = new RandomAccessFile(this.dataFile, "rw");
             FileChannel fc = file.getChannel()) {

            ByteBuffer buff = ByteBuffer.allocate(blockSize);
            fc.position(position);
            set = new TrajSet(buff, fc);

        } catch (IOException e) { e.printStackTrace(); }

        return set;
    }

    public void cleanDataFile() {
        //
    }

    public void truncateFile(long size) {
        // TODO: truncate data file
    }
}
