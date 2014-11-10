package tdbms.StorageManager.Models;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TrajIndexFile {
    public static final String extension = ".index";

    public static final long deletedCountPosition = 0;
    public static final long indexBodyPosition = deletedCountPosition + Long.BYTES;

    public static long getKeysCount(FileChannel fc) throws IOException {
        long bodySize = fc.size() - Long.BYTES;
        return bodySize / TrajIndex.totalBytes;
    }

    public static long getDeletedCount(FileChannel fc) throws IOException {
        ByteBuffer buff = ByteBuffer.allocate(Long.BYTES);
        fc.position(deletedCountPosition);
        fc.read(buff);
        buff.flip();
        return buff.getLong();
    }

    public static boolean fileExists(String tname) {
        Path path = Paths.get(getFileName(tname));
        return Files.exists(path) && Files.isRegularFile(path) && Files.isReadable(path);
    }

    public static String getFileName(String tname) {
        return tname.trim() + extension;
    }

    public static boolean initialize(String tname) throws IOException {
        return initialize(new File(getFileName(tname)));
    }

    public static boolean initialize(File file) throws IOException {
        FileOutputStream out = new FileOutputStream(file);
        FileChannel fc = out.getChannel();

        // Load index header with 0 values
        ByteBuffer buff = ByteBuffer.allocate((int) indexBodyPosition);
        while (buff.position() < buff.limit()) {
            buff.put((byte) 0);
        }
        buff.rewind();
        fc.write(buff);

        return fc.size() > 0;
    }

    public static void eraseFile(String tname) {
        if (fileExists(tname)){
            File file = new File(getFileName(tname));
            file.delete();
        }
    }

    public static boolean isEmpty(FileChannel fc) throws IOException {
        return fc.size() <= indexBodyPosition;
    }
}
