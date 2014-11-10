package tdbms.StorageManager.Models;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TrajDataFile {
    public static final String extension = ".trajdb";

    public static boolean fileExists(String tname) {
        Path path = Paths.get(getFileName(tname));
        return Files.exists(path) && Files.isRegularFile(path) && Files.isReadable(path);
    }

    public static String getFileName(String tname) {
        return tname.trim() + extension;
    }

    public static boolean initialize(String tname) throws IOException {
        File file = new File(getFileName(tname));
        file.createNewFile();
        return initialize(file);
    }

    public static boolean initialize(File file) throws IOException {
        return file.canRead();
    }

    public static void eraseFile(String tname) {
        if (fileExists(tname)){
            File file = new File(getFileName(tname));
            file.delete();
        }
    }
}
