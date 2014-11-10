package tdbms.StorageManager.Controllers;

import tdbms.QueryProcessor.TDBMSException;
import tdbms.StorageManager.Models.TrajDataFile;
import tdbms.StorageManager.Models.TrajIndexFile;

import java.io.File;

public class DatabaseController {
    public static final byte invalidPosition = -1;
    protected static final int blockSize = 512;
    protected String tableName;
    protected File indexFile;
    protected File dataFile;

    public DatabaseController(String tname) throws TDBMSException {
        if (!TrajIndexFile.fileExists(tname) || !TrajDataFile.fileExists(tname)) {
            throw new TDBMSException("Table does not exist.");
        }

        setTableName(tname);
        this.indexFile = new File(TrajIndexFile.getFileName(tname));
        this.dataFile = new File(TrajDataFile.getFileName(tname));
    }

    public String getTableName() { return this.tableName; }
    private void setTableName(String tname) { this.tableName = tname.trim(); }
    public File getIndexFile() { return this.indexFile; }
    public File getDataFile() { return this.dataFile; }

    public void eraseDatabase() {
        if (this.indexFile.exists())
            this.indexFile.delete();
        if (this.dataFile.exists())
            this.dataFile.delete();
    }
}
