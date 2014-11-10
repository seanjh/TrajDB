package tdbms.QueryProcessor;

import tdbms.StorageManager.Controllers.DatabaseController;
import tdbms.StorageManager.Controllers.IndexController;
import tdbms.StorageManager.Controllers.TrajController;
import tdbms.StorageManager.Models.TrajDataFile;
import tdbms.StorageManager.Models.TrajIndex;
import tdbms.StorageManager.Models.TrajIndexFile;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CreateStatement extends BaseStatement {
    protected static final String regexStatement = "[\\s]*(?i)CREATE[\\s]+(?<tableName>[A-z]+[\\w]*)[\\s]*;";
    private String tableName;

    protected CreateStatement() throws TDBMSException {
        this("");
    }

    protected CreateStatement(String statement) throws TDBMSException {
        super(statement);
        parseStatement();

    }

    private void parseStatement() throws TDBMSException {
        Pattern pattern = Pattern.compile(regexStatement, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(this.statement);
        if (matcher.find()) {
            this.tableName = matcher.group("tableName");
        } else {
            throw new TDBMSException("Could not parse table name from statement");
        }
    }

    @Override
    public long execute() throws TDBMSException {
        boolean indexResult = false;
        boolean dataResult = false;
        try {
            DatabaseController controller = new DatabaseController(this.tableName);
            // Throw an error if controller does not.
            // When controller is successfully initialized, then the tables exist already
            throw new IOException("The table \"" + tableName + "\" already exists." );
        } catch (TDBMSException e1) {
            try {
                indexResult = TrajIndexFile.initialize(this.tableName);
                dataResult = TrajDataFile.initialize(this.tableName);
            } catch (IOException e2) { throw new TDBMSException("Failed to create database files.\n\t" + e2.toString()); }
        } catch (IOException e3) {
            throw new TDBMSException(e3.getMessage());
        }
        if (indexResult && dataResult) {
            System.out.println("Successfully created \"" + tableName + "\".");
            return resultValid;
        } else {
            TrajIndexFile.eraseFile(tableName);
            TrajDataFile.eraseFile(tableName);
            throw new TDBMSException("Failed to create database files.");
        }

    }
}
