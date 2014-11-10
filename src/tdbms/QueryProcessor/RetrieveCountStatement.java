package tdbms.QueryProcessor;

import tdbms.StorageManager.Controllers.TrajController;
import tdbms.StorageManager.Models.TrajSet;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RetrieveCountStatement extends BaseStatement {
    protected static final String regexStatement = "[\\s]*(?i)RETRIEVE[\\s]+(?i)FROM[\\s]+(?<tableName>.+)" +
            "[\\s]+(?i)COUNT[\\s]+(?i)OF[\\s]+(?<queryID>[^;]+)[\\s]*;";
    private int queryID;
    private String tableName;

    protected RetrieveCountStatement() throws TDBMSException {
        this("");
    }

    protected RetrieveCountStatement(String statement) throws TDBMSException {
        super(statement);
        parseStatement();
    }

    private void parseStatement() throws TDBMSException {
        Pattern pattern = Pattern.compile(regexStatement, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(this.statement);
        if (matcher.find()) {
            this.tableName = matcher.group("tableName");
            this.queryID = Integer.parseInt(matcher.group("queryID"));
        } else {
            throw new TDBMSException("Could not parse table name from statement");
        }
    }

    @Override
    public long execute() throws TDBMSException {
        long result = resultInvalid;
        TrajController tc = new TrajController(tableName);
        TrajSet set = tc.getTrajById(this.queryID);
        if (set != null && set.getSequenceCount() > 0) {
            result = set.getSequenceCount();
            System.out.printf("Size of trajectory set with Id %d: %d\n", this.queryID, result);
        } else {
            System.out.printf("No trajectory exists in \"%s\" with Id %d\n", this.tableName, this.queryID);
        }
        return result;
    }
}
