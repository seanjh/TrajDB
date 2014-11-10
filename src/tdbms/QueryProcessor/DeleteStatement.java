package tdbms.QueryProcessor;

import tdbms.StorageManager.Controllers.TrajController;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DeleteStatement extends BaseStatement {
    protected static final String regexStatement = "[\\s]*(?i)DELETE[\\s]+(?i)FROM[\\s]+(?<tableName>.+)" +
            "[\\s]+(?i)TRAJECTORY[\\s]+(?<queryID>[^;]+)[\\s]*;";
    private int queryID;
    private String tableName;

    protected DeleteStatement() throws TDBMSException {
    }

    protected DeleteStatement(String statement) throws TDBMSException {
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
        TrajController tc = new TrajController(this.tableName);
        long result = tc.deleteSequenceById(this.queryID);

        if (result != resultInvalid) {
            System.out.printf("Successfully deleted trajectory Id %d\n", this.queryID);
        } else {
            System.out.printf("Failed to delete trajectory Id %d\n", this.queryID);
        }
        return result;
    }
}
