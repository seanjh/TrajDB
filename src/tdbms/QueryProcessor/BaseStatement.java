package tdbms.QueryProcessor;

import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// INSERT INTO <tname> VALUES <sequence>;
// DELETE FROM <tname> TRAJECTORY <id>;
// RETRIEVE FROM <tname> TRAJECTORY <id>;
// EXIT

public class BaseStatement implements IStatement {

    private static enum StatementType {
        INSERT, DELETE, RETRIEVE,
        RETREIVE_COUNT, EXIT, CREATE, INVALID
    }

    protected String statement;
    protected static final long resultInvalid = -1;
    protected static final long resultValid = 1;

    public static BaseStatement parseStatement(String statement) throws TDBMSException, ParseException {
        BaseStatement stmt = null;
        switch (BaseStatement.getType(statement)) {
            case INSERT:
                stmt = new InsertStatement(statement);
                break;
            case DELETE:
                stmt = new DeleteStatement(statement);
                break;
            case RETRIEVE:
                stmt = new RetrieveStatement(statement);
                break;
            case RETREIVE_COUNT:
                stmt = new RetrieveCountStatement(statement);
                break;
            case CREATE:
                stmt = new CreateStatement(statement);
                break;
            case EXIT:
                stmt = new ExitStatement(statement);
                break;
            case INVALID:
                stmt = new BaseStatement();
                break;
            default:
                throw new TDBMSException("Unrecognized statement");
        }
        return stmt;
    }

    protected BaseStatement() throws TDBMSException {
        this("");
    }

    protected BaseStatement(String statement) throws TDBMSException {
        this.statement = statement.trim();
    }

    private static StatementType getType(String statement) throws TDBMSException {
        StatementType type;
        if (statement.toLowerCase().startsWith("exit;")) {
            type = StatementType.EXIT;
        } else if (InsertStatement.matchesPattern(statement, InsertStatement.regexStatement)) {
            type = StatementType.INSERT;
        } else if (DeleteStatement.matchesPattern(statement, DeleteStatement.regexStatement)) {
            type = StatementType.DELETE;
        } else if (RetrieveStatement.matchesPattern(statement, RetrieveStatement.regexStatement)) {
            type = StatementType.RETRIEVE;
        } else if (RetrieveCountStatement.matchesPattern(statement, RetrieveCountStatement.regexStatement)) {
            type = StatementType.RETREIVE_COUNT;
        } else if (CreateStatement.matchesPattern(statement, CreateStatement.regexStatement)) {
            type = StatementType.CREATE;
        } else {
            type = StatementType.INVALID;
        }
        return type;
    }

    public static boolean matchesPattern(String statement, String regex) {
        Pattern pattern = Pattern.compile(regex);
        Matcher match = pattern.matcher(statement);
        return match.matches();
    }

    public boolean isExit() {
        return false;
    }

    public long execute() throws TDBMSException {
        throw new TDBMSException("Unrecognized statement");
    }
}
