package tdbms.QueryProcessor;

public class ExitStatement extends BaseStatement {
    protected ExitStatement() throws TDBMSException {
        super();
    }

    protected ExitStatement(String statement) throws TDBMSException {
        super(statement);
    }

    @Override
    public boolean isExit() {
        return true;
    }
}
