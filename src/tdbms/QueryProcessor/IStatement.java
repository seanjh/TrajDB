package tdbms.QueryProcessor;

/**
 * Created by sean on 11/2/14.
 */
public interface IStatement {
    public boolean isExit();
    public long execute() throws TDBMSException;
}
