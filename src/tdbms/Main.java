package tdbms;

import tdbms.ClientManager.REPL;
import tdbms.QueryProcessor.BaseStatement;
import tdbms.QueryProcessor.TDBMSException;

public class Main {

    public static void main(String[] args) {
        BaseStatement statement = REPL.getInput();
        while (!statement.isExit()) {
            try {
                statement.execute();
            } catch (TDBMSException e) { System.out.println(e.toString()); }
            statement = REPL.getInput();
        }

        System.out.printf("\nExiting TDBMS...\n");
    }
}
