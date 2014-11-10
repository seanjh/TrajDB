package tdbms.ClientManager;

import tdbms.QueryProcessor.BaseStatement;
import tdbms.QueryProcessor.TDBMSException;

import java.io.Console;
import java.text.ParseException;
import java.util.NoSuchElementException;
import java.util.Scanner;

public class REPL {
    private static final String prompt = "tdbms>";
    //private static final String nestedPrompt = "...\t\t";
    private static final Character terminator = ';';

    public static BaseStatement getInput() {
        StringBuilder input = new StringBuilder();
        System.out.printf("%s ", prompt);
        Scanner in = new Scanner(System.in);
        String chunk = in.nextLine();
        input.append(chunk);
        while (!isStatementTerminated(chunk)) {
            //System.out.print(nestedPrompt);
            chunk = "\n" + in.nextLine();
            input.append(chunk);
        }

        BaseStatement statement = null;
        try {
            statement = BaseStatement.parseStatement(input.toString());
        } catch (TDBMSException e) {
            System.out.printf("ERROR - Unrecognized command: %s\n", input.toString());
            REPL.getInput();
        } catch (ParseException e) {
            System.out.printf("ERROR - Unable to parse input: %s\n", input.toString());
        }
        return statement;
    }

    private static String getAllLines(Scanner in) {
        StringBuilder lines = new StringBuilder();
        String oneLine;
        while (true) {
            try {
                oneLine = "\n" + in.nextLine();
                lines.append(oneLine);
            } catch (NoSuchElementException e) {
                break;
            }
        }
        return lines.toString();
    }

    public static void printResult(String result) {
        System.out.println(result);
    }

    public static boolean isStatementTerminated(String statement) {
        for (int i = statement.length() - 1; i >= 0; i--) {
            if (statement.charAt(i) == terminator) {
                return true;
            } else if (!Character.isWhitespace(statement.charAt(i))) {
                break;
            }
        }
        return false;
    }

}
