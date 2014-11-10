package tdbms.QueryProcessor;

import tdbms.StorageManager.Controllers.TrajController;
import tdbms.StorageManager.Models.TrajSequence;

import java.text.ParseException;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InsertStatement extends BaseStatement {
    protected static final String regexStatement = "[\\s]*(?i)INSERT[\\s]+(?i)INTO[\\s]+(?<tableName>.+)" +
            "[\\s]+(?i)VALUES[\\s]+(?<parameters>[^;]+)[\\s]*;";
    private static final String regexParametersPattern = "([0-9:\\.\\-]+[\\s]*,{1}){6}[0-9:]{8}";
    private LinkedList<InsertParameters> parameters;
    private String tableName;


    public InsertStatement() throws TDBMSException, ParseException {
        super();
    }

    public InsertStatement(String statement) throws TDBMSException, ParseException {
        super(statement);
        parseStatement();
    }

    private void parseStatement() throws TDBMSException, ParseException {
        Pattern pattern = Pattern.compile(regexStatement, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(this.statement);
        if (matcher.find()) {
            this.tableName = matcher.group("tableName");
        } else {
            throw new TDBMSException("Could not parse table name from statement");
        }
        parseParameters();
    }

    private void parseParameters() throws TDBMSException, ParseException {
        Pattern pattern = Pattern.compile(regexParametersPattern);
        Matcher matcher = pattern.matcher(this.statement);

        this.parameters = new LinkedList<>();
        while (matcher.find()) {
            this.parameters.add(new InsertParameters(matcher.group()));
        }
    }

    @Override
    public long execute() throws TDBMSException {
        //You may assume that there are exactly 7 fields in each measure.
        // At the end of the operation, the system should either report a success
        // and return the trajectory id, or report a failure.

        long result = resultInvalid;
        TrajController tc = new TrajController(this.tableName);
        LinkedList<TrajSequence> sequences = new LinkedList<>();
        TrajSequence seq;
        for (InsertParameters paramSet : parameters) {
            sequences.add(
                    new TrajSequence(
                    paramSet.getLatitude(), paramSet.getLongitude(),
                    paramSet.getAltitude(), paramSet.getDayOffset(),
                    paramSet.getYear(), paramSet.getMonth(), paramSet.getDay(),
                    paramSet.getHour(), paramSet.getMinute(), paramSet.getSecond()
            ));
        }
        result = tc.insertSequence(sequences);

        if (result == resultInvalid) {
            throw new TDBMSException("Insert statement could not be executed");
        } else {
            System.out.printf("Successfully inserted new trajectory with Id %d\n", result);
            return result;
        }
    }
}
