package tdbms.QueryProcessor;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InsertParameters {
    private static final String regexPattern = "(?<latitude>[0-9\\.]+){1}" +
            "(?<sep>[\\s]*,[\\s]*){1}(?<longitude>[0-9\\.]+){1}\\k<sep>{1}" +
            "(?<zeroField>[0]){1}\\k<sep>{1}(?<altitude>-{0,1}[0-9]+){1}\\k<sep>{1}" +
            "(?<dateOffset>[0-9\\.]+){1}\\k<sep>{1}(?<year>[0-9]{4})-(?<month>[0-9]{2})-" +
            "(?<day>[0-9]{2})\\k<sep>(?<hour>[0-9]{2}):(?<minute>[0-9]{2}):(?<second>[0-9]{2})";
    private BigDecimal latitude;
    private BigDecimal longitude;
    private byte zeroField;
    private int altitude;
    private BigDecimal dateOffset;
    private LocalDate date;
    private LocalTime time;

    public InsertParameters(String params) throws TDBMSException {
        Pattern pattern = Pattern.compile(regexPattern);
        Matcher matcher = pattern.matcher(params);
        if (matcher.find()) {
            this.latitude = new BigDecimal(matcher.group("latitude"));
            this.longitude = new BigDecimal(matcher.group("longitude"));
            this.zeroField = Byte.parseByte(matcher.group("zeroField"));
            this.altitude = Integer.parseInt(matcher.group("altitude"));
            this.dateOffset = new BigDecimal(matcher.group("dateOffset"));
            this.date = LocalDate.of(
                    Integer.parseInt(matcher.group("year")),
                    Integer.parseInt(matcher.group("month")),
                    Integer.parseInt(matcher.group("day"))
            );
            this.time = LocalTime.of(
                    Integer.parseInt(matcher.group("hour")),
                    Integer.parseInt(matcher.group("minute")),
                    Integer.parseInt(matcher.group("second"))
            );
        } else { throw new TDBMSException("Could not parse parameters from statement"); }
    }

    public float getLatitude() { return this.latitude.floatValue(); }
    public float getLongitude() { return this.longitude.floatValue(); }
    public byte getZeroField() { return this.zeroField; }
    public int getAltitude() { return this.altitude; }
    public float getDayOffset() { return this.dateOffset.floatValue(); }
    public LocalDate getDate() { return this.date; }
    public int getYear() { return this.date.getYear(); }
    public byte getMonth() { return (byte) this.date.getMonthValue(); }
    public byte getDay() { return (byte) this.date.getDayOfMonth(); }
    public LocalTime getTime() { return this.time; }
    public byte getHour() { return (byte) this.time.getHour(); }
    public byte getMinute() { return (byte) this.time.getMinute(); }
    public byte getSecond() { return (byte) this.time.getSecond(); }

}
