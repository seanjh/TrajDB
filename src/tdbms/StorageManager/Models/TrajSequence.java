package tdbms.StorageManager.Models;

import java.nio.ByteBuffer;

public class TrajSequence {
    public static final int totalBytes = 26;
    private float latitude;
    private float longitude;
    private byte zero = 0;
    private int altitude;
    private float daysSince1889;
    private int year;
    private byte month;
    private byte day;
    private byte hour;
    private byte minute;
    private byte second;

    public TrajSequence(float lat, float lon, int alt, float days, int year, byte month,
                        byte day, byte hour, byte minute, byte second) {
        this.latitude = lat;
        this.longitude = lon;
        this.altitude = alt;
        this.daysSince1889 = days;
        this.year = year;
        this.month = month;
        this.day = day;
        this.hour = hour;
        this.minute = minute;
        this.second = second;
    }

    public TrajSequence(ByteBuffer buff) {
        this.latitude = buff.getFloat();
        this.longitude = buff.getFloat();
        this.zero = buff.get();
        this.altitude = buff.getInt();
        this.daysSince1889 = buff.getFloat();
        this.year = buff.getInt();
        this.month = buff.get();
        this.day = buff.get();
        this.hour = buff.get();
        this.minute = buff.get();
        this.second = buff.get();
    }

    public float getLatitude() { return this.latitude; }
    public float getLongitude() { return this.longitude; }
    public byte getZero() { return this.zero; }
    public int getAltitude() { return this.altitude; }
    public float getDaysSince1889() { return this.daysSince1889; }
    public int getYear() { return this.year; }
    public byte getMonth() { return this.month; }
    public byte getDay() { return this.day; }
    public byte getHour() { return this.hour; }
    public byte getMinute() { return this.minute; }
    public byte getSecond() { return this.second; }

    public ByteBuffer getBuffer() {
        ByteBuffer buff = ByteBuffer.allocate(totalBytes);
        buff.putFloat(this.latitude);
        buff.putFloat(this.longitude);
        buff.put(this.zero);
        buff.putInt(this.altitude);
        buff.putFloat(this.daysSince1889);
        buff.putInt(this.year);
        buff.put(month);
        buff.put(day);
        buff.put(hour);
        buff.put(minute);
        buff.put(second);
        buff.rewind();
        return buff;
    }

    @Override
    public String toString() {
        return (
            Float.toString(this.latitude) + "," +
            Float.toString(this.longitude) + "," +
            Byte.toString(this.zero) + "," +
            Integer.toString(this.altitude) + "," +
            Float.toString(this.daysSince1889) + "," +
            Integer.toString(this.year) + '-' +
            (this.month < 10 ? "0" : "") + Byte.toString(this.month) + '-' +
            (this.day < 10 ? "0" : "") + Byte.toString(this.day) + ',' +
            (this.hour < 10 ? "0" : "") + Byte.toString(this.hour) + ':' +
            (this.minute < 10 ? "0" : "") + Byte.toString(this.minute) + ':' +
            (this.second < 10 ? "0" : "") + Byte.toString(this.second)
        );
    }
}
