# Databases Assignment 3
## CS5320 F14
## Sean Herman

### Requirements
This application was developed for Java language level 8.0, using the Java 1.8 SDK.

### Instructions
Enter the TrajDB command line using the following command.

    $ java tdbms.Main
   
This command will display the TrajDB command line.

    tdbms> 
   
All TrajDB commands must be terminated by a semicolon. The following commands are supported in TrajDB:

* CREATE *tname*;
* INSERT INTO *tname* VALUES *sequence*;
* DELETE FROM *tname* TRAJECTORY *id*;
* RETRIEVE FROM *tname* TRAJECTORY *id*;
* RETRIEVE FROM *tname* COUNT OF *id*;
* EXIT;

A *sequence* is a series of exactly 7 trajectory measurements. Inserts should consist of a series of 1 or more sequences, with the whole set terminated by a semicolon. 

* Latitude in decimal degrees
* Longitude in decimal degrees
* A field that is always equal to 0
* Altitude in feet (-777 if not valid)
* Date - number of days (with fractional part) that have passed since 12/30/1899
* Date as a string
* Time as a string

### Implementation description
When a *table* is created in TrajDB, 2 files are created on disk: *table.index* and *table.trajdb*. These (naturally) hold the index and the data records respectively.  All data is stored on the disk as raw bytes. Accessing some particular trajectory in the data files requires a sequential scan of the index file, which includes references to positions (as # of bytes) within the data file.
 
#### TrajDB internals
 
The *table.index* file includes a short file header and a series of 0 or more index entries.

* Header
    * Deleted key count - 8 byte integer (long)
* Index Entries
    * Index Key - 8 byte integer (long)
    * Data File Position (in bytes) - 8 byte integer (long)

The *table.trajdb* file includes a series of 0 or more trajectory sets. A trajectory set consists of a header value, which reports the length of the trajectory set (i.e., count of trajectory sequences), and the actual trajectory sequences.

* Trajectory set
    * Length - 4 byte integer (int)
    * Trajectory sequence - 26 bytes (1 or more in series)
        * Latitude - 4 byte decimal (float)
        * Longitude - 4 byte decimal (float)
        * 0 value - 1 byte integer (byte)
        * Altitude in feet - 4 byte integer (int)
        * Day offset from 1899 - 4 byte decimal (float)
        * Date year - 4 byte integer (int)
        * Date month - 1 byte integer (byte)
        * Date day - 1 byte integer (byte)
        * Time hour - 1 byte integer (byte)
        * Time minute - 1 byte integer (byte)
        * Time second - 1 byte integer (byte)

#### Reads & writes

My TrajDB implementation uses FileChannels and ByteBuffers to read and write data in a table. These classes allow for efficient random access within the database files, and even provide locking functionality to maintain database integrity (admittedly, this functionality is lacking polish in places). 

Data is read from the disk in blocks typically of 512 bytes (reflecting a typical block-size on disk). When more data is required from the disk beyond this 512 bytes in the first block, 2 different approaches are used. TrajDB will either read a smaller portion of bytes from the disk, and reshuffle the ByteBuffer to rotate the remaining, unread bytes to the front and the new bytes to the end of the buffer. Under this approach, TrajDB is often reading less than 512 bytes at a time (e.g., 498 bytes).

Alternatively, I also tried an approach where the unread portion of a buffer would simply be re-read from the disk. This meant that 512 bytes were always read from the disk each time, with some overlap at each disk access. More testing is required to determine which method is most efficient, though my inclination is that the former would be best (though it was more difficult to implement in Java).  

#### Deletes
Deleted data is simply marked as deleted in the index and data files. This deleted data is represented as -1 or hex F in the files. It's obvious that this deleted data should be "cleaned" from the index and trajdb files somehow. Some existing design designs make this difficult. For example, I am not currently including a copy of the index key / trajectory ID in the data file. This was definitely a mistake in retrospect, as it makes it impossible to make any use of the datafile without also referencing the index file (e.g., for cleaning deleted data). It also would have been more sensible to somehow preserve the length value in deleted trajectory sets, to make it easier to identify the length of a deleted sequence when working with the data file.