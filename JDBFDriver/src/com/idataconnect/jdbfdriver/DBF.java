/*
 * Copyright (c) 2009-2011, i Data Connect!
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of i Data Connect! nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDER AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.idataconnect.jdbfdriver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <p>The main class for DBF interaction.</p>
 * <p>Example usage:<br/>
<code>
DBF dbf = DBF.use("test.dbf");<br/>
dbf.appendBlank();<br/>
dbf.replace("TEST", "Testing");<br/>
dbf.close();<br/>
</code>
 * </p>
 * @author ben
 */
public class DBF {

    /** The current directory. */
    private static String currentDirectory = System.getProperty("user.home");
    /** Whether auto trim is enabled. */
    private static boolean autoTrimEnabled = true;
    /** The DBT block size, default <em>8</em>. */
    private static int dbtBlockSize = 8;
    /** Whether file locking is enabled. Default <em>false</em>. */
    private static boolean fileLockingEnabled = false;
    /** Whether synchronous writes are enabled. Default <em>false</em>. */
    private static boolean synchronousWritesEnabled = false;
    /** Whether thread safety in the library is enabled. Default <em>false</em>. */
    private static boolean threadSafetyEnabled = false;

    // buf needs to be at least 512 bytes. A larger buffer can result in greater
    // performance, but keep in mind that this buffer is never freed until
    // the JVM exits.
    /** The direct byte buffer for I/O. */
    private static final ByteBuffer buf = ByteBuffer.allocateDirect(8192)
            .order(ByteOrder.LITTLE_ENDIAN);
    private static final ReentrantLock threadLock = new ReentrantLock();
    /** The file object for I/O. */
    private final File dbfFile;
    /** The random access file for I/O. */
    private final RandomAccessFile randomAccessFile;
    /** The associated DBF structure. */
    private final DBFStructure structure;
    /** The current record number. */
    private int recordNumber;
    /** The values in the current record. */
    private DBFValue[] values;
    /** Whether the current record is deleted. */
    private boolean currentRecordDeleted;


    /**
     * Creates a new instance of a DBF file. This method is protected in order
     * to prevent public instantiation.
     * @param dbfFile The file where the DBF is stored.
     * @param file The random access file, used for I/O operations.
     * @param structure The structure of the file.
     */
    protected DBF(File dbfFile, RandomAccessFile file, DBFStructure structure) {
        this.dbfFile = dbfFile;
        this.randomAccessFile = file;
        this.structure = structure;
    }

    /**
     * Opens the DBF with the specified file.
     * @param dbfFile The file to open.
     * @return A DBF instance, if the file is successfully opened.
     * @throws FileNotFoundException If the DBF file could not be found.
     * @throws IOException If an I/O error occurs.
     */
    public static DBF use(File dbfFile) throws FileNotFoundException, IOException {
        RandomAccessFile file = new RandomAccessFile(dbfFile, "rw" + (synchronousWritesEnabled ? "s" : ""));
        DBF dbf = new DBF(dbfFile, file, new DBFStructure());
        dbf.readStructure();
        dbf.gotoRecord(1);
        return dbf;
    }

    /**
     * Opens the DBF with the given relative path. The current directory will
     * be used as the base directory for the file.
     * @param relativeDbfPath The path, relative to the current directory.
     * @return A DBF instance, if the file is successfully opened.
     * @throws FileNotFoundException If the DBF file could not be found.
     * @throws IOException IF an I/O error occurs.
     */
    public static DBF use(String relativeDbfPath) throws FileNotFoundException, IOException {
        if (!relativeDbfPath.toLowerCase().endsWith(".dbf"))
            relativeDbfPath += ".dbf";
        return use(new File(currentDirectory + File.separatorChar + relativeDbfPath));
    }

    /**
     * Closes the current file and releases resources taken by the connection to
     * this DBF file.
     * @throws IOException If an I/O error occurs.
     */
    public void close() throws IOException {
        randomAccessFile.close();
    }

    /**
     * Reads and parses the structure of the DBF and loads its values into memory.
     * @throws IOException If an I/O error occurs.
     */
    protected void readStructure() throws IOException {
        try {
            if (isThreadSafetyEnabled()) {
                threadLock.lock();
            }
            buf.clear();
            FileChannel channel = randomAccessFile.getChannel();
            channel.position(0);
            FileLock lock = null;
            if (isFileLockingEnabled())
                lock = channel.lock(0, 32, true);
            try {
                while (buf.position() < 32 && channel.read(buf) != -1) {}
            } finally {
                if (lock != null)
                    lock.release();
            }
            buf.flip();
            if (buf.remaining() < 33)
                throw new IOException("File too small to be a valid DBF");

            // Basic structure
            byte signature = buf.get();

            // Check creator version
            int version = signature & 7; // 0b00000111
            if (version != 3) // 0b00000011
                System.err.println("Warning: DBF has an unsupported signature (version ID " + version + ")");

            // Check DBT flag
            boolean dbtPaired = (signature & 128) == 128; // 0b10000000
            structure.setDbtPaired(dbtPaired);

            // Check memo flag
            boolean memoExists = (signature & 8) == 8; // 0b00001000
            structure.setMemoExists(memoExists);

            int lastUpdateYear = (buf.get() & 0xff) + 1900;
            int lastUpdateMonth = buf.get() & 0xff;
            int lastUpdateDay = buf.get() & 0xff;
            structure.setLastUpdated(new DBFDate(lastUpdateMonth, lastUpdateDay, lastUpdateYear));
            structure.setNumberOfRecords(buf.getInt()); // Unsigned int, that won't overflow
            structure.setHeaderLength((short)(buf.getShort() & 0xffff)); // Unsigned short
            structure.setRecordLength((short)(buf.getShort() & 0xffff)); // Unsigned short
            if (structure.getRecordLength() == 0)
                throw new IOException("Record length is zero");
            buf.position(buf.position() + 2); // Skip reserved
            structure.setTransactionActive(buf.get() != 0x00);
            structure.setDataEncrypted(buf.get() != 0x00);
            buf.position(buf.position() + 12); // Skip reserved
            structure.setMdxPaired(buf.get() != 0x00);
            buf.position(buf.position() + 3); // Skip code page byte and 2 reserved bytes

            structure.getFields().clear();
            // Header structure
            do {
                byte[] headerBytes = new byte[32];
                for (int count = 0; count < 32; count++) {
                    if (!buf.hasRemaining()) {
                        buf.clear();

                        // Read at least one byte
                        int numRead;
                        while ((numRead = channel.read(buf)) == 0) {}
                        if (numRead == -1) // EOF
                            throw new IOException("End of file encountered while reading header structure");

                        buf.flip();
                    }

                    headerBytes[count] = buf.get();
                }
                // Field name
                DBFField field = new DBFField();
                String fieldName = new String(headerBytes, 0, 11);
                for (int count = 0; count < 11; count++) {
                    if (fieldName.charAt(count) == '\u0000') // null terminator
                    {
                        fieldName = fieldName.substring(0, count);
                        break;
                    }
                }
                fieldName = fieldName.trim();
                if (fieldName.length() == 0)
                    throw new IOException("Blank field name encountered");
                field.setFieldName(fieldName);

                // Field type
                DBFField.FieldType fieldType;
                try {
                    fieldType = DBFField.FieldType.valueOf(String.valueOf((char) headerBytes[11]));
                } catch (IllegalArgumentException ex) {
                    fieldType = DBFField.FieldType.U;
                }
                field.setFieldType(fieldType);
                if (fieldType.equals(DBFField.FieldType.C)) {
                    // Support the field length extension by using the decimal
                    // byte as the most significant byte of a 16 bit unsigned
                    field.setFieldLength((headerBytes[17] << 8)
                            | headerBytes[16]);
                    field.setDecimalLength(0);
                } else if (fieldType.equals(DBFField.FieldType.D)) {
                    field.setFieldLength(8);
                } else {
                    field.setFieldLength(headerBytes[16]);
                    field.setDecimalLength(headerBytes[17]);
                }
                structure.getFields().add(field);

                // Make sure there is at least another byte in the buffer
                if (!buf.hasRemaining()) {
                    buf.clear();

                    // Read at least one byte
                    int numRead;
                    while ((numRead = channel.read(buf)) == 0) {
                    }
                    if (numRead == -1) // EOF
                        throw new IOException("End of file encountered while reading header structure");

                    buf.flip();
                }
            } while (buf.get(buf.position()) != 0x0d); // Keep reading header structure until terminator is encountered
        } finally {
            if (isThreadSafetyEnabled()) {
                threadLock.unlock();
            }
        }
    }

    /**
     * Writes the structure of the DBF file to disk.
     * @throws IOException If an I/O error occurs.
     */
    protected void writeStructure() throws IOException {
        try {
            if (isThreadSafetyEnabled()) {
                threadLock.lock();
            }
            FileChannel channel = randomAccessFile.getChannel();

            if (structure.getHeaderLength() == 0 || structure.getRecordLength() == 0) {
                // New file
                calculateLengths();
                calculateFlags();
                buf.clear();
                buf.put((byte) 0x0d); // end of header
                buf.put((byte) 0x1a); // end of file
                channel.position(structure.getHeaderLength() - 1);
                buf.flip();
                while (buf.hasRemaining())
                    channel.write(buf);
            }

            if (structure.getLastUpdated() == null) {
                GregorianCalendar cal = new GregorianCalendar();
                DBFDate date = new DBFDate(
                        cal.get(Calendar.MONTH) + 1,
                        cal.get(Calendar.DAY_OF_MONTH),
                        cal.get(Calendar.YEAR));
                structure.setLastUpdated(date);
            }

            channel.position(0);

            FileLock lock = null;
            if (isFileLockingEnabled())
                lock = channel.lock(0, 32, false);

            try {
                buf.clear();

                byte signature = 3;
                if (structure.isDbtPaired())
                    signature = (byte) (signature | 128);
                if (structure.isMemoExists())
                    signature = (byte) (signature | 8);
                buf.put(signature);

                buf.put((byte) (structure.getLastUpdated().year - 1900));
                buf.put(structure.getLastUpdated().month);
                buf.put(structure.getLastUpdated().day);

                buf.putInt(structure.getNumberOfRecords());

                assert(structure.getHeaderLength() > 0);
                buf.putShort(structure.getHeaderLength());

                assert(structure.getRecordLength() > 0);
                buf.putShort(structure.getRecordLength());

                // Skip over reserved
                buf.position(buf.position() + 2);

                // Transaction active flag
                buf.put((byte) (structure.isTransactionActive() ? 1 : 0));

                // Encryption flag
                buf.put((byte) (structure.isDataEncrypted() ? 1 : 0));

                // Skip over free record thread and multi-user data
                buf.position(buf.position() + 12);

                // MDX paired flag
                buf.put((byte) (structure.isMdxPaired() ? 1 : 0));

                // Code page byte
                buf.put((byte) 0);

                // Skip over reserved bytes
                buf.position(buf.position() + 2);

                buf.flip();
                while (buf.hasRemaining()) {
                    channel.write(buf);
                }

                assert(channel.position() == 32);

                // Field structure
                for (DBFField field : structure.getFields()) {
                    buf.clear();

                    // Field name
                    byte[] fieldNameBytes = field.getFieldName().getBytes();
                    buf.put(fieldNameBytes);
                    // Pad rest of name bytes with nulls
                    for (int count = 0; count < (11 - fieldNameBytes.length); count++)
                        buf.put((byte) 0);

                    assert(buf.position() == 11);

                    // Field type
                    buf.put((byte) field.getFieldType().name().charAt(0));

                    // Skip memory address
                    buf.position(buf.position() + 4);

                    // Field length and decimal count
                    if (field.getFieldType().equals(DBFField.FieldType.C)) {
                        buf.put((byte) (field.getFieldLength() & 0xff));
                        buf.put((byte) ((field.getFieldLength() & 0xff) >>> 8));
                    } else {
                        buf.put((byte) field.getFieldLength());
                        buf.put((byte) field.getDecimalLength());
                    }

                    assert(buf.position() == 18);

                    // Skip multi user bytes
                    buf.position(buf.position() + 2);

                    // Work area ID
                    buf.put((byte) 1);

                    // Skip multi user bytes
                    buf.position(buf.position() + 2);

                    // Flag for set fields
                    buf.put((byte) 0);

                    assert(buf.position() == 24);

                    // Skip over reserved
                    buf.position(buf.position() + 7);

                    // Index field flag
                    buf.put((byte) 0);

                    assert(buf.position() == 32);

                    buf.flip();
                    while (buf.hasRemaining())
                        channel.write(buf);
                }
            } finally {
                if (isFileLockingEnabled())
                    lock.release();
            }
        } finally {
            if (isThreadSafetyEnabled()) {
                threadLock.unlock();
            }
        }
    }

    /**
     * Gets the current directory which will be used for all relative
     * path file functions.
     * @return The current directory.
     */
    public static String getCurrentDirectory() {
        return currentDirectory;
    }

    /**
     * Sets the current directory which will be used for all relative path
     * file functions.
     * @param currentDirectory The current directory to set.
     */
    public static void setCurrentDirectory(String currentDirectory) {
        DBF.currentDirectory = currentDirectory;
    }

    /**
     * Gets whether auto trim of character fields is enabled.
     * @return Whether auto trim of character fields is enabled.
     */
    public static boolean isAutoTrimEnabled() {
        return autoTrimEnabled;
    }

    /**
     * Sets whether to auto trim character fields.
     * @param autoTrimEnabled Whether to auto trim character fields.
     */
    public static void setAutoTrimEnabled(boolean autoTrimEnabled) {
        DBF.autoTrimEnabled = autoTrimEnabled;
    }

    /**
     * Gets the block size for newly created DBT (memo) files. The block size
     * is multiplied by 64 to obtain the number of bytes. The default block
     * size is 8, and therefore 512 bytes.
     * @return The block size for newly created DBT files.
     */
    public static int getDbtBlockSize() {
        return dbtBlockSize;
    }

    /**
     * Sets the block size for newly created DBT (memo) files. The block size
     * is multiplied by 64 to obtain the number of bytes. The default block
     * size is 8, and therefore 512 bytes.
     * @param dbtBlockSize The block size for newly created DBT files.
     */
    public static void setDbtBlockSize(int dbtBlockSize) {
        DBF.dbtBlockSize = dbtBlockSize;
    }

    /**
     * Gets whether file locking is enabled. Note that more than one DBF instance
     * running within the same JVM should not attempt to access the same tables,
     * even if file locking is active.
     * @return Whether file locking is currently active.
     */
    public static boolean isFileLockingEnabled() {
        return fileLockingEnabled;
    }

    /**
     * Sets whether file locking is enabled. Note that more than one DBF instance
     * running within the same JVM should not attempt to access the same tables,
     * even if file locking is active.
     * @param fileLockingEnabled Whether to activate or deactivate file locking.
     */
    public static void setFileLockingEnabled(boolean fileLockingEnabled) {
        DBF.fileLockingEnabled = fileLockingEnabled;
    }

    /**
     * Gets whether synchronous writes are enabled. Synchronous writes may help
     * prevent data loss upon power failure at the expense of reduced I/O
     * performance.
     * @return Whether synchronous writes are enabled.
     */
    public static boolean isSynchronousWritesEnabled() {
        return synchronousWritesEnabled;
    }

    /**
     * Sets whether synchronous writes are enabled. Synchronous writes may
     * help prevent data loss upon power failure at the expense of reduced I/O
     * performance.
     * @param synchronousWritesEnabled Whether syncronous writes are to be
     * enabled.
     */
    public static void setSynchronousWritesEnabled(boolean synchronousWritesEnabled) {
        DBF.synchronousWritesEnabled = synchronousWritesEnabled;
    }

    /**
     * Gets whether thread safety is enabled in the library. If this library
     * is to be called from multiple threads, thread safety should be enabled.
     * Failing to enable thread safety when necessary will cause crashes, data
     * loss and corruption. When in doubt, enable thread safety. Thread safety
     * is disabled by default for performance reasons.
     * @return whether thread safety is enabled.
     */
    public static boolean isThreadSafetyEnabled() {
        return threadSafetyEnabled;
    }

    /**
     * Sets whether thread safety is enabled in the library. If this library
     * is to be called from multiple threads, thread safety should be enabled.
     * Failing to enable thread safety when necessary will cause crashes, data
     * loss and corruption. When in doubt, enable thread safety. Thread safety
     * is disabled by default for performance reasons.
     * @param threadSafetyEnabled whether to enable thread safety.
     */
    public static void setThreadSafetyEnabled(boolean threadSafetyEnabled) {
        DBF.threadSafetyEnabled = threadSafetyEnabled;
    }

    /**
     * Gets the structure of the current DBF.
     * @return The structure.
     */
    public DBFStructure getStructure() {
        return structure;
    }

    /**
     * Checks if the record pointer is at the beginning of the file.
     * @return Whether the record pointer is at the beginning of the file.
     */
    public boolean bof() {
        return recordNumber == 0;
    }

    /**
     * Checks if the record pointer is at the end of the file.
     * @return Whether the record pointer is at the end of the file.
     */
    public boolean eof() {
        return recordNumber == -1;
    }

    /**
     * Checks if the current record is deleted.
     * @return whether the current record is deleted.
     */
    public boolean deleted() {
        return currentRecordDeleted;
    }

    /**
     * Moves to the specified record number.
     * @param recordNumber The record number to go to.
     * @return The record number after moving.
     * @throws IOException If an I/O error occurs.
     */
    public int gotoRecord(int recordNumber) throws IOException {
        // Re-read the header to obtain the number of records, in case another
        // process changed it.
        readStructure();

        currentRecordDeleted = false;

        if (recordNumber <= 0 || getStructure().getNumberOfRecords() == 0)
            this.recordNumber = 0;
        else if (recordNumber > getStructure().getNumberOfRecords())
            this.recordNumber = -1;
        else
            this.recordNumber = recordNumber;

        readRecord(recordNumber);

        return this.recordNumber;
    }

    /**
     * Skips forward 1 record.
     * @return The new record number.
     * @throws IOException If an I/O error occurs.
     */
    public int skip() throws IOException {
        return skip(1);
    }

    /**
     * Skips forwards or backwards by the number of records given. If
     * <tt>skipCount</tt> is a negative number, the record will be moved backwards.
     * @param skipCount The number of records to move.
     * @return The new record number.
     * @throws IOException If an I/O error occurs.
     */
    public int skip(int skipCount) throws IOException {
        return gotoRecord(recno() + skipCount);
    }

    /**
     * Reads the record specified by <tt>recordNumber</tt> into memory so its
     * values may be pulled. Record numbers start with 1.
     * @param recordNumber The number of the record.
     * @throws IOException If an I/O error occurs.
     */
    protected void readRecord(int recordNumber) throws IOException {
        if (values == null || values.length != structure.getFields().size())
            values = new DBFValue[structure.getFields().size()];

        if ((bof() && structure.getNumberOfRecords() == 0) || eof()) {
            // Set default values
            Iterator<DBFField> i = structure.getFields().iterator();
            for (int count = 0; count < structure.getFields().size(); count++) {
                DBFField field = i.next();
                values[count] = field.getDefaultValue();
            }
        } else {
            try {
                if (isThreadSafetyEnabled()) {
                    threadLock.lock();
                }
                if (bof())
                    recordNumber = 1;
                FileChannel channel = randomAccessFile.getChannel();
                channel.position(structure.getHeaderLength() + (recordNumber - 1) * structure.getRecordLength());
                FileLock lock = null;
                if (isFileLockingEnabled())
                    lock = channel.lock(structure.getHeaderLength() + (recordNumber - 1) * structure.getRecordLength(), structure.getRecordLength(), true);
                ByteBuffer recordBuffer;
                try {
                    buf.clear();
                    if (structure.getRecordLength() <= buf.remaining()) {
                        // The record will fit in the buffer
                        while (buf.position() < structure.getRecordLength()) {
                            if (channel.read(buf) == -1)
                                throw new IOException("End of file encountered while reading record");
                        }

                        recordBuffer = buf;
                    } else {
                        // The record won't fit in the buffer, so we need to allocate
                        // a temporary buffer for the record.
                        recordBuffer = ByteBuffer.allocate(structure.getRecordLength());

                        while (recordBuffer.position() < structure.getRecordLength()) {
                            buf.clear();
                            if (buf.remaining() > recordBuffer.remaining())
                                buf.limit(recordBuffer.remaining());
                            switch (channel.read(buf)) {
                                case -1: // EOF
                                    throw new IOException("End of file encountered while reading record");
                                case 0:
                                    break;
                                default:
                                    buf.flip();
                                    recordBuffer.put(buf);
                            }
                        }
                    }
                } finally {
                    if (lock != null)
                        lock.release();
                }

                recordBuffer.flip();
                currentRecordDeleted = recordBuffer.get() == '*';
                Iterator<DBFField> i = structure.getFields().iterator();
                for (int count = 0; count < structure.getFields().size(); count++) {
                    DBFField currentField = i.next();
                    byte[] fieldData = new byte[currentField.getFieldLength()];
                    recordBuffer.get(fieldData);
                    switch (currentField.getFieldType()) {
                        case C:
                            if (isAutoTrimEnabled())
                                values[count] = new DBFValue(currentField, new String(fieldData).trim());
                            else
                                values[count] = new DBFValue(currentField, new String(fieldData));
                            break;
                        case M:
                        case B:
                        case G:
                            String dataString = new String(fieldData).trim();
                            if (dataString.length() == 0)
                                values[count] = new DBFValue(currentField, "");
                            else {
                                // buf is busy at this point, so allocate another
                                // one that is large enough to read the header
                                // and possibly the field's value (if large enough).
                                // headerBuf must be at least 22 bytes.
                                ByteBuffer headerBuf = ByteBuffer.allocate(512).order(ByteOrder.LITTLE_ENDIAN);
                                int blockNumber = Integer.parseInt(dataString);
                                File dbtFile = getDbtFile();
                                RandomAccessFile dbtRandomAccessFile = new RandomAccessFile(dbtFile, "r");
                                try {
                                    FileChannel dbtChannel = dbtRandomAccessFile.getChannel();

                                    // Read the complete header into headerBuf
                                    dbtChannel.position(0);
                                    headerBuf.limit(64);
                                    do {
                                        if (dbtChannel.read(headerBuf) == -1) // EOF
                                        {
                                            throw new IOException("End of file encountered while reading DBT header");
                                        }
                                    } while (headerBuf.hasRemaining());

                                    headerBuf.position(0);

                                    // Read the block length
                                    headerBuf.position(20);
                                    int blockLength = headerBuf.getShort() & 0xffff; // Unsigned short
                                    if (blockLength < 64)
                                        throw new IOException("DBT appears to be corrupt. Block length (" + blockLength + ") < 64 blocks (512 bytes)");

                                    // Move to the block where the memo field is stored.
                                    dbtChannel.position(blockNumber * blockLength);
                                    headerBuf.clear();
                                    headerBuf.limit(8);
                                    while (headerBuf.position() < 8) {
                                        if (dbtChannel.read(headerBuf) == -1) // EOF
                                            throw new IOException("End of file encountered while reading DBT block header");
                                    }
                                    headerBuf.position(0);
                                    byte byte1 = headerBuf.get();
                                    byte byte2 = headerBuf.get();
                                    byte byte3 = headerBuf.get();
                                    byte byte4 = headerBuf.get();
                                    if (byte1 != (byte) 0xff || byte2 != (byte) 0xff || byte3 != (byte) 0x08 || byte4 != (byte) 0x00)
                                        throw new IOException("DBT appears to be corrupt. Block header start: " + String.format("0x%02x 0x%02x 0x%02x 0x%02x", byte1, byte2, byte3, byte4));
                                    int valueLength = headerBuf.getInt(); // 32-bit unsigned that won't overflow
                                    valueLength -= 8; // Remove 8 byte header length

                                    // Acquire shared lock
                                    if (isFileLockingEnabled())
                                        dbtChannel.lock(blockNumber * blockLength, valueLength + 8, true);

                                    // Use headerBuf if it's large enough. Otherwise
                                    // allocate a new buffer.
                                    ByteBuffer dbtRecordBuffer;
                                    if (headerBuf.capacity() < valueLength) {
                                        dbtRecordBuffer = ByteBuffer.allocate(valueLength);
                                    } else {
                                        headerBuf.position(0);
                                        headerBuf.limit(valueLength);
                                        dbtRecordBuffer = headerBuf;
                                    }
                                    while (dbtRecordBuffer.hasRemaining())
                                        dbtChannel.read(dbtRecordBuffer);
                                    dbtRecordBuffer.flip();
                                    byte[] valueBytes = new byte[valueLength];
                                    dbtRecordBuffer.get(valueBytes);
                                    values[count] = new DBFValue(currentField, new String(valueBytes));
                                } finally {
                                    try {
                                        dbtRandomAccessFile.close();
                                    } catch (Exception ex) {
                                        ex.printStackTrace();
                                    }
                                }
                            }
                            break;
                        case N:
                        case F:
                            dataString = new String(fieldData).trim();
                            if (dataString.length() == 0)
                                values[count] = new DBFValue(currentField, currentField.getDefaultValue().getValue());
                            else
                                values[count] = new DBFValue(currentField, Double.parseDouble(dataString));
                            break;
                        case D:
                            if (fieldData.length == 0 || fieldData[0] == ' ') // Blank date
                                values[count] = new DBFValue(currentField, new DBFDate(0, 0, 0));
                            else {
                                int year = Integer.parseInt(new String(fieldData, 0, 4));
                                int month = Integer.parseInt(new String(fieldData, 4, 2));
                                int day = Integer.parseInt(new String(fieldData, 6, 2));
                                values[count] = new DBFValue(currentField, new DBFDate(month, day, year));
                            }
                            break;
                        case L:
                            values[count] = new DBFValue(currentField, (fieldData[0] == 'y' || fieldData[0] == 'Y' || fieldData[0] == 't' || fieldData[0] == 'T') ? Boolean.TRUE : Boolean.FALSE);
                            break;
                        case U:
                        default:
                            values[count] = new DBFValue(currentField, "");
                            break;
                    }
                }
            } finally {
                if (isThreadSafetyEnabled()) {
                    threadLock.unlock();
                }
            }
        }
    }

    /**
     * Gets a file object pointing to the DBT file which would be paired with
     * this DBF file.
     * @return A file object.
     */
    protected File getDbtFile() {
        return new File(dbfFile.getAbsolutePath().substring(0, dbfFile.getAbsolutePath().length() - 3) + "dbt");
    }

    /**
     * Gets the field number by the field's name. If the field doesn't exist,
     * 0 is returned.
     * @param fieldName The name of the field.
     * @return The field's number if it exists, or 0 if not.
     */
    public int getFieldNumberByName(String fieldName) {
        Iterator<DBFField> i = getStructure().getFields().iterator();
        for (int count = 0; i.hasNext(); count++) {
            DBFField f = i.next();
            if (f.getFieldName().equalsIgnoreCase(fieldName))
                return count + 1;
        }

        return 0;
    }

    /**
     * A convenience method for getting a string value for the specified field.
     * This is equivelant to calling <code>getValue(fieldNumber).getString()</code>.
     * Field numbers start at 1.
     * @param fieldNumber The number of the field.
     * @return The field's on disk value.
     */
    public String getString(int fieldNumber) {
        return getValue(fieldNumber).getString();
    }

    /**
     * A convenience method for getting a string value for the specified field.
     * This is equivalent to calling <code>getValue(fieldName).getString()</code>.
     * @param fieldName The name of the field.
     * @return The field's on disk value.
     */
    public String getString(String fieldName) {
        return getValue(fieldName).getString();
    }

    /**
     * A convenience method for getting an int value for the specified field.
     * This is equivalent to calling <code>getValue(fieldNumber).getInt()</code>.
     * Field numbers start at 1.
     * @param fieldNumber The number of the field.
     * @return The field's on disk value.
     */
    public int getInt(int fieldNumber) {
        return getValue(fieldNumber).getInt();
    }

    /**
     * A convenience method for getting an int value for the specified field.
     * This is equivalent to calling <code>getValue(fieldName).getInt()</code>.
     * @param fieldName The name of the field.
     * @return The field's on disk value.
     */
    public int getInt(String fieldName) {
        return getValue(fieldName).getInt();
    }

    /**
     * A convenience method for getting a double value for the specified field.
     * This is equivalent to calling <code>getValue(fieldNumber).getDouble()</code>.
     * Field numbers start at 1.
     * @param fieldNumber The number of the field.
     * @return The field's on disk value.
     */
    public double getDouble(int fieldNumber) {
        return getValue(fieldNumber).getDouble();
    }

    /**
     * A convenience method for getting a double value for the specified field.
     * This is equivalent to calling <code>getValue(fieldName).getDouble()</code>.
     * @param fieldName The name of the field.
     * @return The field's on disk value.
     */
    public double getDouble(String fieldName) {
        return getValue(fieldName).getDouble();
    }

    /**
     * A convenience method for getting a date value for the specified field.
     * This is equivalent to calling <code>getValue(fieldNumber).getDate()</code>.
     * Field numbers start at 1.
     * @param fieldNumber The number of the field.
     * @return The field's on disk value.
     */
    public DBFDate getDate(int fieldNumber) {
        return getValue(fieldNumber).getDate();
    }

    /**
     * A convenience method for getting a date value for the specified field.
     * This is equivalent to calling <code>getValue(fieldName).getDate()</code>.
     * @param fieldName The name of the field.
     * @return The field's on disk value.
     */
    public DBFDate getDate(String fieldName) {
        return getValue(fieldName).getDate();
    }

    /**
     * A convenience method for getting a boolean value for the specified field.
     * This is equivalent to calling <code>getValue(fieldNumber).getBoolean()</code>.
     * Field numbers start at 1.
     * @param fieldNumber The number of the field.
     * @return The field's on disk value.
     */
    public boolean getBoolean(int fieldNumber) {
        return getValue(fieldNumber).getBoolean();
    }

    /**
     * A convenience method for getting a boolean value for the specified field.
     * This is equivalent to calling <code>getValue(fieldName).getBoolean()</code>.
     * @param fieldName The name of the field.
     * @return The field's on disk value.
     */
    public boolean getBoolean(String fieldName) {
        return getValue(fieldName).getBoolean();
    }

    /**
     * Gets the value of the given field based on its field number. Field
     * numbers start at 1.
     * @param fieldNumber The number of the field.
     * @return The field's on disk value.
     */
    public DBFValue getValue(int fieldNumber) {
        return values[fieldNumber - 1];
    }

    /**
     * Gets the value of the given field.
     * @param fieldName The name of the field to fetch the value for.
     * @throws IllegalArgumentException If a bad field name is specified.
     * @return The field's on disk value.
     */
    public DBFValue getValue(String fieldName) throws IllegalArgumentException {
        int fieldNumber = getFieldNumberByName(fieldName);
        if (fieldNumber == 0)
            throw new IllegalArgumentException("Field " + fieldName + " does not exist");
        else
            return getValue(fieldNumber);
    }

    /**
     * Replaces the specified field's value in the current DBF. Field numbers
     * start at 1.
     * @param fieldNumber The field number.
     * @param value The new value.
     * @throws IOException If an I/O error occurs.
     */
    public void replace(int fieldNumber, Object value) throws IOException {
        if (fieldNumber <= 0)
            throw new IllegalArgumentException("Field number must be greater than zero");
        else if (fieldNumber > structure.getFields().size())
            throw new IllegalArgumentException("Field number greater than the number of fields in the table (" + fieldNumber + " > " + structure.getFields().size() + ")");
        else if (bof())
            throw new IllegalStateException("Cannot replace a value at beginning of file");
        else if (eof())
            throw new IllegalStateException("Cannot replace a value at end of file");

        if (value instanceof DBFValue)
            value = ((DBFValue) value).getValue();

        Object oldValue = values[fieldNumber - 1].getValue();
        values[fieldNumber - 1].setValue(value);
        try {
            if (isThreadSafetyEnabled()) {
                threadLock.lock();
            }
            int fieldSkipLength = 1; // Skip over deleted flag

            // Calculate size of preceding fields to skip over
            Iterator<DBFField> i = structure.getFields().iterator();
            for (int count = 0; count < (fieldNumber - 1); count++) {
                DBFField f = i.next();
                fieldSkipLength += f.getFieldLength();
            }
            DBFField f = i.next();

            FileChannel channel = randomAccessFile.getChannel();

            // If this field is a DBT field, write the external data now.
            switch (f.getFieldType()) {
                case M:
                case B:
                case G:
                    // Open the DBT file
                    File dbtFile = getDbtFile();
                    RandomAccessFile dbtRandomAccessFile = new RandomAccessFile(dbtFile, "rw" + (synchronousWritesEnabled ? "s" : ""));
                    try {
                        FileChannel dbtChannel = dbtRandomAccessFile.getChannel();

                        // Fetch the block size.
                        dbtChannel.position(20);
                        buf.position(0);
                        buf.limit(2);
                        while (buf.hasRemaining())
                            if (dbtChannel.read(buf) == -1) // EOF
                                throw new IOException("End of file encountered while reading DBT structure");
                        buf.position(0);
                        int blockLength = buf.getShort() & 0xffff;
                        if (blockLength < 64)
                            throw new IOException("DBT block length less than 64 bytes");

                        // Calculate the length of both the old value and the new
                        // value.
                        int oldValueLength;
                        int newValueLength;
                        if (f.getFieldType().equals(DBFField.FieldType.M)) // String
                        {
                            oldValueLength = ((String) oldValue).length();
                            newValueLength = ((String) value).length();
                        } else // binary
                        {
                            oldValueLength = ((byte[]) oldValue).length;
                            newValueLength = ((byte[]) value).length;
                        }

                        // Calculate the amount of DBT blocks required to fit the
                        // old value and the new value.
                        int oldValueBlocksRequired = (int) Math.ceil((oldValueLength + 8) / (double) blockLength);
                        int newValueBlocksRequired = (int) Math.ceil((newValueLength + 8) / (double) blockLength);

                        // If the new value requires more blocks than the old
                        // value, we need to append the new value to the end of the
                        // DBT. Otherwise, we can re-use the existing blocks.
                        boolean appendNewRecord = true;
                        if (newValueBlocksRequired <= oldValueBlocksRequired) {
                            // Use the existing blocks; Find the block number.
                            channel.position(structure.getHeaderLength() + fieldSkipLength + (recordNumber - 1) * structure.getRecordLength());
                            buf.position(0);
                            buf.limit(10);
                            while (buf.hasRemaining())
                                channel.read(buf);
                            buf.position(0);
                            byte[] blockNumberBytes = new byte[10];
                            buf.get(blockNumberBytes);
                            String blockNumberString = new String(blockNumberBytes).trim();
                            if (blockNumberString.length() > 0) {
                                // There is a block number attached to this record.
                                appendNewRecord = false;
                                int blockNumber = Integer.parseInt(blockNumberString);

                                // Seek to the existing block plus 4 bytes
                                dbtChannel.position(blockLength * blockNumber + 4);
                            }
                        }
                        if (appendNewRecord) {
                            // Exclusive lock the "next available block" field
                            if (isFileLockingEnabled())
                                dbtChannel.lock(0, 4, false);

                            // Read the next available block
                            buf.position(0);
                            buf.limit(4);
                            dbtChannel.position(0);
                            do {
                                dbtChannel.read(buf);
                            } while (buf.hasRemaining());
                            buf.position(0);
                            int nextAvailableBlock = buf.getInt();

                            // Add the number of required blocks and write out
                            // the next available block field
                            buf.position(0);
                            buf.putInt(nextAvailableBlock + newValueBlocksRequired);
                            buf.position(0); // flip
                            dbtChannel.position(0);
                            do {
                                dbtChannel.write(buf);
                            } while (buf.hasRemaining());

                            // Write the block number of the new value to the
                            // column in the DBF file.
                            channel.position(structure.getHeaderLength() + fieldSkipLength + (recordNumber - 1) * structure.getRecordLength());
                            buf.position(0);
                            buf.limit(10);
                            buf.put(String.format("%10d", nextAvailableBlock).getBytes());
                            buf.position(0); // flip
                            do {
                                channel.write(buf);
                            } while (buf.hasRemaining());

                            // Seek to the next available block
                            dbtChannel.position(nextAvailableBlock * blockLength);

                            // Write the 4 bytes that start a DBT block header
                            buf.position(0);
                            buf.limit(4);
                            buf.put((byte) 0xff);
                            buf.put((byte) 0xff);
                            buf.put((byte) 0x08);
                            buf.put((byte) 0x00);
                            buf.position(0);
                            do {
                                dbtChannel.write(buf);
                            } while (buf.hasRemaining());
                        }
                        // Store the start of the block so we can pad the
                        // remainder with nulls after writing the value.
                        int startOfBlock = (int) dbtChannel.position() - 4;

                        // Write the length of the new value, including the 8 header bytes
                        buf.position(0);
                        buf.limit(4);
                        buf.putInt(newValueLength + 8);
                        buf.position(0);
                        do {
                            dbtChannel.write(buf);
                        } while (buf.hasRemaining());

                        // Try to use the direct buffer if it is large enough.
                        // Otherwise allocate another buffer large enough for the
                        // new value.
                        ByteBuffer fieldBuffer;
                        if (buf.capacity() < newValueLength)
                            fieldBuffer = ByteBuffer.allocate(newValueLength);
                        else {
                            fieldBuffer = buf;
                            fieldBuffer.position(0);
                            fieldBuffer.limit(newValueLength);
                        }

                        // Fill the buffer with the new value
                        if (f.getFieldType().equals(DBFField.FieldType.M))
                            fieldBuffer.put(((String) value).getBytes());
                        else
                            fieldBuffer.put((byte[]) value);

                        // Write the value to the DBT file
                        fieldBuffer.position(0);
                        do {
                            dbtChannel.write(fieldBuffer);
                        } while (fieldBuffer.hasRemaining());

                        // Pad the remainder of the block with nulls so that
                        // the file size is divisible by the block size. This
                        // is not strictly necessary.
                        int remainderLength = newValueBlocksRequired * blockLength
                                - ((int) dbtChannel.position() - startOfBlock);
                        int remainderPosition = (int) dbtChannel.position() + remainderLength;
                        buf.clear();
                        buf.limit(Math.min(buf.capacity(), remainderLength));
                        while (buf.hasRemaining())
                            buf.put((byte) 0); // null
                        while (dbtChannel.position() < remainderPosition) {
                            buf.position(0);
                            buf.limit(Math.min(remainderPosition - (int) dbtChannel.position(), buf.capacity()));
                            do {
                                dbtChannel.write(buf);
                            } while (buf.hasRemaining());
                        }

                        return;
                    } finally {
                        dbtRandomAccessFile.close();
                    }
            }

            // Try to use the direct buffer if it is large enough. Otherwise
            // allocate another buffer large enough for the field.
            ByteBuffer fieldBuffer;
            if (buf.capacity() < f.getFieldLength())
                fieldBuffer = ByteBuffer.allocate(f.getFieldLength());
            else {
                fieldBuffer = buf;
                fieldBuffer.position(0);
                fieldBuffer.limit(f.getFieldLength());
            }

            // Fill the buffer with spaces
            while (fieldBuffer.hasRemaining())
                fieldBuffer.put((byte) ' ');
            fieldBuffer.position(0);

            // Write value, truncating it if necessary, so it doesn't overflow
            // the field.
            byte[] valueBytes = value.toString().getBytes();
            fieldBuffer.put(valueBytes, 0, Math.min(valueBytes.length, f.getFieldLength()));
            fieldBuffer.position(0);
            channel.position(structure.getHeaderLength() + fieldSkipLength + (recordNumber - 1) * structure.getRecordLength());
            FileLock lock = null;
            if (isFileLockingEnabled())
            lock = channel.lock(structure.getHeaderLength() + fieldSkipLength + (recordNumber - 1) * structure.getRecordLength(), f.getFieldLength(), false);
            try {
                while (fieldBuffer.hasRemaining())
                    channel.write(fieldBuffer);
            } finally {
                if (isFileLockingEnabled())
                    lock.release();
            }
        } finally {
            if (isThreadSafetyEnabled()) {
                threadLock.unlock();
            }
        }

        updateLastModifiedDate();
    }

    /**
     * Creates a new DBF file, using the specified structure.
     * @param dbfFile The file to write the new DBF.
     * @param structure The structure of the new DBF.
     * @return The newly created DBF file.
     * @throws IOException If an I/O error occurs.
     */
    public static DBF create(File dbfFile, DBFStructure structure) throws IOException {
        if (!dbfFile.getName().toLowerCase().endsWith(".dbf"))
            dbfFile = new File(dbfFile.getAbsolutePath() + ".dbf");

        if (structure.getFields() == null || structure.getFields().isEmpty())
            throw new IllegalArgumentException("The DBF structure has no fields.");

        for (DBFField field : structure.getFields()) {
            if (field.getFieldType().isMemoField()) {
                structure.setDbtPaired(true);
            }
        }

        structure.setNumberOfRecords(0);
        DBF dbf = new DBF(dbfFile, new RandomAccessFile(dbfFile, "rw" + (synchronousWritesEnabled ? "s" : "")), structure);
        if (dbf.getStructure().isDbtPaired())
            dbf.createDbt();
        dbf.writeStructure();
        dbf.gotoRecord(0);
        return dbf;
    }

    public void createDbt() throws IOException {
        File dbtFile = getDbtFile();
        if (dbtFile.exists()) {
            throw new IOException("File already exists while attempting to create the DBT file: " + dbtFile.getAbsolutePath());
        }

        RandomAccessFile dbtRandomAccessFile = new RandomAccessFile(dbtFile, "rw" + (synchronousWritesEnabled ? "s" : ""));
        try {
            if (isThreadSafetyEnabled()) {
                threadLock.lock();
            }
            FileChannel dbtChannel = dbtRandomAccessFile.getChannel();

            // Write the next available block
            buf.clear();
            buf.putInt(1);
            buf.flip();
            while (buf.hasRemaining())
                dbtChannel.write(buf);

            // Write the block size
            buf.clear();
            buf.putInt(64);
            buf.flip();
            while (buf.hasRemaining())
                dbtChannel.write(buf);

            // Write the name of the DBF file.
            String filename = dbfFile.getPath().replaceAll("\\..+$", "");
            buf.clear();
            for (int count = 0; count < 8; count++) {
                if (count < filename.length()) {
                    buf.put((byte) filename.charAt(count));
                } else {
                    buf.put((byte) 0);
                }
            }
            buf.flip();
            while (buf.hasRemaining()) {
                dbtChannel.write(buf);
            }

            dbtChannel.position(dbtChannel.position() + 4);
            assert(dbtChannel.position() == 20);

            // Write the block length
            buf.clear();
            buf.putShort((short) (getDbtBlockSize() * 64));
            buf.flip();
            while (buf.hasRemaining()) {
                dbtChannel.write(buf);
            }

            buf.clear();
            buf.put((byte) 0);
            buf.flip();
            dbtChannel.position(511);
            dbtChannel.write(buf);
            while (buf.hasRemaining()) {
                dbtChannel.write(buf);
            }
            
            dbtChannel.close();
        } finally {
            if (isThreadSafetyEnabled()) {
                threadLock.unlock();
            }
            dbtRandomAccessFile.close();
        }
    }

    /**
     * Creates a new DBF file, using the specified fields for the structure.
     * @param dbfFile the file to write the new DBF.
     * @param fields the fields which make up the structure of the new DBF.
     * @return the newly created DBF file.
     * @throws IOException if an I/O error occurs.
     */
    public static DBF create(File dbfFile, Iterable<DBFField> fields) throws IOException {
        DBFStructure structure = new DBFStructure();
        for (DBFField field : fields)
            structure.getFields().add(field);

        return create(dbfFile, structure);
    }

    /**
     * Creates a new DBF file, using the specified structure.
     * @param relativePathFile the relative path of the file to write the new DBF.
     * @param structure the structure of the new DBF.
     * @return the newly created DBF file.
     * @throws IOException if an I/O error occurs.
     */
    public static DBF create(String relativePathFile, DBFStructure structure) throws IOException {
        return create(new File(getCurrentDirectory(), relativePathFile), structure);
    }

    /**
     * Creates a new DBF file, using the specified fields for the structure.
     * @param relativePathFile the relative path of the file to write the new DBF.
     * @param fields the fields which make up the structure of the new DBF.
     * @return the newly created DBF file.
     * @throws IOException if an I/O error occurs.
     */
    public static DBF create(String relativePathFile, Iterable<DBFField> fields) throws IOException {
        return create(new File(getCurrentDirectory(), relativePathFile), fields);
    }

    /**
     * Replaces the value of the field with the specified name, with the
     * specified value.
     * @param fieldName The name of the field.
     * @param value The value to replace in the field.
     * @throws IOException If an I/O error occurs.
     */
    public void replace(String fieldName, Object value) throws IOException {
        int fieldNumber = getFieldNumberByName(fieldName);
        if (fieldNumber == 0)
            throw new IllegalArgumentException("Field " + fieldName + " does not exist");
        else
            replace(fieldNumber, value);
    }

    /**
     * Gets the current record number.
     * @return The current record number.
     */
    public int recno() {
        return recordNumber;
    }

    /**
     * Deletes the current record, if it is not deleted already.
     * @throws IOException If an I/O error occurs.
     */
    public void delete() throws IOException {
        setDeleted(true);
    }

    /**
     * Undeletes the current record, if it is deleted.
     * @throws IOException If an I/O error occurs.
     */
    public void undelete() throws IOException {
        setDeleted(false);
    }

    /**
     * Sets the record's deleted status to the given value. If the deleted
     * status for the record is identical to the given argument, no action
     * will be performed.
     * @param delete <code>true</code> if deleting, <code>false</code> if
     * undeleting.
     * @throws IOException If an I/O error occurs.
     */
    protected void setDeleted(boolean delete) throws IOException {
        if (bof())
            throw new IllegalStateException("Cannot delete or undelete at beginning of file");
        else if (eof())
            throw new IllegalStateException("Cannot delete or undelete at end of file");

        if (currentRecordDeleted != delete) {
            try {
                if (isThreadSafetyEnabled()) {
                    threadLock.lock();
                }
                buf.position(0);
                buf.limit(1);
                buf.put((byte) (delete ? '*' : ' '));
                buf.position(0);
                FileChannel channel = randomAccessFile.getChannel();
                channel.position(structure.getHeaderLength() + (recordNumber - 1) * structure.getRecordLength());
                while (buf.hasRemaining())
                    channel.write(buf);
            } finally {
                if (isThreadSafetyEnabled()) {
                    threadLock.unlock();
                }
            }
            currentRecordDeleted = delete;

            updateLastModifiedDate();
        }
    }

    /**
     * Updates the last modified date to the current date. If the last modified
     * date is already the current date, nothing will be performed.
     * @throws IOException If an I/O error occurs.
     */
    protected void updateLastModifiedDate() throws IOException {
        // Get the current day, month, year.
        GregorianCalendar cal = new GregorianCalendar();
        int year = cal.get(Calendar.YEAR);
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DAY_OF_MONTH);

        // Pack it into the 3 bytes expected by the DBF header.
        byte byte1 = (byte) (year - 1900);
        byte byte2 = (byte) month;
        byte byte3 = (byte) day;

        if (structure.getLastUpdated() != null
                && structure.getLastUpdated().equals(new DBFDate(month, day, year)))
            return;

        // Write the date
        try {
            if (isThreadSafetyEnabled()) {
                threadLock.lock();
            }
            buf.clear();
            buf.put(byte1);
            buf.put(byte2);
            buf.put(byte3);
            buf.flip();
            buf.limit(3);
            FileChannel channel = randomAccessFile.getChannel();
            channel.position(1);
            FileLock lock = null;
            if (isFileLockingEnabled())
                lock = channel.lock(1, 3, false);
            try {
                while (buf.hasRemaining())
                    channel.write(buf);
            } finally {
                if (isFileLockingEnabled())
                    lock.release();
            }
        } finally {
            if (isThreadSafetyEnabled()) {
                threadLock.unlock();
            }
        }
    }

    /**
     * Appends a blank record to the end of the current DBF.
     * @throws IOException If an I/O error occurs.
     */
    public void appendBlank() throws IOException {
        try {
            if (isThreadSafetyEnabled()) {
                threadLock.lock();
            }
            FileChannel channel = randomAccessFile.getChannel();
            // Lock "number of records" field in header and then the range of
            // the new record
            FileLock lock1 = null;
            FileLock lock2 = null;
            try {
                if (isFileLockingEnabled()) {
                    lock1 = channel.lock(4, 4, false);
                }

                // Check if increasing the size of the file will put it over
                // the 2GB limit. We check the logical size instead of the
                // actual size, since there will be an EOF mark and possibly
                // even garbage after it.
                if (structure.getHeaderLength() +
                        (1 + structure.getRecordLength() * (structure.getNumberOfRecords() + 1))
                        > 2147483648L)
                    throw new IOException("File too large to append.");

                // Lock and write the new record
                channel.position(structure.getHeaderLength()
                        + structure.getRecordLength() * structure.getNumberOfRecords());
                if (isFileLockingEnabled()) {
                    lock2 = channel.lock(structure.getHeaderLength()
                            + structure.getRecordLength() * structure.getNumberOfRecords(),
                            structure.getRecordLength() + 1, false); // New record plus EOF mark
                }
                buf.position(0);
                buf.limit(1);
                buf.put((byte) ' '); // Not deleted
                buf.position(0);
                while (buf.hasRemaining())
                    channel.write(buf);
                Iterator<DBFField> i = structure.getFields().iterator();
                while (i.hasNext()) {
                    DBFField f = i.next();

                    // Try to use the direct buffer if it is large enough. Otherwise
                    // allocate another buffer large enough for the field.
                    ByteBuffer fieldBuffer;
                    if (buf.capacity() < f.getFieldLength())
                        fieldBuffer = ByteBuffer.allocate(f.getFieldLength());
                    else {
                        fieldBuffer = buf;
                        fieldBuffer.clear();
                        fieldBuffer.limit(f.getFieldLength());
                    }

                    // Fill the buffer with spaces
                    while (fieldBuffer.hasRemaining())
                        fieldBuffer.put((byte) ' ');
                    fieldBuffer.position(0);

                    // Write value, truncating it if necessary, so it doesn't overflow
                    // the field.
                    byte[] valueBytes = f.getDefaultValue().getValue().toString().getBytes();
                    fieldBuffer.put(valueBytes, 0, Math.min(valueBytes.length, f.getFieldLength()));
                    fieldBuffer.position(0);
                    while (fieldBuffer.hasRemaining())
                        channel.write(fieldBuffer);
                }

                // Write the EOF mark
                buf.position(0);
                buf.limit(1);
                buf.put((byte) 0x1a);
                buf.position(0);
                while (buf.hasRemaining())
                    channel.write(buf);

                // Go back and bump the number of records in the table
                channel.position(4);
                buf.clear();
                while (buf.position() < 4)
                    channel.read(buf);
                buf.flip();
                int numberOfRecords = buf.getInt() + 1;
                buf.clear();
                buf.putInt(numberOfRecords);
                buf.flip();
                channel.position(4);
                while (buf.hasRemaining())
                    channel.write(buf);

                // Update the number of records in the in-memory structure
                structure.setNumberOfRecords(numberOfRecords);
            } finally {
                if (lock1 != null) {
                    try {
                        lock1.release();
                    } catch (Exception ex) {
                    }
                }
                if (lock2 != null) {
                    try {
                        lock2.release();
                    } catch (Exception ex) {
                    }
                }
            }
        } finally {
            if (isThreadSafetyEnabled()) {
                threadLock.unlock();
            }
        }

        updateLastModifiedDate();
        gotoRecord(structure.getNumberOfRecords());
    }

    /**
     * Calculates the lengths of each record, and the length of the header, and
     * updates the structure with those values, which are used internally for
     * calculating offsets when writing values and the structure itself.
     */
    protected void calculateLengths() {
        // Record length
        short recordLength = 1;
        for (DBFField field : structure.getFields())
            recordLength += field.getFieldLength();
        structure.setRecordLength(recordLength);

        // Header length
        structure.setHeaderLength((short)(32 + 32 * structure.getFields().size() + 1));
    }

    /**
     * Calculates some structure flags, based on the field configuration. These
     * are used when writing the structure to disk.
     */
    protected void calculateFlags() {

    }

    /**
     * Prints the current DBF file's structure to <code>System.out</code>.
     */
    public void printStructure() {
        printStructure(System.out);
    }

    /**
     * Prints the current DBF file's structure to the requested print stream.
     * @param out The print stream to print the structure to.
     */
    public void printStructure(PrintStream out) {
        out.println("----------------------------------");
        out.println("Last Updated: " + structure.getLastUpdated());
        out.println("Records: " + structure.getNumberOfRecords());

        // Column header
        out.println("  #       Name   Len Dec Type");

        // Fields
        int fieldNumber = 0;
        for (DBFField field : getStructure().getFields()) {
            out.printf("%3d %10s %5d  %2d %s\n",
                    ++fieldNumber, field.getFieldName(), field.getFieldLength(),
                    field.getDecimalLength(), field.getFieldType().getFullName());
        }

        out.println("----------------------------------");
    }
}
