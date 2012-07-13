/*
 * Copyright (c) 2009-2012, i Data Connect!
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
package com.idataconnect.jdbfdriver.index;

import com.idataconnect.jdbfdriver.DBF;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;

/**
 * NDX index implementation.
 */
public class NDX {
    
    public static final int BLOCK_SIZE = 512;

    private final ReentrantLock threadLock;
    private final ByteBuffer buf = ByteBuffer.allocate(BLOCK_SIZE)
                                             .order(ByteOrder.LITTLE_ENDIAN);

    private File ndxFile;
    private RandomAccessFile randomAccessFile;
    private int startBlock;
    private int totalBlocks;
    private int keyLength;
    private int keysPerBlock;
    private IndexDataType dataType;
    private boolean unique;
    private String key;
    
    private int blockNumber;

    private NDX(File ndxFile, RandomAccessFile randomAccessFile, ReentrantLock threadLock) {
        this.ndxFile = ndxFile;
        this.randomAccessFile = randomAccessFile;
        this.threadLock = threadLock;
    }

    public static NDX open(File ndxFile) throws IOException {
        return open(ndxFile, new ReentrantLock());
    }

    public static NDX open(File ndxFile, ReentrantLock threadLock)
            throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(ndxFile, "rw"
                + (DBF.isSynchronousWritesEnabled() ? "s" : ""));
        final NDX ndx = new NDX(ndxFile, randomAccessFile, threadLock);
        ndx.readStructure();
        return ndx;
    }

    protected void readStructure() throws IOException {
        FileChannel channel = randomAccessFile.getChannel();
        channel.position(0);
        buf.position(0);
        buf.limit(BLOCK_SIZE);
        while (buf.hasRemaining()) {
            channel.read(buf);
        }

        buf.position(0);
        startBlock = buf.getInt();
        totalBlocks = buf.getInt();
        buf.position(buf.position() + 4); // Skip reserved
        keyLength = buf.getShort() & 0xffff;
        keysPerBlock = buf.getShort() & 0xffff;
        dataType = IndexDataType.valueOf(buf.getShort() & 0xffff);
        int keyRecordSize = buf.getShort() & 0xffff;
        assert(keyRecordSize() == keyRecordSize)
                : "Invalid key record size. Disk=" + keyRecordSize + "; Asserted=" + keyRecordSize();
        buf.position(buf.position() + 2); // Skip reserved
        unique = buf.getShort() != 0;
        char[] keyChars = new char[buf.remaining()];
        int i;
        for (i = 0; buf.hasRemaining(); i++) {
            byte b = buf.get();
            keyChars[i] = (char) (b & 0xff);
            if (b == 0) {
                break;
            }
        }
        key = String.valueOf(keyChars, 0, i);
    }

    private int keyRecordSize() {
        return (int) Math.ceil(keyLength / 4f) * 4 + 8;
    }

    public void gotoBlock(int block) throws IOException {
        if (blockNumber != block) {
            blockNumber = block;
            readBlock();
        }
    }
    
    public void readBlock() throws IOException {
        if (blockNumber <= 0) {
            throw new IllegalStateException("Invalid block number: " + blockNumber);
        }
        FileChannel channel = randomAccessFile.getChannel();
        buf.position(0);
        buf.limit(BLOCK_SIZE);
        channel.position(BLOCK_SIZE * blockNumber);
        while (buf.hasRemaining()) {
            channel.read(buf);
        }
    }

    /**
     * Finds a DBF field number by searching the index for the given value.
     *
     * @param value the value to search the index for, matching the type of
     * field that the index supports
     *
     * @return the field number in the DBF which contains the given value, or
     * <em>-1</em> if the value was not found in the index
     * @throws IOException if an I/O error occurs
     */
    public int find(Object value) throws IOException {
        return find(value, startBlock);
    }

    private int find(Object value, int blockNumber) throws IOException {
        gotoBlock(blockNumber);
        final int keysInBlock = keysInBlock();

        int nextBlock, recordNumber;
        int compareResult = 0;
        for (int i = 0; i < keysInBlock; i++) {
            nextBlock = nextBlock(i);
            recordNumber = recordNumber(i);
            switch (dataType) {
                default:
                case CHARACTER:
                    byte[] bytes = new byte[keyRecordSize() - 8];
                    byte b;
                    int j;
                    for (j = 0; j < bytes.length; j++) {
                        b = buf.get(12 + i * keyRecordSize() + j);
                        if (b == 0) {
                            break;
                        } else {
                            bytes[j] = b;
                        }
                    }
                    // Pad the search key with spaces so that the length is
                    // equal to the NDX key length
                    StringBuilder sb = new StringBuilder(keyLength);
                    sb.append(value.toString());
                    while (sb.length() < keyLength) {
                        sb.append(' ');
                    }
                    compareResult = sb.toString().compareTo(new String(bytes, 0, j));
                    break;
                case NUMERIC:
                    break;
                case DATE:
                    throw new UnsupportedOperationException("NDX does not support dates. Hint: Convert the date to a string or number first");
            }

            if (compareResult >= 0) {
                if (nextBlock == 0) {
                    // Leaf
                    return recordNumber;
                } else {
                    // Branch
                    return find(value, nextBlock);
                }
            }
        }

        return -1;
    }

    /**
     * Fetches the next block pointer for the given key which exists in
     * <code>buf</code> after a call to {@link #readBlock}.
     * @param key the zero based key within the block
     */
    private int nextBlock(int key) {
        return buf.getInt(4 + key * keyRecordSize());
    }

    private int recordNumber(int key) {
        return buf.getInt(8 + key * keyRecordSize());
    }

    private int keysInBlock() {
        return buf.getInt(0);
    }

    /**
     * Prints the current NDX file's structure to <code>System.out</code>.
     */
    public void printStructure() {
        printStructure(System.out);
    }

    /**
     * Closes the current file and releases resources taken by the connection to
     * this NDX file.
     * @throws IOException if an I/O error occurs
     */
    public void close() throws IOException {
        randomAccessFile.close();
    }

    /**
     * Prints the current NDX file's structure to the requested print stream.
     * @param out The print stream to print the structure to.
     */
    public void printStructure(PrintStream out) {
        out.println("----------------------------------");
        
        out.printf("Start Block:    %18d\n", startBlock);
        out.printf("Total Blocks:   %18d\n", totalBlocks);
        out.printf("Key Length:     %18d\n", keyLength);
        out.printf("Key Record Size:%18d\n", keyRecordSize());
        out.printf("Keys Per Block: %18d\n", keysPerBlock);
        out.printf("Data Type:      %18s\n", dataType.name());
        out.printf("Unique:         %18b\n", unique);
        out.printf("Key: %29s\n", key);

        out.println("----------------------------------");
    }
}
