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
import com.idataconnect.jdbfdriver.DBFDate;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MDX multiple index implementation.
 */
public class MDX {

    private final ReentrantLock threadLock;
    private final File mdxFile;
    private final RandomAccessFile randomAccessFile;
    private final ByteBuffer buf;

    private String dbfName;
    private int blockSize;
    private int blockSizeMultiplier;
    private DBFDate reindexDate;
    private boolean production;
    private int entriesInTag;
    private int tagLength;
    private int tagsInUse;
    private int numberOfBlocks;
    private int firstFreeBlock;
    private int availableBlock;
    private DBFDate lastUpdateDate;
    private Tag[] tags;

    private int blockNumber;

    private MDX(File mdxFile, RandomAccessFile randomAccessFile, ReentrantLock threadLock) {
        this.mdxFile = mdxFile;
        this.randomAccessFile = randomAccessFile;
        this.threadLock = threadLock;
        this.buf = ByteBuffer.allocate(2048).order(ByteOrder.LITTLE_ENDIAN);
    }
    
    private class Tag {

        private int headerPage;
        private String name;
        private IndexDataType dataType;
        private int leftTag;
        private int rightTag;
        private int backwardTag;
        private boolean unique;
        private boolean descending;
        private int rootBlock;
        private int sizeInPages;
        private int keyLength;
        private int keysPerBlock;
        private int secondaryKeyType;
        private int keyItemLength;
    }
    
    public static MDX open(File mdxFile) throws IOException {
        return open(mdxFile, new ReentrantLock());
    }
    
    public static MDX open(File mdxFile, ReentrantLock threadLock) throws IOException {
        final RandomAccessFile randomAccessFile = new RandomAccessFile(mdxFile, "rw" + (DBF.isSynchronousWritesEnabled() ? "s" : ""));
        final MDX mdx = new MDX(mdxFile, randomAccessFile, threadLock);
        mdx.readStructure();
        return mdx;
    }
    
    protected void readStructure() throws IOException {
        FileChannel channel = randomAccessFile.getChannel();
        
        // MDX header
        channel.position(0);
        buf.position(0);
        buf.limit(544);
        while (buf.hasRemaining()) {
            if (channel.read(buf) == -1) {
                throw new IOException("EOF while reading MDX structure");
            }
        }
        buf.position(0);

        byte version = buf.get();
        if (version != 2) {
            Logger.getLogger(getClass().getName()).log(Level.WARNING,
                    "MDX [%s] has unsupported version: %x", new Object[] {mdxFile, version});
        }
        int y = (buf.get() & 0xff) + 2000;
        int m = buf.get() & 0xff;
        int d = buf.get() & 0xff;
        reindexDate = new DBFDate(m, d, y);
        byte[] stringBytes = new byte[16];
        int i;
        byte b;
        for (i = 0; i < stringBytes.length; i++) {
            b = buf.get();
            if (b == 0) {
                break;
            }

            stringBytes[i] = b;
        }
        dbfName = new String(stringBytes, 0, i);
        buf.position(20);
        blockSizeMultiplier = buf.getShort() & 0xffff;
        blockSize = buf.getShort() & 0xffff;
        production = buf.get() != 0;
        entriesInTag = buf.get() & 0xff;
        if (entriesInTag > 48 || entriesInTag < 1) {
            throw new IOException("Invalid MDX header. Entries in tag=" + entriesInTag);
        }
        tagLength = buf.get() & 0xff;
        if (tagLength > 32 || tagLength < 1) {
            throw new IOException("Invalid MDX header. Tag length=" + tagLength);
        }
        buf.position(buf.position() + 1);
        tagsInUse = buf.getShort() & 0xffff;
        buf.position(buf.position() + 2);
        numberOfBlocks = (int) (buf.getInt() & 0xffffffffL);
        firstFreeBlock = (int) (buf.getInt() & 0xffffffffL);
        availableBlock = (int) (buf.getInt() & 0xffffffffL);
        y = (buf.get() & 0xff) + 2000;
        m = buf.get() & 0xff;
        d = buf.get() & 0xff;
        lastUpdateDate = new DBFDate(m, d, y);

        // Tags
        byte keyType, keyFormat, keyFormatInHeader, keyTypeInHeader;
        tags = new Tag[tagsInUse];
        for (int tagIndex = 0; tagIndex < tags.length; tagIndex++) {
            tags[tagIndex] = new Tag();

            channel.position(544 + tagIndex * tagLength);
            buf.position(0);
            buf.limit(21);
            while (buf.hasRemaining()) {
                if (channel.read(buf) == -1) {
                    throw new IOException("EOF while reading MDX [" + mdxFile.getName() + "] tag " + tagIndex);
                }
            }
            buf.position(0);
            tags[tagIndex].headerPage = (int) (buf.getInt() & 0xffffffffL);
            for (i = 0; i < 10; i++) {
                b = buf.get();
                if (b == 0) {
                    break;
                }

                stringBytes[i] = b;
            }
            tags[tagIndex].name = new String(stringBytes, 0, i);
            if (i < 10) {
                buf.position(buf.position() + 10 - i);
            }

            keyFormat = buf.get();
            tags[tagIndex].descending = (keyFormat & 0x08) == 0x08;
            tags[tagIndex].unique = (keyFormat & 0x40) == 0x40;
            tags[tagIndex].leftTag = buf.get() & 0xff;
            tags[tagIndex].rightTag = buf.get() & 0xff;
            tags[tagIndex].backwardTag = buf.get() & 0xff;
            buf.position(buf.position() + 1);
            keyType = buf.get();
            switch (keyType) {
                case 'C':
                    tags[tagIndex].dataType = IndexDataType.CHARACTER;
                    break;
                case 'N':
                    tags[tagIndex].dataType = IndexDataType.NUMERIC;
                    break;
                case 'D':
                    tags[tagIndex].dataType = IndexDataType.CHARACTER;
                    break;
                default:
                    throw new IOException("Unknown key type: " + (char) keyType);
            }

            // Tag header
            channel.position(tags[tagIndex].headerPage * 512);
            buf.position(0);
            buf.limit(512);
            while (buf.hasRemaining()) {
                if (channel.read(buf) == -1) {
                    throw new IOException("EOF while reading tag headers");
                }
            }
            buf.position(0);

            tags[tagIndex].rootBlock = (int) (buf.getInt() & 0xffffffffL);
            tags[tagIndex].sizeInPages = (int) (buf.getInt() & 0xffffffffL);
            keyFormatInHeader = buf.get();
            if (keyFormatInHeader != keyFormat) {
                throw new IOException("Key format byte in header != key format byte in tag descriptor: " + keyFormat + " != " + keyFormatInHeader);
            }
            keyTypeInHeader = buf.get();
            if (keyTypeInHeader != keyType) {
                throw new IOException("Key type byte in header != key type byte in tag descriptor: " + keyType + " != " + keyTypeInHeader);
            }

            buf.position(buf.position() + 2);
            tags[tagIndex].keyLength = buf.getShort() & 0xffff;
            tags[tagIndex].keysPerBlock = buf.getShort() & 0xffff;
            tags[tagIndex].secondaryKeyType = buf.getShort() & 0xffff;
            tags[tagIndex].keyItemLength = buf.getShort() & 0xffff;
            buf.position(buf.position() + 3);
            if ((buf.get() != 0) != tags[tagIndex].unique) {
                throw new IOException("Unique flag in header != unique flag in tag descriptor: Key Format=" + keyFormat);
            }
        }
    }

    /**
     * Moves to the given block number and reads the block. Block numbers start
     * at index <em>1</em>.
     *
     * @param block the block number to move to
     * @throws IOException if an I/O error occurs
     */
    public void gotoBlock(int block) throws IOException {
        if (blockNumber != block) {
            blockNumber = block;
            readBlock();
        }
    }

    private int keysInBlock() {
        return buf.getInt(0);
    }

    /**
     * Finds a DBF field number by searching the given tag in the index for the
     * given value.
     *
     * @param tagName the name of the tag within the index file
     * @param value the value to search the index for, matching the type of
     * field that the index supports
     *
     * @return the field number in the DBF which contains the given value, or
     * <em>-1</em> if the value was not found in the index
     * @throws IOException if an I/O error occurs
     */
    public int find(String tagName, Object value) throws IOException {
        // Find the start block for the given tag
        for (Tag tag : tags) {
            if (tag.name.equalsIgnoreCase(tagName)) {
                return find(value, tag, tag.rootBlock);
            }
        }

        throw new IOException(String.format("Tag [%s] not found in MDX [%s]",
                tagName, mdxFile.getName()));
    }

    private static int keyRecordSize(Tag tag) {
        return (int) Math.ceil(tag.keyLength / 4f) * 4 + 8;
    }

    /**
     * Fetches the next block pointer for the given key which exists in
     * <code>buf</code> after a call to {@link #readBlock}. This is only
     * applicable for keys which are not leaves. For leaf keys,
     * {@link #recordNumber} should be used instead, in order to fetch the
     * record number.
     * @param key the zero based key within the block
     * @return the next block number, or <em>0</em> if the given key is a leaf
     */
    private int nextBlock(int key, Tag tag) {
        return buf.getInt(4 + key * keyRecordSize(tag));
    }

    /**
     * Fetches the record number for the given key which exists in
     * <code>buf</code> after a call to {@link readBlock}. This is only
     * applicable for keys which are leaves. For non-leave keys,
     * {@link #nextBlock} should be used instead, in order to fetch the
     * next block number which is used to continue the search.
     * @param key the zero based key within the block
     * @return the record number, or <em>0</em> if the given key is not a leaf
     */
    private int recordNumber(int key, Tag tag) {
        return buf.getInt(8 + key * keyRecordSize(tag));
    }

    private int find(Object value, Tag tag, int blockNumber) throws IOException {
        gotoBlock(blockNumber);
        final int keysInBlock = keysInBlock();

        int nextBlock, recordNumber;
        int compareResult = 0;
        for (int i = 0; i < keysInBlock; i++) {
            nextBlock = nextBlock(i, tag);
            recordNumber = recordNumber(i, tag);
            switch (tag.dataType) {
                case DATE: {
                    DBFDate date = (DBFDate) value;
                    value = String.valueOf(date.year) + String.valueOf(date.month) + String.valueOf(date.day);
                }
                default:
                case CHARACTER:
                    byte[] bytes = new byte[keyRecordSize(tag) - 8];
                    byte b;
                    int j;
                    for (j = 0; j < bytes.length; j++) {
                        b = buf.get(12 + i * keyRecordSize(tag) + j);
                        if (b == 0) {
                            break;
                        } else {
                            bytes[j] = b;
                        }
                    }
                    // Pad the search key with spaces so that the length is
                    // equal to the NDX key length
                    StringBuilder sb = new StringBuilder(tag.keyLength);
                    sb.append(value.toString());
                    while (sb.length() < tag.keyLength) {
                        sb.append(' ');
                    }
                    compareResult = sb.toString().compareTo(new String(bytes, 0, j));
                    break;
                case NUMERIC:
                    break;
            }

            if (compareResult >= 0) {
                if (nextBlock == 0) {
                    // Leaf
                    return recordNumber;
                } else {
                    // Branch
                    return find(value, tag, nextBlock);
                }
            }
        }

        return -1;
    }

    /**
     * Re-reads the current block.
     *
     * @throws IOException if an I/O error occurs
     */
    public void readBlock() throws IOException {
        if (blockNumber <= 0) {
            throw new IllegalStateException("Invalid block number: " + blockNumber);
        }
        FileChannel channel = randomAccessFile.getChannel();
        buf.position(0);
        buf.limit(blockSize);
        channel.position(512 * blockNumber);
        while (buf.hasRemaining()) {
            channel.read(buf);
        }
    }

    /**
     * Closes the current file and releases resources taken by the connection to
     * this MDX file.
     * @throws IOException if an I/O error occurs
     */
    public void close() throws IOException {
        randomAccessFile.close();
    }

    /**
     * Prints the current MDX file's structure to <code>System.out</code>.
     */
    public void printStructure() {
        printStructure(System.out);
    }

    /**
     * Prints the current MDX file's structure to the requested print stream.
     * @param out The print stream to print the structure to.
     */
    public void printStructure(PrintStream out) {
        out.println("----------------------------------");
        
        out.printf("DBF Name:        %17s\n", dbfName);
        out.printf("Production:      %17b\n", production);
        out.printf("Block Size:      %17d\n", blockSize);
        out.printf("Block Size Mult: %17d\n", blockSizeMultiplier);
        out.printf("Entries In Tag:  %17d\n", entriesInTag);
        out.printf("Tag Length:      %17d\n", tagLength);
        out.printf("Tags In Use:     %17d\n", tagsInUse);
        out.printf("Number Of Blocks:%17d\n", numberOfBlocks);
        out.printf("First Free Block:%17d\n", firstFreeBlock);
        out.printf("Available Block: %17d\n", availableBlock);
        out.printf("Last Updated:    %17s\n", lastUpdateDate);
        out.printf("Reindex Date:    %17s\n", reindexDate);
        out.println("Tags:");
        
        for (int i = 0; i < tags.length; i++) {
            if (i != 0) {
                out.println(" ---");
            }

            out.printf(" Name:           %17s\n", tags[i].name);
            out.printf(" Descending:     %17b\n", tags[i].descending);
            out.printf(" Unique:         %17b\n", tags[i].unique);
            out.printf(" Header Page:    %17s\n", tags[i].headerPage);
            out.printf(" Root Page:      %17s\n", tags[i].rootBlock);
            out.printf(" Size In Pages   %17s\n", tags[i].sizeInPages);
            out.printf(" Left Tag:       %17s\n", tags[i].leftTag);
            out.printf(" Right Tag:      %17s\n", tags[i].rightTag);
            out.printf(" Backward Tag:   %17s\n", tags[i].backwardTag);
            out.printf(" Key Length:     %17s\n", tags[i].keyLength);
            out.printf(" Keys Per Block  %17s\n", tags[i].keysPerBlock);
            out.printf(" 2nd Key Type:   %17s\n", tags[i].secondaryKeyType);
            out.printf(" Key Item Length:%17s\n", tags[i].keyItemLength);
        }

        out.println("----------------------------------");
    }
}
