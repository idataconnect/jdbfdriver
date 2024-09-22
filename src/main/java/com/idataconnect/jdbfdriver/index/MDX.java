/*
 * Copyright (c) 2009-2024, i Data Connect!
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
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MDX multiple index implementation.
 */
public class MDX implements DBFIndex {

    public static final int BLOCK_SIZE = 512;

    protected final ReentrantLock threadLock;
    protected final File mdxFile;
    protected final RandomAccessFile randomAccessFile;
    protected final ByteBuffer buf;

    protected String dbfName;
    protected int nodeSize;
    protected int blockSizeMultiplier;
    protected DBFDate reindexDate;
    protected boolean production;
    protected int keysInTag;
    protected int tagLength;
    protected int tagsInUse;
    protected int numberOfBlocks;
    protected int firstFreeBlock;
    protected int availableBlock;
    protected DBFDate lastUpdateDate;
    protected Tag[] tags;

    protected int blockNumber;
    protected int keyIndex; // key within node
    protected Tag tag;

    protected MDX(File mdxFile, RandomAccessFile randomAccessFile, ReentrantLock threadLock) {
        this.mdxFile = mdxFile;
        this.randomAccessFile = randomAccessFile;
        this.threadLock = threadLock;
        this.buf = ByteBuffer.allocate(BLOCK_SIZE * 4).order(ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * A tag within an MDX file, representing one of many indexes
     * contained within the MDX.
     */
    public class Tag implements Serializable {

        private static final long serialVersionUID = 1L;

        private int headerBlock;
        private String name;
        private IndexDataType dataType;
        private int leftTag;
        private int rightTag;
        private int backwardTag;
        private boolean unique;
        private boolean descending;
        private int rootBlock;
        private int sizeInBlocks;
        private int keyLength;
        private int keysPerBlock;
        private int secondaryKeyType;
        private int keyItemLength;

        /**
         * Getter for the header block.
         * @return the header block
         */
        private int getHeaderBlock() {
            return headerBlock;
        }

        /**
         * Setter for the header block
         * @param headerBlock the header block index
         */
        private void setHeaderBlock(int headerBlock) {
            this.headerBlock = headerBlock;
        }

        /**
         * Gets the user-defined name of the tag.
         * @return the name of the tag
         */
        public String getName() {
            return name;
        }

        /**
         * Sets the user-defined name of the tag.
         * @param name the name of the tag to set
         */
        private void setName(String name) {
            this.name = name;
        }

        /**
         * Gets the data type that this tag indexes. Possible types for MDX
         * tags are <em>character</em>, <em>numeric</em>, and <em>date</em>.
         * @return the data type of the tag
         */
        private IndexDataType getDataType() {
            return dataType;
        }

        /**
         * Sets the data type that this tag indexes. Possible types for MDX
         * tags are <em>character</em>, <em>numeric</em>, and <em>date</em>.
         * @param dataType the data type to set
         */
        private void setDataType(IndexDataType dataType) {
            this.dataType = dataType;
        }

        /**
         * @return the leftTag
         */
        private int getLeftTag() {
            return leftTag;
        }

        /**
         * @param leftTag the leftTag to set
         */
        private void setLeftTag(int leftTag) {
            this.leftTag = leftTag;
        }

        /**
         * @return the rightTag
         */
        private int getRightTag() {
            return rightTag;
        }

        /**
         * @param rightTag the rightTag to set
         */
        private void setRightTag(int rightTag) {
            this.rightTag = rightTag;
        }

        /**
         * @return the backwardTag
         */
        private int getBackwardTag() {
            return backwardTag;
        }

        /**
         * @param backwardTag the backwardTag to set
         */
        private void setBackwardTag(int backwardTag) {
            this.backwardTag = backwardTag;
        }

        /**
         * Gets whether this tag indexes unique values only once.
         * @return the unique flag
         */
        private boolean isUnique() {
            return unique;
        }

        /**
         * Sets whether this tag indexes unique values only once.
         * @param unique the unique flag to set
         */
        private void setUnique(boolean unique) {
            this.unique = unique;
        }

        /**
         * Gets whether this tag indexes data in descending order.
         * @return the descending flag
         */
        private boolean isDescending() {
            return descending;
        }

        /**
         * Sets whether this tag indexes data in descending order.
         * @param descending the descending flag to set
         */
        private void setDescending(boolean descending) {
            this.descending = descending;
        }

        /**
         * Gets the block in the tag where the root block begins.
         * @return the root block
         */
        private int getRootBlock() {
            return rootBlock;
        }

        /**
         * Sets the block in the tag where the root block begins.
         * @param rootBlock the index of the root block
         */
        private void setRootBlock(int rootBlock) {
            this.rootBlock = rootBlock;
        }

        /**
         * Gets the number of blocks that this tag is using.
         * @return the size in blocks
         */
        private int getSizeInBlocks() {
            return sizeInBlocks;
        }

        /**
         * Sets the number of blocks that this tag is using.
         * @param sizeInBlocks the size in blocks to set
         */
        private void setSizeInBlocks(int sizeInBlocks) {
            this.sizeInBlocks = sizeInBlocks;
        }

        /**
         * Gets the key length in bytes for this tag. For dates, this length is
         * <em>8</em>, while for numeric types, this length is <em>12</em>.
         * @return the key length
         */
        private int getKeyLength() {
            return keyLength;
        }

        /**
         * Sets the key length in bytes for this tag. For dates, this length
         * should be set to <em>8</em>, while for numeric types, this length
         * should be set to <em>12</em>.
         * @param keyLength the key length to set
         */
        private void setKeyLength(int keyLength) {
            this.keyLength = keyLength;
        }

        /**
         * Gets the maximum number of keys which can exist in one block. This is
         * inversely proportional to the key length.
         * @return the maximum number of keys per block
         */
        private int getKeysPerBlock() {
            return keysPerBlock;
        }

        /**
         * Sets the maximum number of keys which can exist in one block. This is
         * inversely proportional to the key length.
         * @param keysPerBlock the maximum number of keys per block to set
         */
        private void setKeysPerBlock(int keysPerBlock) {
            this.keysPerBlock = keysPerBlock;
        }

        /**
         * Gets the secondary key type.
         * @return the secondary key type
         */
        private int getSecondaryKeyType() {
            return secondaryKeyType;
        }

        /**
         * Sets the secondary key type.
         * @param secondaryKeyType the secondary key type to set
         */
        private void setSecondaryKeyType(int secondaryKeyType) {
            this.secondaryKeyType = secondaryKeyType;
        }

        /**
         * Gets the key item length.
         * @return the key item length
         */
        private int getKeyItemLength() {
            return keyItemLength;
        }

        /**
         * Sets the key item length.
         * @param keyItemLength the key item length to set
         */
        private void setKeyItemLength(int keyItemLength) {
            this.keyItemLength = keyItemLength;
        }
    }

    /**
     * Opens the given MDX file with a new lock. If locking is enabled, and
     * multiple threads will be accessing this index file, the alternate
     * constructor {@link #open(File, ReentrantLock)} should be used instead.
     *
     * @param mdxFile the file to open
     * @return a reference to the opened file
     * @throws IOException if an I/O error occurs
     */
    public static MDX open(File mdxFile) throws IOException {
        return open(mdxFile, new ReentrantLock());
    }

    /**
     * Opens the given MDX file with the given thread lock. If locking is enabled,
     * this allows multiple threads to share the same lock.
     *
     * @param mdxFile the file to open
     * @param threadLock an existing thread lock
     * @return a reference to the opened file
     * @throws IOException if an I/O error occurs
     */
    public static MDX open(File mdxFile, ReentrantLock threadLock) throws IOException {
        final RandomAccessFile randomAccessFile = new RandomAccessFile(mdxFile,
                "rw" + (DBF.isSynchronousWritesEnabled() ? "s" : ""));
        final MDX mdx = new MDX(mdxFile, randomAccessFile, threadLock);
        mdx.readStructure();
        return mdx;
    }

    public DBFDate reindexDate() {
        return reindexDate;
    }

    /**
     * Reads the structure of the MDX file and caches it in memory. The structure
     * for each tag is loaded into memory.
     * @throws IOException if an I/O error occurs
     */
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
        dbfName = new String(stringBytes, 0, i, StandardCharsets.UTF_8);
        buf.position(20);
        blockSizeMultiplier = buf.getShort() & 0xffff;
        nodeSize = buf.getShort() & 0xffff;
        if (nodeSize != blockSizeMultiplier * BLOCK_SIZE) {
            throw new IOException("Node size mismatch. Got " + nodeSize + "; expected " + (blockSizeMultiplier * BLOCK_SIZE));
        }
        production = buf.get() != 0;
        keysInTag = buf.get() & 0xff;
        if (keysInTag > 48 || keysInTag < 1) {
            throw new IOException("Invalid MDX header. Entries in tag=" + keysInTag);
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
            tags[tagIndex].setHeaderBlock((int) (buf.getInt() & 0xffffffffL));
            for (i = 0; i < 10; i++) {
                b = buf.get();
                if (b == 0) {
                    break;
                }

                stringBytes[i] = b;
            }
            tags[tagIndex].setName(new String(stringBytes, 0, i, StandardCharsets.UTF_8));
            if (i < 10) {
                buf.position(buf.position() + 10 - i);
            }

            keyFormat = buf.get();
            tags[tagIndex].setDescending((keyFormat & 0x08) == 0x08);
            tags[tagIndex].setUnique((keyFormat & 0x40) == 0x40);
            tags[tagIndex].setLeftTag(buf.get() & 0xff);
            tags[tagIndex].setRightTag(buf.get() & 0xff);
            tags[tagIndex].setBackwardTag(buf.get() & 0xff);
            buf.position(buf.position() + 1);
            keyType = buf.get();
            switch (keyType) {
                case 'C':
                    tags[tagIndex].setDataType(IndexDataType.CHARACTER);
                    break;
                case 'N':
                    tags[tagIndex].setDataType(IndexDataType.NUMERIC);
                    break;
                case 'D':
                    tags[tagIndex].setDataType(IndexDataType.CHARACTER);
                    break;
                default:
                    throw new IOException("Unknown key type: " + (char) keyType);
            }

            // Tag header
            channel.position(tags[tagIndex].getHeaderBlock() * BLOCK_SIZE);
            buf.position(0);
            buf.limit(BLOCK_SIZE);
            while (buf.hasRemaining()) {
                if (channel.read(buf) == -1) {
                    throw new IOException("EOF while reading tag headers");
                }
            }
            buf.position(0);

            tags[tagIndex].setRootBlock((int) (buf.getInt() & 0xffffffffL));
            tags[tagIndex].setSizeInBlocks((int) (buf.getInt() & 0xffffffffL));
            keyFormatInHeader = buf.get();
            if (keyFormatInHeader != keyFormat) {
                throw new IOException("Key format byte in header != key format byte in tag descriptor: " + keyFormat + " != " + keyFormatInHeader);
            }
            keyTypeInHeader = buf.get();
            if (keyTypeInHeader != keyType) {
                throw new IOException("Key type byte in header != key type byte in tag descriptor: " + keyType + " != " + keyTypeInHeader);
            }

            buf.position(buf.position() + 2);
            tags[tagIndex].setKeyLength(buf.getShort() & 0xffff);
            tags[tagIndex].setKeysPerBlock(buf.getShort() & 0xffff);
            tags[tagIndex].setSecondaryKeyType(buf.getShort() & 0xffff);
            tags[tagIndex].setKeyItemLength(buf.getShort() & 0xffff);
            buf.position(buf.position() + 3);
            if ((buf.get() != 0) != tags[tagIndex].isUnique()) {
                throw new IOException("Unique flag in header != unique flag in tag descriptor: Key Format=" + keyFormat);
            }
        }
    }

    /**
     * Moves to the given block number and reads the block into a memory buffer.
     * Block numbers start at index <em>1</em>. Blocks start at block boundaries
     * and will span multiple blocks, based on the block size.
     *
     * @param blockNumber the block number to move to
     * @throws IOException if an I/O error occurs
     */
    public void gotoBlock(int blockNumber) throws IOException {
        if (this.blockNumber != blockNumber) {
            if (blockNumber > numberOfBlocks) {
                throw new IllegalArgumentException("Block does not exist: " + blockNumber);
            } else if (blockNumber <= 0) {
                throw new IllegalArgumentException("Invalid block number: " + blockNumber);
            }
            this.blockNumber = blockNumber;
            this.keyIndex = 0;
            readBlock();
        }
    }

    private int keysInBlock() {
        return buf.getInt(0);
    }

    private int previousBlock(int key, Tag tag) {
        return buf.getInt(4 + key * keyRecordSize(tag));
    }

    private int previousBlock() {
        return previousBlock(this.keyIndex, this.tag);
    }

    public int find(Object value) throws IOException {
        return find(value, this.tag, this.tag.getRootBlock());
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
            if (tag.getName().equalsIgnoreCase(tagName)) {
                return find(value, tag, tag.getRootBlock());
            }
        }

        throw new IOException(String.format("Tag [%s] not found in MDX [%s]",
                tagName, mdxFile.getName()));
    }

    private static int keyRecordSize(Tag tag) {
        return (int) Math.ceil(tag.getKeyLength() / 4f) * 4 + 4;
    }

    /**
     * Fetches either the next block pointer or the record number for the given
     * key which exists in <code>buf</code> after a call to {@link #readBlock}.
     * The next block pointer is returned if the current block is not a leaf, and
     * the record number is returned if the current block is a leaf.
     *
     * @param key the zero based key within the block
     * @return the next block number, or the record number if the current block
     * is a leaf
     */
    private int nextBlockOrRecordNumber(int key, Tag tag) {
        return buf.getInt(8 + key * keyRecordSize(tag));
    }

    private int nextBlockOrRecordNumber() {
        return nextBlockOrRecordNumber(this.keyIndex, this.tag);
    }

    private int find(Object value, Tag tag, int blockNumber) throws IOException {
        gotoBlock(blockNumber);
        final int keysInBlock = keysInBlock();
        final boolean leaf = nextBlockOrRecordNumber(0, tag) != 0;

        int nextBlockOrRecordNumber;
        int compareResult = -1;
        for (int i = 0; i < keysInBlock && compareResult < 0; i++) {
            nextBlockOrRecordNumber = nextBlockOrRecordNumber(i, tag);
            switch (tag.getDataType()) {
                case DATE: {
                    DBFDate date = (DBFDate) value;
                    value = date.dtos();
                    // fallthrough
                }
                default:
                case CHARACTER:
                    ByteBuffer bytes = readKey(i, -1);
                    // Pad the search key with spaces so that the length is
                    // equal to the MDX key length
                    StringBuilder sb = new StringBuilder(tag.getKeyLength());
                    sb.append(value.toString());
                    while (sb.length() < tag.getKeyLength()) {
                        sb.append(' ');
                    }
                    String keyValue = new String(bytes.array(), bytes.arrayOffset() + bytes.position(), bytes.limit() - bytes.position(), StandardCharsets.UTF_8);
                    compareResult = keyValue.compareTo(sb.toString());
                    break;
                case NUMERIC:
                    BigDecimal searchTerm = null;
                    if (value instanceof BigDecimal) {
                        searchTerm = (BigDecimal) value;
                    } else if (value instanceof BigInteger) {
                        searchTerm = new BigDecimal((BigInteger) value);
                    } else if (value instanceof Long) {
                        searchTerm = BigDecimal.valueOf((Long) value);
                    } else if (value instanceof Number) {
                        searchTerm = new BigDecimal(((Number) value).doubleValue());
                    }
                    final BigDecimal storedNumber;
                    if (tag.keyLength == 8) {
                        // float64 le
                        bytes = readKey(i, 8);
                        storedNumber = BigDecimal.valueOf(bytes.getDouble());
                    } else if (tag.keyLength == 12) {
                        // dBase 12-byte numeric encoding
                        bytes = readKey(i, 12);
                        storedNumber = BigDecimal.valueOf(decodeNumeric(bytes));
                    } else {
                        // Unknown
                        storedNumber = BigDecimal.ZERO;
                    }

                    compareResult = storedNumber.compareTo(searchTerm);
                    break;
            }

            if (leaf) {
                // A leaf node can either have a match or fail to match
                if (compareResult == 0) {
                    // Search term match
                    return nextBlockOrRecordNumber;
                } else if (compareResult > 0) {
                    break;
                }
            } else {
                // A root or inner node will follow the highest record that is less than a match
                if (compareResult > 0) {
                    if (i == 0) {
                        break;
                    }
                    return find(value, tag, nextBlockOrRecordNumber(i - 1, tag));
                }
            }
        }

        return DBF.RECORD_NUMBER_EOF;
    }

    static byte SIGN_NEGATIVE_WITH_DECIMAL = (byte) 0xd1;
    static byte SIGN_NEGATIVE_WITHOUT_DECIMAL = (byte) 0xa9;
    static byte SIGN_POSITIVE_WITH_DECIMAL = (byte) 0x51;
    static byte SIGN_POSITIVE_WITHOUT_DECIMAL = (byte) 0x29;
    static byte SIGN_ZERO = (byte) 0x10;

    /**
     * Decodes dBase MDX custom float64 encoding.
     * @param bb a byte buffer, positioned at the start of the raw encoded bytes.
     * @return the decoded double
     */
    static double decodeNumeric(ByteBuffer bb) {
        if (bb.limit() - bb.position() < 12) {
            return 0d;
        }

        byte size = bb.get();
        byte sign = bb.get();

        if (sign == SIGN_ZERO) {
            return 0d;
        }

        int digitsLeftOfDecimal = size - 0x34;

        boolean negative =
                sign == SIGN_NEGATIVE_WITHOUT_DECIMAL
                || sign == SIGN_NEGATIVE_WITH_DECIMAL;

        long lv = 0L;
        byte b;
        for (int digitCount = 0; digitCount < 18; digitCount += 2) {
            b = bb.get();
            lv *= 100;

            if (b == 0) {
                continue;
            }

            int digit1 = (b & 0xf0) >> 4;
            int digit2 = b & 0x0f;
            lv += digit1 * 10;
            lv += digit2;
        }

        double dv = lv / Math.pow(10, 18 - digitsLeftOfDecimal);
        if (negative) {
            dv *= -1;
        }

        return dv;
    }

    /**
     * Reads the data for the key with the given index. {@code length} bytes will be read,
     * or if the length key is less than or equal to zero, reading will stop at the maximum
     * of the length of the key's data, or the first encountered null byte.
     * @param keyIndex the index of the key, within the current node
     * @param length the length to read, or {@code -1} to read a variable length string
     * @return a byte buffer with the correct byte order and limit, and the position set to zero
     */
    private ByteBuffer readKey(int keyIndex, int length) {
        int offset = 12 + keyIndex * keyRecordSize(tag);
        if (length > 0) {
            return (ByteBuffer) buf
                    .duplicate()
                    .position(offset)
                    .limit(offset + length);
        }

        buf.position(offset);
        while (buf.position() < offset + tag.keyLength) {
            if (buf.get() == 0) {
                break;
            }
        }

        return (ByteBuffer) buf
                .duplicate()
                .position(offset)
                .limit(buf.position());
    }

    /**
     * Reads the current block into the internal buffer. The node is not decoded into
     * memory; each method that uses node data will decode the fields on demand from
     * the buffer.
     *
     * @throws IOException if an I/O error occurs
     */
    public void readBlock() throws IOException {
        if (blockNumber <= 0) {
            throw new IllegalStateException("Invalid block number: " + blockNumber);
        }
        FileChannel channel = randomAccessFile.getChannel();
        buf.position(0);
        buf.limit(nodeSize);
        channel.position(BLOCK_SIZE * (long) blockNumber);
        while (buf.hasRemaining()) {
            if (channel.read(buf) == -1) {
                throw new IOException("EOF while reading block " + blockNumber);
            }
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
        out.printf("Block Size:      %17d\n", nodeSize);
        out.printf("Block Size Mult: %17d\n", blockSizeMultiplier);
        out.printf("Entries In Tag:  %17d\n", keysInTag);
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

            out.printf(" Name:           %17s\n", tags[i].getName());
            out.printf(" Descending:     %17b\n", tags[i].isDescending());
            out.printf(" Unique:         %17b\n", tags[i].isUnique());
            out.printf(" Header Block:   %17s\n", tags[i].getHeaderBlock());
            out.printf(" Root Block:     %17s\n", tags[i].getRootBlock());
            out.printf(" Size In Blocks  %17s\n", tags[i].getSizeInBlocks());
            out.printf(" Left Tag:       %17s\n", tags[i].getLeftTag());
            out.printf(" Right Tag:      %17s\n", tags[i].getRightTag());
            out.printf(" Backward Tag:   %17s\n", tags[i].getBackwardTag());
            out.printf(" Key Length:     %17s\n", tags[i].getKeyLength());
            out.printf(" Keys Per Block  %17s\n", tags[i].getKeysPerBlock());
            out.printf(" 2nd Key Type:   %17s\n", tags[i].getSecondaryKeyType());
            out.printf(" Key Item Length:%17s\n", tags[i].getKeyItemLength());
        }

        out.println("----------------------------------");
    }

    @Override
    public int next() throws IOException {
        final int keysInBlock = keysInBlock();
        boolean leaf = nextBlockOrRecordNumber() != 0;
        while (true) {
            if (leaf) {
                if (this.keyIndex >= keysInBlock - 1) {
                    return DBF.RECORD_NUMBER_EOF;
                } else {
                    this.keyIndex++;
                    return nextBlockOrRecordNumber();
                }
            }
            gotoBlock(nextBlockOrRecordNumber());
            leaf = nextBlockOrRecordNumber() != 0;
        }
    }

    @Override
    public int prev() throws IOException {
        boolean leaf = nextBlockOrRecordNumber() != 0;
        while (true) {
            if (leaf) {
                if (this.keyIndex == 0) {
                    return DBF.RECORD_NUMBER_BOF;
                } else {
                    this.keyIndex--;
                    return nextBlockOrRecordNumber();
                }
            }
            gotoBlock(previousBlock());
            leaf = nextBlockOrRecordNumber() != 0;
        }
    }

    /**
     * Sets the primary (aka master) tag.
     * @param tag the tag to set as primary
     */
    public void setTag(Tag tag) {
        this.tag = tag;
    }

    /**
     * Sets the primary (aka master) tag, by name. If a tag with the given name
     * exists in the index, it will be returned, otherwise, an empty optional will be returned.
     *
     * @param tagName the name of the tag
     * @return the optional tag that was set
     */
    public Optional<Tag> setTag(String tagName) {
        for (Tag t : tags) {
            if (t.getName().equalsIgnoreCase(tagName)) {
                this.tag = t;
                return Optional.of(tag);
            }
        }

        return Optional.empty();
    }

    /**
     * Loads the first block and record for the primary tag.
     *
     * @throws IOException if an I/O error occurs
     */
    public int gotoTop() throws IOException {
        gotoBlock(Objects.requireNonNull(this.tag, "tag is not set").getRootBlock());
        while (previousBlock() != 0) {
            gotoBlock(nextBlockOrRecordNumber());
        }
        return nextBlockOrRecordNumber();
    }

    /**
     * Goes to the record that represents the last record in the index.
     *
     * @throws IOException if an I/O error occurs
     */
    public int gotoBottom() throws IOException {
        gotoTop();
        while (next() != DBF.RECORD_NUMBER_EOF);
        return nextBlockOrRecordNumber();
    }
}
