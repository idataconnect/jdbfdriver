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
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Deque;
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

    /** Stack of (blockNumber, keyIndex) pairs for multi-level tree traversal. */
    private final Deque<int[]> traversalStack = new ArrayDeque<>();

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
        /** Number of levels in the B-tree for this tag. */
        int levels = 1;

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
        // Leaf detection: if the child pointer of entry 0 is zero, it's a leaf
        final boolean leaf = previousBlock(0, tag) == 0;

        int compareResult = -1;
        for (int i = 0; i < keysInBlock && compareResult < 0; i++) {
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
                    // Search term match — record number is in nextBlockOrRecordNumber
                    return nextBlockOrRecordNumber(i, tag);
                } else if (compareResult > 0) {
                    break;
                }
            } else {
                // Internal node: keys represent max values in subtrees.
                // previousBlock(i) is the child pointer for key[i]'s subtree.
                if (compareResult >= 0) {
                    // key[i] >= searchValue — descend into this child
                    return find(value, tag, previousBlock(i, tag));
                }
            }
        }

        // For internal nodes: if searchValue > all keys, descend into the last child
        if (!leaf && keysInBlock > 0) {
            return find(value, tag, previousBlock(keysInBlock - 1, tag));
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
        boolean leaf = previousBlock(0, tag) == 0;
        if (leaf) {
            if (this.keyIndex >= keysInBlock - 1) {
                // Try to pop up to a parent
                while (!traversalStack.isEmpty()) {
                    int[] parent = traversalStack.pop();
                    gotoBlock(parent[0]);
                    this.keyIndex = parent[1];
                    if (this.keyIndex < keysInBlock() - 1) {
                        this.keyIndex++;
                        // Descend leftmost from this new child
                        return descendToLeftmostLeaf(previousBlock());
                    }
                    // else keep popping
                }
                return DBF.RECORD_NUMBER_EOF;
            } else {
                this.keyIndex++;
                return nextBlockOrRecordNumber();
            }
        } else {
            // Internal node — descend into next child
            this.keyIndex++;
            if (this.keyIndex < keysInBlock) {
                return descendToLeftmostLeaf(previousBlock());
            }
            // Pop up
            while (!traversalStack.isEmpty()) {
                int[] parent = traversalStack.pop();
                gotoBlock(parent[0]);
                this.keyIndex = parent[1];
                if (this.keyIndex < keysInBlock() - 1) {
                    this.keyIndex++;
                    return descendToLeftmostLeaf(previousBlock());
                }
            }
            return DBF.RECORD_NUMBER_EOF;
        }
    }

    @Override
    public int prev() throws IOException {
        boolean leaf = previousBlock(0, tag) == 0;
        if (leaf) {
            if (this.keyIndex == 0) {
                // Try to pop to parent
                while (!traversalStack.isEmpty()) {
                    int[] parent = traversalStack.pop();
                    gotoBlock(parent[0]);
                    this.keyIndex = parent[1];
                    if (this.keyIndex > 0) {
                        this.keyIndex--;
                        return descendToRightmostLeaf(previousBlock());
                    }
                }
                return DBF.RECORD_NUMBER_BOF;
            } else {
                this.keyIndex--;
                return nextBlockOrRecordNumber();
            }
        } else {
            // Internal node — shouldn't normally be here, but handle it
            if (this.keyIndex > 0) {
                this.keyIndex--;
                return descendToRightmostLeaf(previousBlock());
            }
            while (!traversalStack.isEmpty()) {
                int[] parent = traversalStack.pop();
                gotoBlock(parent[0]);
                this.keyIndex = parent[1];
                if (this.keyIndex > 0) {
                    this.keyIndex--;
                    return descendToRightmostLeaf(previousBlock());
                }
            }
            return DBF.RECORD_NUMBER_BOF;
        }
    }

    /**
     * Descends from the given child block to the leftmost leaf entry.
     * Pushes traversal state onto the stack along the way.
     *
     * @param childBlock the block to start descending from
     * @return the record number of the leftmost leaf entry
     * @throws IOException if an I/O error occurs
     */
    private int descendToLeftmostLeaf(int childBlock) throws IOException {
        traversalStack.push(new int[] { blockNumber, keyIndex });
        gotoBlock(childBlock);
        while (previousBlock(0, tag) != 0) {
            traversalStack.push(new int[] { blockNumber, 0 });
            gotoBlock(previousBlock(0, tag));
        }
        this.keyIndex = 0;
        return nextBlockOrRecordNumber();
    }

    /**
     * Descends from the given child block to the rightmost leaf entry.
     * Pushes traversal state onto the stack along the way.
     *
     * @param childBlock the block to start descending from
     * @return the record number of the rightmost leaf entry
     * @throws IOException if an I/O error occurs
     */
    private int descendToRightmostLeaf(int childBlock) throws IOException {
        traversalStack.push(new int[] { blockNumber, keyIndex });
        gotoBlock(childBlock);
        while (previousBlock(0, tag) != 0) {
            int last = keysInBlock() - 1;
            traversalStack.push(new int[] { blockNumber, last });
            gotoBlock(previousBlock(last, tag));
        }
        this.keyIndex = keysInBlock() - 1;
        return nextBlockOrRecordNumber();
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
        traversalStack.clear();
        gotoBlock(Objects.requireNonNull(this.tag, "tag is not set").getRootBlock());
        // Descend to leftmost leaf via child pointers
        while (previousBlock(0, tag) != 0) {
            traversalStack.push(new int[] { blockNumber, 0 });
            gotoBlock(previousBlock(0, tag));
        }
        this.keyIndex = 0;
        return nextBlockOrRecordNumber();
    }

    /**
     * Goes to the record that represents the last record in the index.
     *
     * @throws IOException if an I/O error occurs
     */
    public int gotoBottom() throws IOException {
        traversalStack.clear();
        gotoBlock(Objects.requireNonNull(this.tag, "tag is not set").getRootBlock());
        // Descend to rightmost leaf via child pointers
        while (previousBlock(0, tag) != 0) {
            int last = keysInBlock() - 1;
            traversalStack.push(new int[] { blockNumber, last });
            gotoBlock(previousBlock(last, tag));
        }
        this.keyIndex = keysInBlock() - 1;
        return nextBlockOrRecordNumber();
    }

    // -----------------------------------------------------------------------
    // Write support
    // -----------------------------------------------------------------------

    /**
     * Creates a new, empty MDX file.
     *
     * @param mdxFile the file to create
     * @param dbfName the name of the associated DBF file (up to 15 characters)
     * @return a reference to the created MDX
     * @throws IOException if an I/O error occurs
     */
    public static MDX create(File mdxFile, String dbfName) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(mdxFile,
                "rw" + (DBF.isSynchronousWritesEnabled() ? "s" : ""));
        MDX mdx = new MDX(mdxFile, raf, new ReentrantLock());
        mdx.dbfName = dbfName;
        mdx.blockSizeMultiplier = 2;
        mdx.nodeSize = 2 * BLOCK_SIZE;
        mdx.production = false;
        mdx.keysInTag = 48;
        mdx.tagLength = 32;
        mdx.tagsInUse = 0;
        // Block 0 is the MDX header (first 544 bytes + tag descriptors).
        // Tag descriptors: up to 48 tags × 32 bytes = 1536 bytes + 544 = 2080 bytes.
        // ceil(2080/512) = 5 blocks consumed by the header area (blocks 0-4).
        // Block 5 onwards are used for tag-header blocks and b-tree data.
        mdx.numberOfBlocks = 5;
        mdx.firstFreeBlock = 0;
        mdx.availableBlock = 5;
        mdx.tags = new Tag[0];
        mdx.writeHeader();
        return mdx;
    }

    /**
     * Adds a new tag (index) to this MDX file.
     *
     * @param tagName the name of the tag (up to 10 characters, will be upper-cased)
     * @param expression the key expression (field name or xBase expression)
     * @param dataType the data type of the key
     * @param unique whether the index should enforce unique keys
     * @param descending whether the index should be in descending order
     * @throws IOException if an I/O error occurs
     */
    public void addTag(String tagName, String expression, IndexDataType dataType,
            boolean unique, boolean descending) throws IOException {
        if (tagsInUse >= keysInTag) {
            throw new IOException("MDX is full (" + keysInTag + " tags maximum)");
        }

        Tag t = new Tag();
        t.setName(tagName.toUpperCase());
        t.setDataType(dataType);
        t.setUnique(unique);
        t.setDescending(descending);

        // Determine key length from data type
        switch (dataType) {
            case NUMERIC:
                t.setKeyLength(12); // dBase custom BCD encoding
                break;
            case DATE:
                t.setKeyLength(8);  // YYYYMMDD as string
                break;
            default: // CHARACTER
                // Use expression length, but at least 1 and at most 240
                t.setKeyLength(Math.max(1, Math.min(240, expression.length())));
                break;
        }
        int krs = keyRecordSize(t);
        // keysPerBlock: the node holds [count:4][leftPtr:4] + keysPerBlock * krs <= nodeSize
        t.setKeysPerBlock((nodeSize - 8) / krs);
        t.setKeyItemLength(krs);

        // Allocate a header block for this tag
        int headerBlock = allocateBlock();
        t.setHeaderBlock(headerBlock);

        // Allocate the initial (empty) root block
        int rootBlock = allocateBlock();
        t.setRootBlock(rootBlock);
        t.setSizeInBlocks(1);

        // Write the empty root block (all zeros = 0 keys)
        buf.position(0);
        buf.limit(nodeSize);
        Arrays.fill(buf.array(), 0, nodeSize, (byte) 0);
        writeBlock(rootBlock);

        // Write tag header block
        writeTagHeader(t, expression);

        // Append tag to the array and write updated MDX header + tag descriptors
        Tag[] newTags = Arrays.copyOf(tags, tagsInUse + 1);
        newTags[tagsInUse] = t;
        tags = newTags;
        tagsInUse++;
        writeHeader();
    }

    /**
     * Inserts a key/record-number pair into the currently active tag.
     *
     * @param key the key value
     * @param recordNumber the record number in the DBF
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void insert(Object key, int recordNumber) throws IOException {
        if (tag == null) {
            throw new IllegalStateException("No tag is set");
        }
        byte[] encodedKey = encodeKey(key, tag);
        SplitResult split = insertIntoBlock(tag.getRootBlock(), encodedKey, recordNumber, tag.levels);
        if (split != null) {
            // Root was split — create a new root with two entries
            int newRoot = allocateBlock();
            int oldRoot = tag.getRootBlock();
            int krs = keyRecordSize(tag);
            buf.position(0);
            buf.limit(nodeSize);
            Arrays.fill(buf.array(), 0, nodeSize, (byte) 0);
            // Entry 0: child=oldRoot (left subtree), key=promotedKey (max of left)
            buf.putInt(0, 2);                  // count = 2
            buf.putInt(4, oldRoot);            // entry[0].childPtr = left subtree
            // entry[0].record = 0 (internal node)
            buf.position(12);
            buf.put(split.promotedKey);
            // Entry 1: child=rightBlock (right subtree), key=max key of right subtree
            buf.putInt(4 + krs, split.rightBlock); // entry[1].childPtr = right subtree
            // entry[1].record = 0 (internal node)
            buf.position(12 + krs);
            buf.put(split.rightMaxKey);
            writeBlock(newRoot);
            tag.setRootBlock(newRoot);
            tag.levels++;
            writeTagHeader(tag, null);
            writeHeader();
        }
    }

    /**
     * Deletes a key/record-number pair from the currently active tag.
     * This implementation performs a simple search-and-remove on the leaf node.
     * It does not rebalance the tree.
     *
     * @param key the key value
     * @param recordNumber the record number in the DBF
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void delete(Object key, int recordNumber) throws IOException {
        if (tag == null) {
            return;
        }
        byte[] encodedKey = encodeKey(key, tag);
        deleteFromBlock(tag.getRootBlock(), encodedKey, recordNumber, tag.levels);
    }

    /**
     * Recursively inserts a key into the B-tree rooted at {@code blockNum}.
     *
     * @param blockNum the block to insert into
     * @param key the encoded key bytes
     * @param recordNumber the record number
     * @param level the level in the tree (1 = leaf)
     * @return a SplitResult if the block was split, or null if no split occurred
     */
    private SplitResult insertIntoBlock(int blockNum, byte[] key, int recordNumber, int level)
            throws IOException {
        gotoBlock(blockNum);
        int count = keysInBlock();
        int krs = keyRecordSize(tag);
        boolean isLeaf = (level <= 1);

        // Find insertion position (sorted order)
        int pos = 0;
        while (pos < count) {
            byte[] storedKey = readRawKey(pos);
            if (compareKeys(key, storedKey) < 0) {
                break;
            }
            pos++;
        }

        if (isLeaf) {
            // Leaf insert: childPtr=0, recordNum=actual record
            if (count < tag.getKeysPerBlock()) {
                insertKeyAt(pos, 0, recordNumber, key, count, krs);
                buf.putInt(0, count + 1);
                writeBlock(blockNum);
                return null;
            } else {
                return splitBlock(blockNum, pos, 0, recordNumber, key, count, krs, true);
            }
        } else {
            // Internal node: descend into correct child
            // Each key[i] represents the max value in the subtree at previousBlock(i).
            // If pos < count, searchKey < key[pos], so descend into previousBlock(pos).
            // If pos == count, searchKey > all keys, descend into previousBlock(count-1)
            // and we may need to update that entry's key afterwards.
            int childIdx = (pos < count) ? pos : count - 1;
            int childBlock = previousBlock(childIdx, tag);
            SplitResult childSplit = insertIntoBlock(childBlock, key, recordNumber, level - 1);

            // Re-read this block (since gotoBlock may have changed the buffer)
            gotoBlock(blockNum);
            count = keysInBlock();

            if (childSplit == null) {
                // If we inserted into the last child and the new key is larger
                // than the current max, update the key in the parent
                if (pos == count && compareKeys(key, readRawKey(count - 1)) > 0) {
                    buf.position(12 + (count - 1) * krs);
                    buf.put(key, 0, key.length);
                    writeBlock(blockNum);
                }
                return null;
            }

            // Child was split — update the existing entry's key to the promoted key
            // (max of left child) and insert a new entry for the right child
            buf.position(12 + childIdx * krs);
            buf.put(childSplit.promotedKey, 0, childSplit.promotedKey.length);

            // Insert new entry for right child AFTER childIdx
            int insertPos = childIdx + 1;
            if (count < tag.getKeysPerBlock()) {
                insertKeyAt(insertPos, childSplit.rightBlock, 0, childSplit.rightMaxKey, count, krs);
                buf.putInt(0, count + 1);
                writeBlock(blockNum);
                return null;
            } else {
                return splitBlock(blockNum, insertPos, childSplit.rightBlock, 0,
                        childSplit.rightMaxKey, count, krs, false);
            }
        }
    }

    /**
     * Inserts a key at position {@code pos} within the current buffer,
     * shifting existing entries right.
     *
     * @param pos the position to insert at
     * @param childPtr the child pointer (0 for leaf entries)
     * @param recNum the record number (0 for internal entries)
     * @param key the key data bytes
     * @param count the current number of entries
     * @param krs the key record size (stride)
     */
    private void insertKeyAt(int pos, int childPtr, int recNum, byte[] key, int count, int krs) {
        // Shift entries [pos..count-1] one slot to the right
        for (int i = count - 1; i >= pos; i--) {
            int srcOff = 4 + i * krs;
            int dstOff = 4 + (i + 1) * krs;
            for (int b = 0; b < krs; b++) {
                buf.put(dstOff + b, buf.get(srcOff + b));
            }
        }
        // Write the new entry
        buf.putInt(4 + pos * krs, childPtr);
        buf.putInt(8 + pos * krs, recNum);
        buf.position(12 + pos * krs);
        buf.put(key, 0, key.length);
    }

    /**
     * Splits a full block and returns the promoted middle key.
     * The left half stays in {@code blockNum}; the right half goes into a new block.
     *
     * @param blockNum the block being split
     * @param insertPos where the new entry should be inserted
     * @param childPtr child pointer for the new entry
     * @param recNum record number for the new entry
     * @param newKey key data for the new entry
     * @param count current number of entries
     * @param krs key record size (stride)
     * @param isLeaf whether this is a leaf node
     * @return the split result containing promoted key, right max key, and right block number
     */
    private SplitResult splitBlock(int blockNum, int insertPos, int childPtr, int recNum,
            byte[] newKey, int count, int krs, boolean isLeaf) throws IOException {
        // Materialise all count+1 entries (existing + the new one to insert)
        int total = count + 1;
        int[] childPtrs = new int[total];
        int[] recNums = new int[total];
        byte[][] keys = new byte[total][newKey.length];

        for (int i = 0, src = 0; i < total; i++) {
            if (i == insertPos) {
                childPtrs[i] = childPtr;
                recNums[i] = recNum;
                System.arraycopy(newKey, 0, keys[i], 0, newKey.length);
            } else {
                childPtrs[i] = buf.getInt(4 + src * krs);
                recNums[i] = buf.getInt(8 + src * krs);
                buf.position(12 + src * krs);
                buf.get(keys[i], 0, newKey.length);
                src++;
            }
        }

        int mid = total / 2;
        byte[] promotedKey = Arrays.copyOf(keys[mid], keys[mid].length);
        byte[] rightMaxKey = Arrays.copyOf(keys[total - 1], keys[total - 1].length);

        // Write left block (entries 0..mid)
        Arrays.fill(buf.array(), 0, nodeSize, (byte) 0);
        int leftCount = mid + 1;
        for (int i = 0; i < leftCount; i++) {
            buf.putInt(4 + i * krs, childPtrs[i]);
            buf.putInt(8 + i * krs, recNums[i]);
            buf.position(12 + i * krs);
            buf.put(keys[i]);
        }
        buf.putInt(0, leftCount);
        writeBlock(blockNum);

        // Write right block (entries mid+1..total-1)
        int rightBlock = allocateBlock();
        Arrays.fill(buf.array(), 0, nodeSize, (byte) 0);
        int rightStart = mid + 1;
        int rightCount = total - rightStart;
        for (int i = 0; i < rightCount; i++) {
            buf.putInt(4 + i * krs, childPtrs[rightStart + i]);
            buf.putInt(8 + i * krs, recNums[rightStart + i]);
            buf.position(12 + i * krs);
            buf.put(keys[rightStart + i]);
        }
        buf.putInt(0, rightCount);
        writeBlock(rightBlock);

        return new SplitResult(promotedKey, rightMaxKey, rightBlock);
    }

    /**
     * Recursively deletes a key from the B-tree.
     * Does not rebalance; simply removes the entry from the leaf.
     */
    private boolean deleteFromBlock(int blockNum, byte[] key, int recordNumber, int level)
            throws IOException {
        gotoBlock(blockNum);
        int count = keysInBlock();
        int krs = keyRecordSize(tag);
        boolean isLeaf = (level <= 1);

        for (int i = 0; i < count; i++) {
            byte[] storedKey = readRawKey(i);
            int cmp = compareKeys(key, storedKey);
            if (cmp < 0) {
                if (!isLeaf) {
                    int childBlock = previousBlock(i, tag);
                    return deleteFromBlock(childBlock, key, recordNumber, level - 1);
                }
                return false; // key not found
            }
            if (cmp == 0) {
                if (isLeaf) {
                    int ptr = buf.getInt(8 + i * krs);
                    if (ptr == recordNumber) {
                        // Shift entries left (each entry spans krs bytes starting at offset 4+i*krs)
                        for (int j = i; j < count - 1; j++) {
                            int srcOff = 4 + (j + 1) * krs;
                            int dstOff = 4 + j * krs;
                            for (int b = 0; b < krs; b++) {
                                buf.put(dstOff + b, buf.get(srcOff + b));
                            }
                        }
                        // Zero out the last slot
                        int lastOff = 4 + (count - 1) * krs;
                        Arrays.fill(buf.array(), lastOff, lastOff + krs, (byte) 0);
                        buf.putInt(0, count - 1);
                        writeBlock(blockNum);
                        return true;
                    }
                } else {
                    // key matches entry[i], descend into child[i]
                    int childBlock = previousBlock(i, tag);
                    return deleteFromBlock(childBlock, key, recordNumber, level - 1);
                }
            }
        }
        if (!isLeaf && count > 0) {
            // key > all entries, descend into last child
            int childBlock = previousBlock(count - 1, tag);
            return deleteFromBlock(childBlock, key, recordNumber, level - 1);
        }
        return false;
    }

    /**
     * Reads the raw key bytes for slot {@code i} from the current buffer.
     */
    private byte[] readRawKey(int i) {
        int keyLen = tag.getKeyLength();
        byte[] k = new byte[keyLen];
        buf.position(12 + i * keyRecordSize(tag));
        buf.get(k);
        return k;
    }

    /**
     * Compares two keys lexicographically (unsigned byte comparison).
     */
    private static int compareKeys(byte[] a, byte[] b) {
        for (int i = 0; i < a.length && i < b.length; i++) {
            int diff = (a[i] & 0xff) - (b[i] & 0xff);
            if (diff != 0) return diff;
        }
        return a.length - b.length;
    }

    /**
     * Encodes a Java value into the raw byte representation used by the MDX B-tree.
     */
    private static byte[] encodeKey(Object value, Tag tag) {
        byte[] bytes = new byte[tag.getKeyLength()];
        switch (tag.getDataType()) {
            case CHARACTER:
            case DATE: {
                String s = (value instanceof DBFDate)
                        ? ((DBFDate) value).dtos()
                        : (value == null ? "" : value.toString());
                byte[] src = s.getBytes(StandardCharsets.UTF_8);
                int len = Math.min(src.length, bytes.length);
                System.arraycopy(src, 0, bytes, 0, len);
                // Pad remainder with spaces (matches how dBase stores character keys)
                Arrays.fill(bytes, len, bytes.length, (byte) ' ');
                break;
            }
            case NUMERIC: {
                double d;
                if (value instanceof Number) {
                    d = ((Number) value).doubleValue();
                } else {
                    d = Double.parseDouble(value.toString());
                }
                encodeNumeric(d, bytes);
                break;
            }
        }
        return bytes;
    }

    /**
     * Encodes a double value into the 12-byte dBase BCD format used for MDX numeric keys.
     *
     * @param d the value to encode
     * @param out a 12-byte array to receive the encoded value
     */
    static void encodeNumeric(double d, byte[] out) {
        if (out.length < 12) {
            throw new IllegalArgumentException("Output array must be at least 12 bytes");
        }
        Arrays.fill(out, (byte) 0);
        if (d == 0) {
            out[1] = SIGN_ZERO;
            return;
        }
        boolean negative = d < 0;
        if (negative) d = -d;

        // Count digits to left of decimal to compute size byte
        int digitsLeft = (d >= 1) ? (int) Math.floor(Math.log10(d)) + 1 : 0;
        out[0] = (byte) (0x34 + digitsLeft);
        out[1] = negative ? SIGN_NEGATIVE_WITHOUT_DECIMAL : SIGN_POSITIVE_WITHOUT_DECIMAL;

        // Round to 18 significant digits and encode as BCD
        // Scale so integer part holds all 18 digits
        double scaled = d * Math.pow(10, 18 - digitsLeft);
        long lv = Math.round(scaled);

        // Encode 18 digits as 9 BCD bytes (2 digits per byte), stored in out[2..11]
        // Digits are stored most-significant first
        String digits = String.format("%018d", lv);
        for (int i = 0; i < 9; i++) {
            int d1 = digits.charAt(i * 2) - '0';
            int d2 = digits.charAt(i * 2 + 1) - '0';
            out[2 + i] = (byte) ((d1 << 4) | d2);
        }
    }

    /**
     * Allocates a new block. Blocks are addressed relative to BLOCK_SIZE(512)
     * but a "block" in the B-tree occupies {@code blockSizeMultiplier} physical blocks.
     *
     * @return the block number of the newly allocated block
     * @throws IOException if an I/O error occurs
     */
    private int allocateBlock() throws IOException {
        int blockNum = availableBlock;
        availableBlock += blockSizeMultiplier;
        numberOfBlocks = availableBlock;
        // Zero-fill the new block on disk
        FileChannel ch = randomAccessFile.getChannel();
        byte[] zeros = new byte[nodeSize];
        ch.position((long) BLOCK_SIZE * blockNum);
        ch.write(ByteBuffer.wrap(zeros));
        return blockNum;
    }

    /**
     * Writes the current internal buffer as the specified block.
     *
     * @param blockNum the block number to write
     * @throws IOException if an I/O error occurs
     */
    private void writeBlock(int blockNum) throws IOException {
        FileChannel ch = randomAccessFile.getChannel();
        buf.position(0);
        buf.limit(nodeSize);
        ch.position((long) BLOCK_SIZE * blockNum);
        while (buf.hasRemaining()) {
            ch.write(buf);
        }
    }

    /**
     * Writes the tag header block for the given tag.
     * Pass {@code expression} as null to preserve the existing expression stored
     * in the block (used when updating rootBlock/levels after a split).
     */
    private void writeTagHeader(Tag t, String expression) throws IOException {
        FileChannel ch = randomAccessFile.getChannel();
        ByteBuffer hb = ByteBuffer.allocate(BLOCK_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        // If we need to preserve an existing expression, read it first
        if (expression == null) {
            ch.position((long) BLOCK_SIZE * t.getHeaderBlock());
            ch.read(hb);
            hb.flip();
            // Overwrite only the fields we manage
            hb.putInt(0, t.getRootBlock());
            // levels is at byte 22 in our scheme — update it
            hb.put(22, (byte) t.levels);
        } else {
            // Fresh write
            byte kfmt = t.isDescending() ? (byte) 0x18 : (byte) 0x10;
            byte ktype = (t.getDataType() == IndexDataType.NUMERIC) ? (byte) 'N' : (byte) 'C';
            hb.putInt(0, t.getRootBlock());
            hb.putInt(4, t.getSizeInBlocks());
            hb.put(8, kfmt);
            hb.put(9, ktype);
            // skip 2 bytes (10,11)
            hb.putShort(12, (short) t.getKeyLength());
            hb.putShort(14, (short) t.getKeysPerBlock());
            hb.putShort(16, (short) t.getSecondaryKeyType());
            hb.putShort(18, (short) t.getKeyItemLength());
            // byte 20: reserved; byte 21: unique flag
            hb.put(21, t.isUnique() ? (byte) 1 : (byte) 0);
            hb.put(22, (byte) t.levels);
            // Write expression starting at byte 24
            byte[] exprBytes = expression.toUpperCase().getBytes(StandardCharsets.UTF_8);
            for (int i = 0; i < exprBytes.length && 24 + i < BLOCK_SIZE; i++) {
                hb.put(24 + i, exprBytes[i]);
            }
        }
        hb.position(0);
        hb.limit(BLOCK_SIZE);
        ch.position((long) BLOCK_SIZE * t.getHeaderBlock());
        while (hb.hasRemaining()) {
            ch.write(hb);
        }
    }

    /**
     * Writes the MDX file header and tag descriptor area.
     *
     * @throws IOException if an I/O error occurs
     */
    void writeHeader() throws IOException {
        FileChannel ch = randomAccessFile.getChannel();
        // Header is 544 bytes + tagsInUse * tagLength bytes
        int headerSize = 544 + keysInTag * tagLength;
        ByteBuffer hb = ByteBuffer.allocate(headerSize).order(ByteOrder.LITTLE_ENDIAN);

        Calendar cal = Calendar.getInstance();
        hb.put(0, (byte) 2); // version
        hb.put(1, (byte) (cal.get(Calendar.YEAR) - 2000));
        hb.put(2, (byte) (cal.get(Calendar.MONTH) + 1));
        hb.put(3, (byte) cal.get(Calendar.DAY_OF_MONTH));

        // DBF name: 16 bytes, zero-padded
        byte[] nameBytes = dbfName.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < 16; i++) {
            hb.put(4 + i, i < nameBytes.length ? nameBytes[i] : (byte) 0);
        }

        hb.putShort(20, (short) blockSizeMultiplier);
        hb.putShort(22, (short) nodeSize);
        hb.put(24, production ? (byte) 1 : (byte) 0);
        hb.put(25, (byte) keysInTag);
        hb.put(26, (byte) tagLength);
        // byte 27: reserved
        hb.putShort(28, (short) tagsInUse);
        // bytes 30,31: reserved
        hb.putInt(32, numberOfBlocks);
        hb.putInt(36, firstFreeBlock);
        hb.putInt(40, availableBlock);
        // Last update date at bytes 44-46
        hb.put(44, (byte) (cal.get(Calendar.YEAR) - 2000));
        hb.put(45, (byte) (cal.get(Calendar.MONTH) + 1));
        hb.put(46, (byte) cal.get(Calendar.DAY_OF_MONTH));

        // Tag descriptors starting at byte 544
        for (int i = 0; i < tags.length; i++) {
            Tag t = tags[i];
            int base = 544 + i * tagLength;
            hb.putInt(base, t.getHeaderBlock());
            // Name: bytes 4..13 (10-byte fixed field, zero-padded)
            byte[] nb = t.getName().getBytes(StandardCharsets.UTF_8);
            for (int j = 0; j < 10; j++) {
                hb.put(base + 4 + j, j < nb.length ? nb[j] : (byte) 0);
            }
            // byte 14: reserved (zero, aligns reader to position 15 after name)
            hb.put(base + 14, (byte) 0);
            // byte 15: keyFormat
            byte kfmt = (byte) (0x10 | (t.isDescending() ? 0x08 : 0) | (t.isUnique() ? 0x40 : 0));
            hb.put(base + 15, kfmt);
            hb.put(base + 16, (byte) (i > 0 ? i - 1 : 0));   // leftTag
            hb.put(base + 17, (byte) (i + 1 < tags.length ? i + 1 : 0)); // rightTag
            hb.put(base + 18, (byte) 0); // backwardTag
            // byte 19: reserved (skip)
            hb.put(base + 19, (byte) 0);
            // byte 20: keyType
            byte ktype = (t.getDataType() == IndexDataType.NUMERIC) ? (byte) 'N' : (byte) 'C';
            hb.put(base + 20, ktype);
        }

        hb.position(0);
        ch.position(0);
        while (hb.hasRemaining()) {
            ch.write(hb);
        }
    }

    /** Holds the result of a B-tree node split. */
    private static class SplitResult {
        final byte[] promotedKey;
        final byte[] rightMaxKey;
        final int rightBlock;

        SplitResult(byte[] promotedKey, byte[] rightMaxKey, int rightBlock) {
            this.promotedKey = promotedKey;
            this.rightMaxKey = rightMaxKey;
            this.rightBlock = rightBlock;
        }
    }
}
