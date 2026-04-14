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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

/**
 * NDX single index implementation.
 */
public class NDX implements DBFIndex {
    
    public static final int PAGE_SIZE = 512; // Same as block size for NDX

    private final ReentrantLock threadLock;
    private final ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE)
                                             .order(ByteOrder.LITTLE_ENDIAN);
    private final File ndxFile;
    private final RandomAccessFile randomAccessFile;

    private int startPage;
    private int totalPages;
    private int keyLength;
    private int keysPerPage;
    private IndexDataType dataType;
    private boolean unique;
    private String key;
    
    private int pageNumber;
    private int keyIndex;
    private Charset charset;

    private NDX(File ndxFile, RandomAccessFile randomAccessFile, ReentrantLock threadLock) {
        this.ndxFile = ndxFile;
        this.randomAccessFile = randomAccessFile;
        this.threadLock = threadLock;
        charset = StandardCharsets.UTF_8;
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
        buf.limit(PAGE_SIZE);
        while (buf.hasRemaining()) {
            if (channel.read(buf) == -1) {
                throw new IOException("EOF encountered while reading NDX structure");
            }
        }

        buf.position(0);
        startPage = buf.getInt();
        totalPages = buf.getInt();
        buf.position(buf.position() + 4); // Skip reserved
        keyLength = buf.getShort() & 0xffff;
        keysPerPage = buf.getShort() & 0xffff;
        dataType = IndexDataType.valueOf(buf.getShort() & 0xffff);
        int keyRecordSize = buf.getShort() & 0xffff;
        assert(keyRecordSize() == keyRecordSize)
                : "Invalid key record size. Indicated=" + keyRecordSize + "; Expected=" + keyRecordSize();
        buf.position(buf.position() + 2); // Skip reserved
        unique = buf.getShort() != 0;
        byte[] keyBytes = new byte[buf.remaining()];
        int i;
        for (i = 0; buf.hasRemaining(); i++) {
            byte b = buf.get();
            keyBytes[i] = (byte) (b & 0xff);
            if (b == 0) {
                break;
            }
        }
        key = new String(keyBytes, charset);
    }

    private int keyRecordSize() {
        return ((int) Math.ceil(keyLength / 4f)) * 4 + 8;
    }

    /**
     * Moves to the given b+ tree page number and reads the page into the
     * internal buffer.
     * Page numbers start at index <em>1</em>.
     *
     * @param pageNumber the page number to move to
     * @throws IOException if an I/O error occurs
     */
    public void gotoPage(int pageNumber) throws IOException {
        if (this.pageNumber != pageNumber) {
            this.pageNumber = pageNumber;
            readPage();
        }
    }

    /**
     * Reads the current page into the internal buffer.
     *
     * @throws IOException if an I/O error occurs
     */
    public void readPage() throws IOException {
        if (pageNumber <= 0) {
            throw new IllegalStateException("Invalid page number: " + pageNumber);
        }
        FileChannel channel = randomAccessFile.getChannel();
        buf.position(0);
        buf.limit(PAGE_SIZE);
        channel.position(PAGE_SIZE * (long) pageNumber);
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
        return find(value, startPage);
    }

    private int find(Object value, int pageNumber) throws IOException {
        gotoPage(pageNumber);
        final int keysInPage = keysInPage();

        int nextPage, recordNumber;
        int compareResult = 0;
        for (int i = 0; i < keysInPage; i++) {
            nextPage = nextPage(i);
            recordNumber = recordNumber(i);
            switch (dataType) {
                case DATE: {
                    DBFDate date = (DBFDate) value;
                    // redefine requested value as string
                    value = date.dtos();
                    // fall through
                }
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
                    compareResult = new String(bytes, 0, j).compareTo(sb.toString());
                    break;
                case NUMERIC:
                    break;
            }

            if (compareResult >= 0) {
                if (nextPage == 0) {
                    // Leaf
                    return recordNumber;
                } else {
                    // Branch
                    return find(value, nextPage);
                }
            }
        }

        return -1;
    }

    /**
     * Fetches the next page pointer for the given key which exists in
     * <code>buf</code> after a call to {@link #readPage}. This is only
     * applicable for keys which are not leaves. For leaf keys,
     * {@link #recordNumber} should be used instead, in order to fetch the
     * record number.
     * @param key the zero based key within the page
     * @return the next page number, or <em>0</em> if the given key is a leaf
     */
    private int nextPage(int key) {
        return buf.getInt(4 + key * keyRecordSize());
    }

    /**
     * Fetches the record number for the given key which exists in
     * <code>buf</code> after a call to {@link readPage}. This is only
     * applicable for keys which are leaves. For non-leave keys,
     * {@link #nextPage} should be used instead, in order to fetch the
     * next page number which is used to continue the search.
     * @param key the zero based key within the page
     * @return the record number, or <em>0</em> if the given key is not a leaf
     */
    private int recordNumber(int key) {
        return buf.getInt(8 + key * keyRecordSize());
    }

    private int keysInPage() {
        return buf.getInt(0);
    }

    /**
     * Closes the current file and releases resources taken by the connection to
     * this NDX file.
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void update(DBF dbf, java.util.Map<String, Object> oldKeys) throws IOException {
        Object oldKey = oldKeys.get("NDX");
        Object newKey = dbf.evaluateExpression(getExpression());
        
        if (oldKey != null || newKey != null) {
            if (oldKey == null || !oldKey.equals(newKey)) {
                if (oldKey != null) {
                    delete(oldKey, dbf.recno());
                }
                if (newKey != null) {
                    insert(newKey, dbf.recno());
                }
            }
        }
    }

    public void close() throws IOException {
        randomAccessFile.close();
    }

    /**
     * Prints the current NDX file's structure to <code>System.out</code>.
     */
    public void printStructure() {
        printStructure(System.out);
    }

    /**
     * Prints the current NDX file's structure to the requested print stream.
     * @param out The print stream to print the structure to.
     */
    public void printStructure(PrintStream out) {
        out.println("----------------------------------");
        
        out.printf("Start Page:     %18d\n", startPage);
        out.printf("Total Pages:    %18d\n", totalPages);
        out.printf("Key Length:     %18d\n", keyLength);
        out.printf("Key Record Size:%18d\n", keyRecordSize());
        out.printf("Keys Per Page:  %18d\n", keysPerPage);
        out.printf("Data Type:      %18s\n", dataType.name());
        out.printf("Unique:         %18b\n", unique);
        out.printf("Key: %29s\n", key);

        out.println("----------------------------------");
    }

    // -----------------------------------------------------------------------
    // DBFIndex navigation
    // -----------------------------------------------------------------------

    /**
     * Returns the expression (field name) for this index.
     * @return the key expression
     */
    public String getExpression() {
        return key;
    }

    @Override
    public int next() throws IOException {
        int count = keysInPage();
        if (keyIndex < count - 1) {
            keyIndex++;
            return recordNumber(keyIndex);
        }
        return DBF.RECORD_NUMBER_EOF;
    }

    @Override
    public int prev() throws IOException {
        if (keyIndex > 0) {
            keyIndex--;
            return recordNumber(keyIndex);
        }
        return DBF.RECORD_NUMBER_BOF;
    }

    @Override
    public int gotoTop() throws IOException {
        gotoPage(startPage);
        // Descend left-most path to leaf
        while (nextPage(0) != 0) {
            gotoPage(nextPage(0));
        }
        keyIndex = 0;
        return recordNumber(0);
    }

    @Override
    public int gotoBottom() throws IOException {
        gotoPage(startPage);
        // Descend right-most path to leaf
        while (true) {
            int count = keysInPage();
            if (nextPage(count - 1) == 0) {
                break;
            }
            gotoPage(nextPage(count - 1));
        }
        keyIndex = keysInPage() - 1;
        return recordNumber(keyIndex);
    }

    // -----------------------------------------------------------------------
    // Write support
    // -----------------------------------------------------------------------

    /**
     * Creates a new, empty NDX file.
     *
     * @param ndxFile  the file to create
     * @param keyExpr  the key expression (field name, up to 488 characters)
     * @param dataType the data type of the key
     * @param unique   whether the index enforces unique keys
     * @return a reference to the created NDX
     * @throws IOException if an I/O error occurs
     */
    public static NDX create(File ndxFile, String keyExpr, IndexDataType dataType,
            boolean unique) throws IOException {
        return create(ndxFile, keyExpr, dataType, unique, new ReentrantLock());
    }

    /**
     * Creates a new, empty NDX file with the specified thread lock.
     *
     * @param ndxFile    the file to create
     * @param keyExpr    the key expression
     * @param dataType   the data type
     * @param unique     whether the index enforces unique keys
     * @param threadLock an existing thread lock
     * @return a reference to the created NDX
     * @throws IOException if an I/O error occurs
     */
    public static NDX create(File ndxFile, String keyExpr, IndexDataType dataType,
            boolean unique, ReentrantLock threadLock) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(ndxFile,
                "rw" + (DBF.isSynchronousWritesEnabled() ? "s" : ""));
        NDX ndx = new NDX(ndxFile, raf, threadLock);
        ndx.dataType = dataType;
        ndx.unique = unique;
        ndx.key = keyExpr;

        // Determine key length from data type
        switch (dataType) {
            case NUMERIC:
                ndx.keyLength = 8; // NDX stores numeric keys as IEEE 754 double LE
                break;
            case DATE:
                ndx.keyLength = 8; // 'YYYYMMDD'
                break;
            default: // CHARACTER
                ndx.keyLength = Math.max(1, Math.min(240, keyExpr.length()));
                break;
        }

        ndx.keysPerPage = (PAGE_SIZE - 4) / ndx.keyRecordSize();
        // Allocate header page (0) and initial root/leaf page (1)
        ndx.startPage = 1;
        ndx.totalPages = 1;
        ndx.pageNumber = 0;
        ndx.writeHeader();
        // Write empty root page
        ndx.buf.clear();
        Arrays.fill(ndx.buf.array(), (byte) 0);
        ndx.writePage(1);
        return ndx;
    }

    /**
     * Inserts a key and record number into this index.
     *
     * @param key          the key value
     * @param recordNumber the record number in the DBF
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void insert(Object key, int recordNumber) throws IOException {
        byte[] encodedKey = encodeKey(key);
        SplitResult split = insertIntoPage(startPage, encodedKey, recordNumber);
        if (split != null) {
            // Root page was split — create a new root
            int newRoot = allocatePage();
            buf.clear();
            Arrays.fill(buf.array(), (byte) 0);
            // New root has 1 key: leftChild=startPage, this key's nextPage=split.rightPage
            buf.putInt(0, 1);          // key count
            buf.putInt(4, startPage);  // nextPage for key 0 (points to left subtree)
            buf.putInt(8, 0);          // record number (0 for internal nodes)
            buf.position(12);
            buf.put(split.promotedKey);
            // The slot at index 0 has nextPage=startPage; above the promoted key
            // the right subtree is tracked via the next slot's nextPage.
            // Insert a dummy slot pointing to split.rightPage after the promoted key.
            int krs = keyRecordSize();
            buf.putInt(4 + 1 * krs, split.rightPage);
            writePage(newRoot);
            startPage = newRoot;
            writeHeader();
        }
    }

    /**
     * Deletes a key and record number from this index.
     * Performs a simple removal without rebalancing.
     *
     * @param key          the key value
     * @param recordNumber the record number
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void delete(Object key, int recordNumber) throws IOException {
        byte[] encodedKey = encodeKey(key);
        deleteFromPage(startPage, encodedKey, recordNumber);
    }

    /**
     * Recursively inserts a key into the B-tree rooted at {@code pageNum}.
     *
     * @return a SplitResult if the page was split, or null
     */
    private SplitResult insertIntoPage(int pageNum, byte[] encodedKey, int recordNumber)
            throws IOException {
        gotoPage(pageNum);
        int count = keysInPage();
        int krs = keyRecordSize();
        boolean isLeaf = nextPage(0) == 0 && count == 0 || isLeafPage();

        // Find insertion position
        int pos = 0;
        while (pos < count) {
            byte[] stored = readRawKey(pos);
            if (compareKeys(encodedKey, stored) < 0) {
                break;
            }
            pos++;
        }

        if (!isLeaf) {
            // Recurse into the appropriate child page
            int childPage = (pos < count) ? nextPage(pos) : nextPage(count - 1);
            if (pos == 0 && count == 0) {
                childPage = nextPage(0);
            }
            SplitResult childSplit = insertIntoPage(childPage, encodedKey, recordNumber);
            if (childSplit == null) {
                return null;
            }
            // Re-read this page (gotoPage caches)
            gotoPage(pageNum);
            count = keysInPage();
            pos = 0;
            while (pos < count) {
                if (compareKeys(childSplit.promotedKey, readRawKey(pos)) < 0) break;
                pos++;
            }
            encodedKey = childSplit.promotedKey;
            recordNumber = 0; // internal nodes store 0 as record number
            // Update the nextPage for the right child — insert as next entry
            // For simplicity, insert the promoted key at pos with rightPage as nextPage
            if (count < keysPerPage) {
                insertKeyAt(pos, encodedKey, childSplit.rightPage, recordNumber, count, krs);
                buf.putInt(0, count + 1);
                writePage(pageNum);
                return null;
            } else {
                return splitPage(pageNum, pos, encodedKey, childSplit.rightPage, recordNumber, count, krs);
            }
        }

        // Leaf insert
        if (count < keysPerPage) {
            insertKeyAt(pos, encodedKey, 0, recordNumber, count, krs);
            buf.putInt(0, count + 1);
            writePage(pageNum);
            return null;
        } else {
            return splitPage(pageNum, pos, encodedKey, 0, recordNumber, count, krs);
        }
    }

    /**
     * Returns true if the current page is a leaf (all nextPage pointers are 0).
     */
    private boolean isLeafPage() {
        int count = keysInPage();
        if (count == 0) return true;
        for (int i = 0; i < count; i++) {
            if (nextPage(i) != 0) return false;
        }
        return true;
    }

    /**
     * Inserts a key at position {@code pos} in the current page buffer,
     * shifting existing entries right.
     */
    private void insertKeyAt(int pos, byte[] encodedKey, int nextPg, int recNum,
            int count, int krs) {
        // Shift entries right
        for (int i = count - 1; i >= pos; i--) {
            int srcOff = 4 + i * krs;
            int dstOff = 4 + (i + 1) * krs;
            for (int b = 0; b < krs; b++) {
                buf.put(dstOff + b, buf.get(srcOff + b));
            }
        }
        int off = 4 + pos * krs;
        buf.putInt(off, nextPg);
        buf.putInt(off + 4, recNum);
        buf.position(off + 8);
        buf.put(encodedKey, 0, encodedKey.length);
        // zero-pad
        for (int b = off + 8 + encodedKey.length; b < off + krs; b++) {
            buf.put(b, (byte) 0);
        }
    }

    /**
     * Splits a full page and returns the promoted key and right-page number.
     */
    private SplitResult splitPage(int pageNum, int insertPos, byte[] newKey,
            int newNextPg, int newRecNum, int count, int krs) throws IOException {
        int total = count + 1;
        int[] nextPages = new int[total];
        int[] recNums  = new int[total];
        byte[][] keys  = new byte[total][newKey.length];

        for (int i = 0, src = 0; i < total; i++) {
            if (i == insertPos) {
                nextPages[i] = newNextPg;
                recNums[i]   = newRecNum;
                System.arraycopy(newKey, 0, keys[i], 0, newKey.length);
            } else {
                nextPages[i] = nextPage(src);
                recNums[i]   = recordNumber(src);
                System.arraycopy(readRawKey(src), 0, keys[i], 0, newKey.length);
                src++;
            }
        }

        int mid = total / 2;
        byte[] promotedKey = Arrays.copyOf(keys[mid], keys[mid].length);

        // Write left page (entries 0..mid-1)
        Arrays.fill(buf.array(), (byte) 0);
        buf.putInt(0, mid);
        for (int i = 0; i < mid; i++) {
            buf.putInt(4 + i * krs, nextPages[i]);
            buf.putInt(4 + i * krs + 4, recNums[i]);
            buf.position(4 + i * krs + 8);
            buf.put(keys[i]);
        }
        writePage(pageNum);

        // Write right page (entries mid..total-1)
        int rightPage = allocatePage();
        Arrays.fill(buf.array(), (byte) 0);
        int rightCount = total - mid;
        buf.putInt(0, rightCount);
        for (int i = 0; i < rightCount; i++) {
            buf.putInt(4 + i * krs, nextPages[mid + i]);
            buf.putInt(4 + i * krs + 4, recNums[mid + i]);
            buf.position(4 + i * krs + 8);
            buf.put(keys[mid + i]);
        }
        writePage(rightPage);

        return new SplitResult(promotedKey, rightPage);
    }

    /**
     * Recursively deletes a key from the B-tree.
     * Does not rebalance; simply removes the entry from the leaf.
     */
    private boolean deleteFromPage(int pageNum, byte[] encodedKey, int recordNumber)
            throws IOException {
        gotoPage(pageNum);
        int count = keysInPage();
        int krs = keyRecordSize();
        boolean isLeaf = isLeafPage();

        for (int i = 0; i < count; i++) {
            byte[] stored = readRawKey(i);
            int cmp = compareKeys(encodedKey, stored);
            if (cmp < 0) {
                if (!isLeaf) {
                    return deleteFromPage(nextPage(i), encodedKey, recordNumber);
                }
                return false;
            }
            if (cmp == 0) {
                if (isLeaf && recordNumber(i) == recordNumber) {
                    // Shift entries left
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
                    writePage(pageNum);
                    return true;
                } else if (!isLeaf) {
                    return deleteFromPage(nextPage(i), encodedKey, recordNumber);
                }
            }
        }
        if (!isLeaf && count > 0) {
            return deleteFromPage(nextPage(count - 1), encodedKey, recordNumber);
        }
        return false;
    }

    /**
     * Reads the raw key bytes at slot {@code i} in the current page buffer.
     */
    private byte[] readRawKey(int i) {
        byte[] k = new byte[keyLength];
        buf.position(4 + i * keyRecordSize() + 8);
        buf.get(k);
        return k;
    }

    /**
     * Compares two keys lexicographically (unsigned bytes).
     */
    private static int compareKeys(byte[] a, byte[] b) {
        for (int i = 0; i < a.length && i < b.length; i++) {
            int diff = (a[i] & 0xff) - (b[i] & 0xff);
            if (diff != 0) return diff;
        }
        return a.length - b.length;
    }

    /**
     * Encodes a value to the raw byte representation for this index.
     */
    private byte[] encodeKey(Object value) {
        byte[] bytes = new byte[keyLength];
        switch (dataType) {
            case CHARACTER:
            case DATE: {
                String s = (value instanceof DBFDate)
                        ? ((DBFDate) value).dtos()
                        : (value == null ? "" : value.toString());
                byte[] src = s.getBytes(charset);
                int len = Math.min(src.length, bytes.length);
                System.arraycopy(src, 0, bytes, 0, len);
                Arrays.fill(bytes, len, bytes.length, (byte) ' ');
                break;
            }
            case NUMERIC: {
                // NDX stores numeric keys as IEEE 754 double in little-endian byte order
                double d = (value instanceof Number)
                        ? ((Number) value).doubleValue()
                        : Double.parseDouble(value.toString());
                ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putDouble(d);
                break;
            }
        }
        return bytes;
    }

    /**
     * Allocates a new page by appending to the file.
     *
     * @return the page number of the new page
     * @throws IOException if an I/O error occurs
     */
    private int allocatePage() throws IOException {
        totalPages++;
        writeHeader();
        buf.clear();
        Arrays.fill(buf.array(), (byte) 0);
        writePage(totalPages);
        return totalPages;
    }

    /**
     * Writes the current buffer as the specified page.
     *
     * @param pageNum the page number to write
     * @throws IOException if an I/O error occurs
     */
    private void writePage(int pageNum) throws IOException {
        FileChannel channel = randomAccessFile.getChannel();
        buf.position(0);
        buf.limit(PAGE_SIZE);
        channel.position(PAGE_SIZE * (long) pageNum);
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
    }

    /**
     * Writes the NDX file header (page 0).
     *
     * @throws IOException if an I/O error occurs
     */
    private void writeHeader() throws IOException {
        FileChannel channel = randomAccessFile.getChannel();
        ByteBuffer hb = ByteBuffer.allocate(PAGE_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        hb.putInt(0, startPage);
        hb.putInt(4, totalPages);
        // bytes 8-11: reserved (zero)
        hb.putShort(12, (short) keyLength);
        hb.putShort(14, (short) keysPerPage);
        hb.putShort(16, (short) (dataType == IndexDataType.NUMERIC ? 1 : 0));
        hb.putShort(18, (short) keyRecordSize());
        // bytes 20-21: reserved
        hb.putShort(22, (short) (unique ? 1 : 0));
        byte[] kBytes = key.getBytes(charset);
        for (int i = 0; i < kBytes.length && 24 + i < PAGE_SIZE; i++) {
            hb.put(24 + i, kBytes[i]);
        }
        hb.position(0);
        channel.position(0);
        while (hb.hasRemaining()) {
            channel.write(hb);
        }
    }

    /** Holds the result of a page split. */
    private static class SplitResult {
        final byte[] promotedKey;
        final int rightPage;

        SplitResult(byte[] promotedKey, int rightPage) {
            this.promotedKey = promotedKey;
            this.rightPage = rightPage;
        }
    }
}
