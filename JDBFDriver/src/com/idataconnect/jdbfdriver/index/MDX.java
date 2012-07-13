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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;

/**
 * MDX multiple index implementation.
 */
public class MDX {

    private int blockSize;
    private final ReentrantLock threadLock;
    private final File mdxFile;
    private final RandomAccessFile randomAccessFile;
    private final ByteBuffer buf;

    private MDX(File mdxFile, RandomAccessFile randomAccessFile, ReentrantLock threadLock) {
        this.mdxFile = mdxFile;
        this.randomAccessFile = randomAccessFile;
        this.threadLock = threadLock;
        this.buf = ByteBuffer.allocate(2048);
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
        channel.position(0);
        buf.position(0);
        buf.limit(blockSize);
        while (buf.hasRemaining()) {
            channel.read(buf);
        }
    }
}
