package org.simpledb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SimpleDB {

    private static final int BUFFER_POOL_SIZE = 1024;

    private final String dataDir;
    private final WAL wal;
    private final BPlusTree index;
    private final BufferPool bufferPool;
    private int nextPageId;
    private long nextTxnId;

    private long activeTxn;
    private final List<WAL.WalRecord> activeTxnWrites;

    public SimpleDB(String dataDir) throws IOException {
        this.dataDir = dataDir;
        new File(dataDir).mkdirs();

        this.wal = new WAL(dataDir + "/wal.log");
        this.index = new BPlusTree();
        this.bufferPool = new BufferPool(BUFFER_POOL_SIZE, dataDir);

        this.nextPageId = 0;
        this.nextTxnId = 0;
        this.activeTxn = -1;
        this.activeTxnWrites = new ArrayList<>();
        nextPageId = index.loadFromFile(dataDir + "/index.db");
        bufferPool.setNextPageId(nextPageId);
    }

    public long begin() {
        if (activeTxn >= 0) {
            throw new IllegalStateException("transaction already active");
        }

        activeTxn = ++nextTxnId;
        activeTxnWrites.clear();
        wal.logBegin(activeTxn);
        return activeTxn;
    }

    public void commit() throws IOException {
        if (activeTxn < 0) {
            throw new IllegalStateException("no active transaction");
        }
        wal.logCommit(activeTxn);
        flush();
        activeTxn = -1;
        activeTxnWrites.clear();
    }

    public void abort() throws IOException {
        if (activeTxn < 0) {
            throw new IllegalStateException("no active transaction");
        }

        for (int i = activeTxnWrites.size() - 1; i >= 0; i--) {
            WAL.WalRecord record = activeTxnWrites.get(i);

            if (record.operation.equals("PUT")) {
                if (record.oldValue == null) {
                    removeInternal(record.key);
                } else {
                    putInternal(record.key, record.oldValue);
                }
            } else if (record.operation.equals("DELETE")) {
                putInternal(record.key, record.oldValue);
            }
        }

        wal.logAbort(activeTxn);
        activeTxn = -1;
        activeTxnWrites.clear();
    }

    public void put(String key, String value) throws IOException {
        boolean autoCommit = (activeTxn < 0);
        if (autoCommit) {
            begin();
        }

        String oldValue = "";
        wal.logPut(activeTxn, key, oldValue, value);
        WAL.WalRecord record = new WAL.WalRecord();
        record.key = key;
        record.oldValue = value;
        record.operation = "PUT";
        record.newValue = value;
        activeTxnWrites.add(record);

        putInternal(record.key, oldValue);
        commit();
    }

    public String get(String key) throws IOException {
        int pageId = index.get(key);
        if (pageId < 0) {
            return null;
        }

        Page page = bufferPool.getPage(pageId);
        return page.get(key);
    }

    public void delete(String key) throws IOException {
        boolean autoCommit = (activeTxn < 0);
        if (autoCommit) {
            begin();
        }

        String oldValue = get(key);
        if (oldValue == null) {
            if (autoCommit) {
                commit();
            }
            return;
        }

        wal.logDelete(activeTxn, key, oldValue);
        WAL.WalRecord record = new WAL.WalRecord();
        record.key = key;
        record.oldValue = oldValue;
        record.operation = "DELETE";
        activeTxnWrites.add(record);
        removeInternal(record.key);

        if (autoCommit) {
            commit();
        }
    }

    public void close() throws IOException {
        flush();
        wal.close();
    }

    public void recover() throws IOException {
        List<WAL.WalRecord> records = wal.readAll();

        Set<Long> commited = new HashSet<>();
        Set<Long> aborted = new HashSet<>();

        for (WAL.WalRecord record : records) {
            if (record.operation.equals("COMMIT")) {
                commited.add(record.txnId);
            }
            if (record.operation.equals("ABORT")) {
                aborted.add(record.txnId);
            }
        }

        int redone = 0;
        for (WAL.WalRecord record : records) {
            if (!commited.contains(record.txnId)) {
                continue;
            }

            if (record.operation.equals("PUT")) {
                putInternal(record.key, record.newValue);
                redone++;

            } else if (record.operation.equals("DELETE")) {
                removeInternal(record.key);
                redone++;
            }
        }

        flush();
    }

    private void putInternal(String key, String value) throws IOException {
        int pageId = index.get(key);

        if (pageId >= 0) {
            bufferPool.getPage(pageId).put(key, value);
            return;
        }

        int needed = 8 + key.length() + value.length();

        Page target = null;

        for (int pid : bufferPool.getCachedPagesIds()) {
            Page p = bufferPool.getPage(pid);

            if (p.getUsedBytes() + needed <= Page.PAGE_SIZE) {
                target = p;
                break;
            }
        }

        if (target == null) {
            target = new Page(nextPageId++);
            bufferPool.putPage(target);
        }

        target.put(key, value);
        index.put(key, target.getPageId());
    }

    private void removeInternal(String key) throws IOException {
        int pageId = index.get(key);
        if (pageId >= 0) {
            bufferPool.getPage(pageId).remove(key);
            index.remove(key);
        }
    }

    private void flush() throws IOException {
        bufferPool.flushAll();
        index.saveToFile(dataDir + "/index.db", nextPageId);
    }
}
