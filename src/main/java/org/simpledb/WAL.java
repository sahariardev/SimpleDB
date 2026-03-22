package org.simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class WAL {
    private final String filePath;
    private PrintWriter writer;
    private long lsn;

    public WAL(String filePath) throws IOException {
        this.filePath = filePath;
        this.lsn = 0;

        File f = new File(filePath);
        if (f.exists()) {
            try (BufferedReader br = new BufferedReader(new FileReader(f))) {
                String line;
                while ((line = br.readLine()) != null) {
                    try {
                        long n = Long.parseLong(line.split("\\|")[0]);

                        if (n > lsn) {
                            lsn = n;
                        }
                    } catch (NumberFormatException e) {
                    }
                }
            }
        }

        this.writer = new PrintWriter(new FileWriter(filePath));
    }

    public synchronized long logPut(long txnId, String key, String oldValue, String newValue) {
        lsn++;
        String oldVal = (oldValue == null) ? "NULL" : oldValue;
        writer.println(lsn + "|" + txnId + "PUT" + "|" + key + "|" + oldVal + "|" + newValue);
        writer.flush();

        return lsn;
    }

    public synchronized long logDelete(long txnId, String key, String oldValue) {
        lsn++;
        writer.println(lsn + "|" + txnId + "DELETE" + "|" + key + "|" + oldValue + "|NULL");
        writer.flush();

        return lsn;
    }

    public synchronized long logBegin(long txnId, String key, String oldValue) {
        lsn++;
        writer.println(lsn + "|" + txnId + "BEGIN" + "|||");
        writer.flush();

        return lsn;
    }

    public synchronized long logCommit(long txnId) {
        lsn++;
        writer.println(lsn + "|" + txnId + "COMMIT" + "|||");
        writer.flush();

        return lsn;
    }

    public synchronized long logAbort(long txnId) {
        lsn++;
        writer.println(lsn + "|" + txnId + "ABORT" + "|||");
        writer.flush();

        return lsn;
    }

    public List<WalRecord> readAll() throws IOException {
        List<WalRecord> records = new ArrayList<>();
        File f = new File(filePath);

        if (!f.exists()) {
            return records;
        }

        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\|", -1);
                if (parts.length >= 6) {
                    WalRecord record = new WalRecord();
                    record.lsn = Long.parseLong(parts[0]);
                    record.txnId = Long.parseLong(parts[1]);
                    record.operation = parts[2];
                    record.key = parts[3];
                    record.oldValue = parts[4].equals("NULL") ? null : parts[4];
                    record.newValue = parts[5].equals("NULL") ? null : parts[5];

                    records.add(record);
                }
            }
        }

        return records;
    }

    public void close() {
        writer.close();
    }

    public static class WalRecord {
        public long lsn;
        public long txnId;
        public String operation;
        public String key;
        public String oldValue;
        public String newValue;
    }

}
