package org.simpledb;

import java.util.*;

public class Page {
    public static final int PAGE_SIZE = 4 * 1024;

    private final int pageId;

    private final Map<String, String> entries;

    private int usedBytes;

    private boolean dirty;

    public Page(int pageId) {
        this.pageId = pageId;
        this.entries = new LinkedHashMap<String, String>();
        this.usedBytes = 8;
        this.dirty = false;
    }

    public boolean put(String key, String value) {
        int entrySize = 4 + key.length() + 4 + value.length();

        if (entries.containsKey(key)) {
            String oldValue = entries.get(key);
            usedBytes -= (4 + oldValue.length() + 4 + key.length());
        }


        if (usedBytes + entrySize > PAGE_SIZE) {
            return false;
        }

        entries.put(key, value);
        usedBytes += entrySize;
        dirty = true;
        return true;
    }

    public String get(String key) {
        return entries.get(key);
    }

    public boolean remove(String key) {
        String old = entries.remove(key);

        if (old != null) {
            usedBytes -= (4 + old.length() + 4 + key.length());
            dirty = true;
            return true;
        }

        return false;
    }

    public boolean containsKey(String key) {
        return entries.containsKey(key);
    }

    public int getPageId() {
        return pageId;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void markClean() {
        dirty = false;
    }

    public int getUsedBytes() {
        return usedBytes;
    }

    public Map<String, String> getEntries() {
        return Collections.unmodifiableMap(entries);
    }

    public String serialize() {
        StringBuilder sb = new StringBuilder();
        sb.append("PAGE:").append(pageId).append(":COUNT:").append(entries.size()).append("\n");

        for (Map.Entry<String, String> entry : entries.entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
        }

        sb.append("END_PAGE\n");
        return sb.toString();
    }

    public static Page deserialize(List<String> lines) {
        String header = lines.getFirst();
        String[] parts = header.split(":");
        int id = Integer.parseInt(parts[1]);
        Page page = new Page(id);

        for (int i = 1; i < parts.length; i++) {
            String line = lines.get(i);
            if (line.equals("END_PAGE")) break;

            int eq = line.indexOf("=");
            if (eq > 0) {
                page.put(line.substring(0, eq), line.substring(eq + 1));
            }
        }

        page.markClean();
        return page;
    }

}
