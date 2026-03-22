package org.simpledb;

import java.io.*;
import java.util.*;

public class BufferPool {

    private final int capacity;

    private final String dataDir;

    private final Map<Integer, Page> cache;

    private final LinkedHashMap<Integer, Integer> lruOrder;

    private int accessSeq;

    private int nextPageId;

    public BufferPool(int capacity, String dataDir) {
        this.capacity = capacity;
        this.dataDir = dataDir;
        this.cache = new HashMap<>();

        this.lruOrder = new LinkedHashMap<>(capacity, 0.75f, true);
        this.accessSeq = 0;
        this.nextPageId = 0;
    }

    public Page getPage(int pageId) throws IOException {
        Page page = cache.get(pageId);

        if (page != null) {
            touch(pageId);
            return page;
        }
        page = loadPage(pageId);

        if (page == null) {
            page = new Page(pageId);
        }

        while (cache.size() >= capacity && !cache.isEmpty()) {
            evictOne();
        }

        cache.put(pageId, page);
        touch(pageId);
        return page;
    }

    public void putPage(Page page) throws IOException {
        while (cache.size() >= capacity && !cache.containsKey(page.getPageId())) {
            evictOne();
        }

        cache.put(page.getPageId(), page);
        touch(page.getPageId());
        if (page.getPageId() >= nextPageId) {
            nextPageId = page.getPageId() + 1;
        }
    }

    private void touch(int pageId) {
        lruOrder.put(pageId, accessSeq++);
    }

    private void evictOne() throws IOException {
        if (lruOrder.isEmpty()) {
            return;
        }

        Integer victimId = lruOrder.keySet().iterator().next();
        lruOrder.remove(victimId);

        Page victim = cache.get(victimId);

        if (victim != null && victim.isDirty()) {
            writePage(victim);
        }
    }

    private Page loadPage(int pageId) throws IOException {
        File f = new File(dataDir + "/data" + pageId);

        if (!f.exists()) {
            return null;
        }

        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            List<String> lines = new ArrayList<>();
            String line;
            while ((line = br.readLine()) != null) {
                lines.add(line);
            }

            Page p = Page.deserialize(lines);
            p.markClean();
            return p;
        }
    }

    private void writePage(Page page) throws IOException {
        File f = new File(dataDir + "/data_" + page.getPageId());
        try (PrintWriter pw = new PrintWriter(new FileWriter(f))) {
            pw.println(page.serialize());
        }

        page.markClean();
    }

    public void flushAll() throws IOException {
        for (Page p : cache.values()) {
            if (p.isDirty()) {
                writePage(p);
            }
        }
    }

    public int getNextPageId() {
        return nextPageId;
    }

    public void setNextPageId(int nextPageId) {
        this.nextPageId = nextPageId;
    }

    public boolean contains(int pageId) {
        return cache.containsKey(pageId);
    }

    public Collection<Integer> getCachedPagesIds() {
        return new ArrayList<>(cache.keySet());
    }
}
