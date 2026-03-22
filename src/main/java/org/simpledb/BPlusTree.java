package org.simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class BPlusTree {

    private static final int ORDER = 4;

    private Node root;

    private static abstract class Node {
        List<String> keys = new ArrayList<>();
    }

    private static class LeafNode extends Node {
        List<Integer> pageIds = new ArrayList<>();
        Node next;
    }

    private static class InternalNode extends Node {
        List<Node> children = new ArrayList<>();
    }

    public BPlusTree() {
        this.root = new LeafNode();
    }

    public int get(String key) {
        LeafNode leaf = findLeaf(key);
        for (int i = 0; i < leaf.pageIds.size(); i++) {
            if (leaf.keys.get(i).equals(key)) {
                return leaf.pageIds.get(i);
            }
        }

        return -1;
    }

    public void put(String key, int pageId) {
        LeafNode leaf = findLeaf(key);

        for (int i = 0; i < leaf.keys.size(); i++) {
            if (leaf.keys.get(i).equals(key)) {
                leaf.pageIds.set(i, pageId);
                return;
            }
        }

        int pos = 0;

        while (pos < leaf.keys.size() && key.compareTo(leaf.keys.get(pos)) >= 0) {
            pos++;
        }

        leaf.keys.add(pos, key);
        leaf.pageIds.add(pos, pageId);

        if (leaf.keys.size() > ORDER) {
            splitLeafNode(leaf);
        }
    }

    public void remove(String key) {
        LeafNode leaf = findLeaf(key);
        for (int i = 0; i < leaf.keys.size(); i++) {
            if (leaf.keys.get(i).equals(key)) {
                leaf.keys.remove(i);
                leaf.pageIds.remove(i);
                return;
            }
        }
    }

    public void saveToFile(String path, int nextPageId) throws IOException {
        try (PrintWriter pw = new PrintWriter(new FileWriter(path))) {
            pw.println("NEXTPAGEID:" + nextPageId);
            LeafNode leaf = findLeaf("");

            while (leaf != null) {
                for (int i = 0; i < leaf.keys.size(); i++) {
                    pw.println(leaf.keys.get(i) + "=" + leaf.pageIds.get(i));
                }

                leaf = (LeafNode) leaf.next;
            }
        }
    }

    public int loadFromFile(String path) throws IOException {
        this.root = new LeafNode();
        File f = new File(path);

        if (!f.exists()) {
            return 0;
        }

        int nextPageId = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            String line = br.readLine();
            //todo:: extract NEXTPAGEID into a constant
            if (line != null && line.startsWith("NEXTPAGEID:")) {
                nextPageId = Integer.parseInt(line.substring("NEXTPAGEID:".length()));
            }

            while ((line = br.readLine()) != null) {
                int eq = line.indexOf('=');

                if (eq > 0) {
                    String key = line.substring(0, eq);
                    int pageId = Integer.parseInt(line.substring(eq + 1));
                    put(key, pageId);
                }
            }
        }

        return nextPageId;
    }

    private LeafNode findLeaf(String key) {
        Node current = root;

        while (current instanceof InternalNode internalNode) {
            int i = 0;

            while (i < internalNode.keys.size() && key.compareTo(internalNode.keys.get(i)) >= 0) {
                i++;
            }

            current = internalNode.children.get(i);
        }

        return (LeafNode) current;
    }

    private void splitLeafNode(LeafNode left) {
        int mid = left.keys.size() / 2;

        LeafNode right = new LeafNode();
        right.keys.addAll(left.keys.subList(mid, left.keys.size()));
        right.pageIds.addAll(left.pageIds.subList(mid, left.keys.size()));

        right.next = left.next;
        left.next = right;

        left.keys.subList(mid, left.keys.size()).clear();
        left.pageIds.subList(mid, left.keys.size()).clear();

        String promotedKey = right.keys.getFirst();
        insertIntoParent(left, promotedKey, right);
    }

    private void insertIntoParent(Node left, String key, Node right) {
        if (left == root) {
            InternalNode root = new InternalNode();
            root.keys.add(key);
            root.children.add(left);
            root.children.add(right);
            this.root = root;
            return;
        }

        InternalNode parent = findParent(root, left);
        int indexOfLeft = parent.children.indexOf(left);

        parent.children.set(indexOfLeft + 1, right);
        parent.keys.add(indexOfLeft, key);

        if (parent.keys.size() > ORDER) {
            splitInternalNode(parent);
        }
    }

    private void splitInternalNode(InternalNode node) {
        int mid = node.keys.size() / 2;
        String promotedKey = node.keys.get(mid);

        InternalNode newNode = new InternalNode();
        newNode.keys.addAll(node.keys.subList(mid + 1, node.keys.size()));
        newNode.children.addAll(node.children.subList(mid + 1, node.children.size()));

        node.keys.subList(mid, node.keys.size()).clear();
        node.children.subList(mid + 1, node.children.size()).clear();

        insertIntoParent(node, promotedKey, newNode);
    }

    private InternalNode findParent(Node current, Node child) {
        if (!(current instanceof LeafNode)) {
            return null;
        }

        InternalNode parent = (InternalNode) current;

        for (Node c : parent.children) {
            if (c == child) {
                return parent;
            }

            InternalNode childParent = findParent(c, child);

            if (childParent != null) {
                return childParent;
            }
        }

        return null;
    }
}
