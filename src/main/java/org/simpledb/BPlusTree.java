package org.simpledb;

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
        LeafNode leaf = (LeafNode) findLeaf(key);

        if (leaf == null) {
            return -1;
        }

        for (int i = 0; i < leaf.pageIds.size(); i++) {
            if (leaf.keys.get(i).equals(key)) {
                return leaf.pageIds.get(i);
            }
        }

        return -1;
    }
    //put
    //remove
    //range

    private LeafNode findLeaf(String key) {
        Node current = root;

        while (current instanceof InternalNode internalNode) {
            int i = 0;

            while (i < internalNode.keys.size() && key.compareTo(internalNode.keys.get(i)) >= 0) {
                i++;
            }

            if (i == internalNode.keys.size()) {
                return null;
            }

            current = internalNode.children.get(i);
        }

        return (LeafNode) current;
    }
}
