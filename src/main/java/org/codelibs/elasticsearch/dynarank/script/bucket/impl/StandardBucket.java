package org.codelibs.elasticsearch.dynarank.script.bucket.impl;

import java.util.LinkedList;
import java.util.Queue;

import org.codelibs.elasticsearch.dynarank.script.bucket.Bucket;
import org.codelibs.minhash.MinHash;
import org.elasticsearch.search.SearchHit;

public class StandardBucket implements Bucket {
    protected Queue<SearchHit> queue = new LinkedList<>();

    protected Object hash;

    private float threshold;

    public StandardBucket(final SearchHit hit, final Object hash,
            final float threshold) {
        this.hash = hash;
        this.threshold = threshold;
        queue.add(hit);
    }

    @Override
    public void consume() {
        queue.poll();
    }

    @Override
    public SearchHit get() {
        return queue.peek();
    }

    @Override
    public boolean contains(final Object value) {
        if (hash == null) {
            return value == null;
        }

        if (value == null) {
            return false;
        }

        if (!hash.getClass().equals(value.getClass())) {
            return false;
        }

        if (value instanceof String) {
            return value.toString().equals(hash);
        } else if (value instanceof Number) {
            return Math.abs(((Number) value).doubleValue()
                    - ((Number) hash).doubleValue()) < (double) threshold;
        } else if (value instanceof byte[]) {
            final byte[] target = (byte[]) value;
            return MinHash.compare((byte[]) hash, target) >= threshold;
        }
        return false;
    }

    @Override
    public void add(final Object... args) {
        queue.add((SearchHit) args[0]);
    }

    @Override
    public int size() {
        return queue.size();
    }

    @Override
    public String toString() {
        return "StandardBucket [queue=" + queue + ", hash=" + hash
                + ", threshold=" + threshold + "]";
    }
}
