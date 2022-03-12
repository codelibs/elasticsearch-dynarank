package org.codelibs.elasticsearch.dynarank.script.bucket.impl;

import org.codelibs.elasticsearch.dynarank.script.bucket.Bucket;
import org.codelibs.minhash.MinHash;
import org.elasticsearch.search.SearchHit;

import java.util.LinkedList;
import java.util.Queue;

public class MinhashBucket implements Bucket {
    protected Queue<SearchHit> queue = new LinkedList<>();

    protected Object hash;

    private final float threshold;

    private final boolean isMinhash;

    public MinhashBucket(final SearchHit hit, final Object hash, final float threshold, final boolean isMinhash) {
        this.hash = hash;
        this.threshold = threshold;
        this.isMinhash = isMinhash;
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
            if (isMinhash) {
                return MinHash.compare(hash.toString(), value.toString()) >= threshold;
            }
            return value.toString().equals(hash);
        } else if (value instanceof Number) {
            return Math.abs(((Number) value).doubleValue() - ((Number) hash).doubleValue()) < threshold;
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
        return "MinhashBucket [queue=" + queue + ", hash=" + hash + ", threshold=" + threshold + ", isMinhash=" + isMinhash + "]";
    }
}
