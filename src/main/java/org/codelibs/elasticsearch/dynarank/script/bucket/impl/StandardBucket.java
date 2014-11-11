package org.codelibs.elasticsearch.dynarank.script.bucket.impl;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

import org.codelibs.elasticsearch.dynarank.script.bucket.Bucket;
import org.codelibs.minhash.MinHash;
import org.elasticsearch.search.internal.InternalSearchHit;

public class StandardBucket implements Bucket {
    protected Queue<InternalSearchHit> queue = new LinkedList<>();

    protected byte[] hash;

    private float threshold;

    public StandardBucket(final InternalSearchHit hit, final byte[] hash,
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
    public InternalSearchHit get() {
        return queue.peek();
    }

    @Override
    public boolean contains(final Object value) {
        final byte[] target = (byte[]) value;
        return MinHash.compare(hash, target) >= threshold;
    }

    @Override
    public void add(final Object... args) {
        queue.add((InternalSearchHit) args[0]);
    }

    @Override
    public int size() {
        return queue.size();
    }

    @Override
    public String toString() {
        return "StandardBucket [queue=" + queue + ", hash="
                + Arrays.toString(hash) + ", threshold=" + threshold + "]";
    }
}
