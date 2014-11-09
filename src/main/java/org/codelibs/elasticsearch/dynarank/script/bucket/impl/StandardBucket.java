package org.codelibs.elasticsearch.dynarank.script.bucket.impl;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

import org.codelibs.elasticsearch.dynarank.script.bucket.Bucket;
import org.codelibs.minhash.MinHash;
import org.elasticsearch.search.internal.InternalSearchHit;

public class StandardBucket implements Bucket {
    protected Queue<InternalSearchHit> queue = new LinkedList<>();

    protected byte[][] hashes;

    private float[] thresholds;

    public StandardBucket(final InternalSearchHit hit, final byte[][] hashes,
            final float[] thresholds) {
        this.hashes = hashes;
        this.thresholds = thresholds;
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
        final byte[][] targets = (byte[][]) value;
        for (int i = 0; i < thresholds.length; i++) {
            final byte[] hash = hashes[i];
            final byte[] target = targets[i];
            if (MinHash.compare(hash, target) < thresholds[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void add(final Object... args) {
        queue.add((InternalSearchHit) args[0]);
    }

    @Override
    public String toString() {
        return "StandardBucket [hash=" + Arrays.toString(hashes) + ", queue="
                + queue + "]";
    }

    @Override
    public int size() {
        return queue.size();
    }
}
