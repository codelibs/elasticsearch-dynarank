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

    public StandardBucket(final byte[] hash, final InternalSearchHit hit) {
        this.hash = hash;
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
    public float compare(final Object... values) {
        return MinHash.compare(hash, (byte[])values[0]);
    }

    
    @Override
    public void add(final InternalSearchHit hit) {
        queue.add(hit);
    }

    @Override
    public String toString() {
        return "StandardBucket [hash=" + Arrays.toString(hash) + ", queue="
                + queue + "]";
    }
}
