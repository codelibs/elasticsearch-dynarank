package org.codelibs.elasticsearch.dynarank.script.bucket;

import org.elasticsearch.search.internal.InternalSearchHit;

public interface Bucket {

    public abstract float compare(Object... values);

    public abstract InternalSearchHit get();

    public abstract void add(InternalSearchHit hit);

    public abstract void consume();

}