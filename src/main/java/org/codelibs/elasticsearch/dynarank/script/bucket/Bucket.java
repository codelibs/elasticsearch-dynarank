package org.codelibs.elasticsearch.dynarank.script.bucket;

import org.elasticsearch.search.internal.InternalSearchHit;

public interface Bucket {

    boolean contains(Object value);

    InternalSearchHit get();

    void add(Object... args);

    void consume();

    int size();
}
