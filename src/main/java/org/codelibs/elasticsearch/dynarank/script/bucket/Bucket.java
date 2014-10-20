package org.codelibs.elasticsearch.dynarank.script.bucket;

import org.elasticsearch.search.internal.InternalSearchHit;

public interface Bucket {

    float compare(Object... values);

    InternalSearchHit get();

    void add(InternalSearchHit hit);

    void consume();

    int size();
}