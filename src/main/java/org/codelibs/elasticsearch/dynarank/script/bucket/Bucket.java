package org.codelibs.elasticsearch.dynarank.script.bucket;

import org.elasticsearch.search.SearchHit;

public interface Bucket {

    boolean contains(Object value);

    SearchHit get();

    void add(Object... args);

    void consume();

    int size();
}
