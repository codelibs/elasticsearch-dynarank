package org.codelibs.elasticsearch.dynarank.script.bucket;

import org.elasticsearch.search.SearchHit;

public interface Buckets {

    SearchHit[] getHits();

}
