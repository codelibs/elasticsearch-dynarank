package org.codelibs.elasticsearch.dynarank.script.bucket;

import org.elasticsearch.search.internal.InternalSearchHit;

public interface Buckets {

    InternalSearchHit[] getHits();

}
