package org.codelibs.elasticsearch.dynarank.script.bucket.impl;

import java.util.Map;

import org.codelibs.elasticsearch.dynarank.script.bucket.Bucket;
import org.codelibs.elasticsearch.dynarank.script.bucket.BucketFactory;
import org.codelibs.elasticsearch.dynarank.script.bucket.Buckets;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHit;

public class StandardBucketFactory implements BucketFactory {

    protected Settings settings;

    public StandardBucketFactory(final Settings settings) {
        this.settings = settings;
    }

    @Override
    public Buckets createBucketList(final Map<String, Object> params) {
        return new StandardBuckets(this, params);
    }

    @Override
    public Bucket createBucket(final Object... args) {
        return new StandardBucket((SearchHit) args[0], args[1], (float) args[2]);
    }
}
