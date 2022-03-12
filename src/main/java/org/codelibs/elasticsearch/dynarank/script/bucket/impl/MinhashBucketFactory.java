package org.codelibs.elasticsearch.dynarank.script.bucket.impl;

import org.codelibs.elasticsearch.dynarank.script.bucket.Bucket;
import org.codelibs.elasticsearch.dynarank.script.bucket.BucketFactory;
import org.codelibs.elasticsearch.dynarank.script.bucket.Buckets;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHit;

import java.util.Map;

public class MinhashBucketFactory implements BucketFactory {

    protected Settings settings;

    public MinhashBucketFactory(final Settings settings) {
        this.settings = settings;
    }

    @Override
    public Buckets createBucketList(final Map<String, Object> params) {
        return new MinhashBuckets(this, params);
    }

    @Override
    public Bucket createBucket(final Object... args) {
        return new MinhashBucket((SearchHit) args[0], args[1], (float) args[2], (boolean) args[3]);
    }
}
