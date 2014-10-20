package org.codelibs.elasticsearch.dynarank.script.bucket;

import java.util.Map;

public interface BucketFactory {

    Buckets createBucketList(Map<String, Object> params);

    Bucket createBucket(Object... args);

}
