package org.codelibs.elasticsearch.dynarank.script.bucket.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.codelibs.elasticsearch.dynarank.DynamicRankingException;
import org.codelibs.elasticsearch.dynarank.script.bucket.Bucket;
import org.codelibs.elasticsearch.dynarank.script.bucket.BucketFactory;
import org.codelibs.elasticsearch.dynarank.script.bucket.Buckets;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.internal.InternalSearchHit;

public class StandardBuckets implements Buckets {

    protected BucketFactory bucketFactory;

    protected Map<String, Object> params;

    public StandardBuckets(final BucketFactory bucketFactory,
            final Map<String, Object> params) {
        this.bucketFactory = bucketFactory;
        this.params = params;
    }

    @Override
    public InternalSearchHit[] getHits() {
        final InternalSearchHit[] searchHits = (InternalSearchHit[]) params
                .get("searchHits");
        final int length = searchHits.length;
        final String diversityField = (String) params.get("diversity_field");
        final float threshold = Float.parseFloat((String) params
                .get("diversity_threshold"));
        final List<Bucket> bucketList = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            boolean insert = false;
            final InternalSearchHit hit = searchHits[i];
            final SearchHitField field = hit.getFields().get(diversityField);
            if (field == null) {
                throw new DynamicRankingException(diversityField
                        + " field does not exists: field:" + field);
            }
            final BytesArray value = field.getValue();
            final byte[] hash = value.toBytes();
            for (final Bucket bucket : bucketList) {
                if (bucket.compare(hash) >= threshold) {
                    bucket.add(hit);
                    insert = true;
                    break;
                }
            }
            if (!insert) {
                bucketList.add(bucketFactory.createBucket(hash, hit));
            }
        }

        return createHits(length, bucketList);
    }

    protected InternalSearchHit[] createHits(final int size,
            final List<Bucket> bucketList) {
        int pos = 0;
        final InternalSearchHit[] newSearchHits = new InternalSearchHit[size];
        while (pos < size) {
            for (final Bucket bucket : bucketList) {
                final InternalSearchHit hit = bucket.get();
                if (hit != null) {
                    newSearchHits[pos] = hit;
                    pos++;
                    bucket.consume();
                }
            }
        }

        return newSearchHits;
    }

}
