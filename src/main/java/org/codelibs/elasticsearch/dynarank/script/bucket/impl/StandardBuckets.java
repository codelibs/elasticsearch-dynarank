package org.codelibs.elasticsearch.dynarank.script.bucket.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.codelibs.elasticsearch.dynarank.DynamicRankingException;
import org.codelibs.elasticsearch.dynarank.script.bucket.Bucket;
import org.codelibs.elasticsearch.dynarank.script.bucket.BucketFactory;
import org.codelibs.elasticsearch.dynarank.script.bucket.Buckets;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.internal.InternalSearchHit;

public class StandardBuckets implements Buckets {

    private static ESLogger logger = ESLoggerFactory
            .getLogger("script.dynarank.sort.bucket.standard");

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
        final String[] diversityFields = (String[]) params
                .get("diversity_fields");
        if (diversityFields == null) {
            throw new DynamicRankingException("diversity_fields is null.");
        }
        final String[] thresholds = (String[]) params
                .get("diversity_thresholds");
        if (thresholds == null) {
            throw new DynamicRankingException("diversity_thresholds is null.");
        }
        final float[] diversityThresholds = parseFloats(thresholds);
        final List<Bucket> bucketList = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            boolean insert = false;
            final InternalSearchHit hit = searchHits[i];
            final byte[][] hashes = new byte[diversityFields.length][];

            for (int j = 0; j < diversityFields.length; j++) {
                hashes[j] = getFieldValue(hit, diversityFields[j]);
            }

            for (final Bucket bucket : bucketList) {
                if (bucket.contains(hashes)) {
                    bucket.add(hit, hashes);
                    insert = true;
                    break;
                }
            }
            if (!insert) {
                bucketList.add(bucketFactory.createBucket(hit, hashes,
                        diversityThresholds));
            }
        }

        return createHits(length, bucketList);
    }

    private byte[] getFieldValue(final InternalSearchHit hit,
            final String fieldName) {
        final SearchHitField field = hit.getFields().get(fieldName);
        if (field == null) {
            throw new DynamicRankingException(fieldName
                    + " field does not exists: field:" + field);
        }
        final Object object = field.getValue();
        if (object instanceof BytesArray) {
            return ((BytesArray) object).toBytes();
        } else if (object instanceof String) {
            return ((String) object).getBytes(Charsets.UTF_8);
        }
        throw new DynamicRankingException(fieldName
                + " field is unknown type: " + object);
    }

    private float[] parseFloats(final String[] strings) {
        final float[] values = new float[strings.length];
        for (int i = 0; i < strings.length; i++) {
            values[i] = Float.parseFloat(strings[i]);
        }
        return values;
    }

    protected InternalSearchHit[] createHits(final int size,
            final List<Bucket> bucketList) {
        if (logger.isDebugEnabled()) {
            logger.debug("{} docs -> {} buckets", size, bucketList.size());
            for (int i = 0; i < bucketList.size(); i++) {
                final Bucket bucket = bucketList.get(i);
                logger.debug(" bucket[{}] -> {} docs", i, bucket.size());
            }
        }

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
