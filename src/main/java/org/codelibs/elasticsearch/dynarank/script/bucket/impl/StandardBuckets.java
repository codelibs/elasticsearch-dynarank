package org.codelibs.elasticsearch.dynarank.script.bucket.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.elasticsearch.dynarank.DynamicRankingException;
import org.codelibs.elasticsearch.dynarank.ranker.RetrySearchException;
import org.codelibs.elasticsearch.dynarank.script.bucket.Bucket;
import org.codelibs.elasticsearch.dynarank.script.bucket.BucketFactory;
import org.codelibs.elasticsearch.dynarank.script.bucket.Buckets;
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
        InternalSearchHit[] searchHits = (InternalSearchHit[]) params
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

        if (logger.isDebugEnabled()) {
            logger.debug("diversity_fields: {}, : diversity_thresholds{}",
                    Arrays.toString(diversityFields),
                    Arrays.toString(thresholds));
        }
        int maxNumOfBuckets = 0;
        for (int i = diversityFields.length - 1; i >= 0; i--) {
            final String diversityField = diversityFields[i];
            final float diversityThreshold = diversityThresholds[i];
            final List<Bucket> bucketList = new ArrayList<>();
            for (int j = 0; j < length; j++) {
                boolean insert = false;
                final InternalSearchHit hit = searchHits[j];
                final Object value = getFieldValue(hit, diversityField);
                if (value == this) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                                "diversityField {} does not exist. Reranking is skipped.",
                                diversityField);
                    }
                    return searchHits;
                }
                for (final Bucket bucket : bucketList) {
                    if (bucket.contains(value)) {
                        bucket.add(hit, value);
                        insert = true;
                        break;
                    }
                }
                if (!insert) {
                    bucketList.add(bucketFactory.createBucket(hit, value,
                            diversityThreshold));
                }
            }
            if (bucketList.size() > maxNumOfBuckets) {
                maxNumOfBuckets = bucketList.size();
            }
            searchHits = createHits(length, bucketList);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("searchHits: {}, maxNumOfBuckets: {}", searchHits.length, maxNumOfBuckets);
        }

        Object value = params.get("min_bucket_size");
        int minBucketSize = 0;
        if (value instanceof String) {
            try {
                minBucketSize = Integer.parseInt(value.toString());
            } catch (NumberFormatException e) {
                throw new DynamicRankingException("Invalid value of min_bucket_size: " + value.toString(), e);
            }
        } else if (value instanceof Number) {
            minBucketSize = ((Number) value).intValue();
        }
        if (minBucketSize > 0 && minBucketSize >= maxNumOfBuckets) {
            final Object shuffleSeed =  params.get("shuffle_seed");
            if (shuffleSeed != null) {
                throw new RetrySearchException(new RetrySearchException.QueryRewriter() {
                    @Override
                    public Map<String, Object> rewrite(Map<String, Object> source) {
                        Object queryObj = source.remove("query");
                        if (queryObj == null) {
                            return null;
                        }
                        Object shuffleWeight = params.get("shuffle_weight");
                        if (shuffleWeight == null) {
                            shuffleWeight = 1;
                        }

                        Map<String, Object> randomScoreMap = new HashMap<>(1, 1f);
                        randomScoreMap.put("seed", shuffleSeed);
                        Map<String, Object> funcMap = new HashMap<>(2, 1f);
                        funcMap.put("random_score", randomScoreMap);
                        funcMap.put("weight", shuffleWeight);
                        List<Map<String, Object>> funcList = new ArrayList<>(1);
                        funcList.add(funcMap);
                        Map<String, Object> funcScoreMap = new HashMap<>(3, 1f);
                        funcScoreMap.put("query", queryObj);
                        funcScoreMap.put("functions", funcList);
                        funcScoreMap.put("boost_mode", "sum");
                        Map<String, Object> queryMap = new HashMap<>(1, 1f);
                        queryMap.put("function_score", funcScoreMap);
                        source.put("query", queryMap);
                        return source;
                    }
                });
            }
        }

        return searchHits;
    }

    private Object getFieldValue(final InternalSearchHit hit,
            final String fieldName) {
        final SearchHitField field = hit.getFields().get(fieldName);
        if (field == null) {
            return this;
        }
        final Object object = field.getValue();
        if (object instanceof BytesArray) {
            return ((BytesArray) object).toBytes();
        } else if (object instanceof String) {
            return object;
        } else if (object instanceof Number) {
            return object;
        }
        return null;
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
