package org.codelibs.elasticsearch.dynarank.script.bucket.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.elasticsearch.dynarank.ranker.RetrySearchException;
import org.codelibs.elasticsearch.dynarank.script.bucket.Bucket;
import org.codelibs.elasticsearch.dynarank.script.bucket.BucketFactory;
import org.codelibs.elasticsearch.dynarank.script.bucket.Buckets;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MinhashBuckets implements Buckets {

    private static final Logger logger = LogManager.getLogger(MinhashBuckets.class);

    protected BucketFactory bucketFactory;

    protected Map<String, Object> params;

    public MinhashBuckets(final BucketFactory bucketFactory, final Map<String, Object> params) {
        this.bucketFactory = bucketFactory;
        this.params = params;
    }

    @Override
    public SearchHit[] getHits(final SearchHit[] searchHits) {
        SearchHit[] hits = searchHits;
        final int length = hits.length;
        final String[] diversityFields = (String[]) params.get("diversity_fields");
        if (diversityFields == null) {
            throw new ElasticsearchException("diversity_fields is null.");
        }
        final String[] thresholds = (String[]) params.get("diversity_thresholds");
        if (thresholds == null) {
            throw new ElasticsearchException("diversity_thresholds is null.");
        }
        final Object sourceAsMap = params.get("source_as_map");
        final float[] diversityThresholds = parseFloats(thresholds);
        final Object[][] ignoredObjGroups = new Object[diversityFields.length][];
        final String[] minhashFields = new String[diversityFields.length];
        for (int i = 0; i < diversityFields.length; i++) {
            ignoredObjGroups[i] = (String[]) params.get(diversityFields[i] + "_ignored_objects");
            if (isMinhashFields(sourceAsMap, diversityFields[i])) {
                minhashFields[i] = diversityFields[i];
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("diversity_fields: {}, : diversity_thresholds{}", diversityFields, thresholds);
        }
        int maxNumOfBuckets = 0;
        int minNumOfBuckets = Integer.MAX_VALUE;
        for (int i = diversityFields.length - 1; i >= 0; i--) {
            final String diversityField = diversityFields[i];
            final boolean isMinhash = Arrays.asList(minhashFields).contains(diversityField);
            final float diversityThreshold = diversityThresholds[i];
            final Object[] ignoredObjs = ignoredObjGroups[i];
            final List<Bucket> bucketList = new ArrayList<>();
            for (int j = 0; j < length; j++) {
                boolean insert = false;
                final SearchHit hit = hits[j];
                final Object value = getFieldValue(hit, diversityField);
                if (value == null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("diversityField {} does not exist. Reranking is skipped.", diversityField);
                    }
                    return hits;
                }
                if (ignoredObjs != null) {
                    for (final Object ignoredObj : ignoredObjs) {
                        if (ignoredObj.equals(value)) {
                            bucketList.add(bucketFactory.createBucket(hit, value, diversityThreshold, isMinhash));
                            insert = true;
                            break;
                        }
                    }
                }
                if (!insert) {
                    for (final Bucket bucket : bucketList) {
                        if (bucket.contains(value)) {
                            bucket.add(hit, value);
                            insert = true;
                            break;
                        }
                    }
                    if (!insert) {
                        bucketList.add(bucketFactory.createBucket(hit, value, diversityThreshold, isMinhash));
                    }
                }
            }
            if (bucketList.size() > maxNumOfBuckets) {
                maxNumOfBuckets = bucketList.size();
            }
            if (bucketList.size() < minNumOfBuckets) {
                minNumOfBuckets = bucketList.size();
            }
            hits = createHits(length, bucketList);
        }

        int minBucketThreshold = 0;
        int maxBucketThreshold = 0;

        final Object minBucketThresholdStr = params.get("min_bucket_threshold");
        if (minBucketThresholdStr instanceof String) {
            try {
                minBucketThreshold = Integer.parseInt(minBucketThresholdStr.toString());
            } catch (final NumberFormatException e) {
                throw new ElasticsearchException("Invalid value of min_bucket_threshold: " + minBucketThresholdStr.toString(), e);
            }
        } else if (minBucketThresholdStr instanceof Number) {
            minBucketThreshold = ((Number) minBucketThresholdStr).intValue();
        }

        final Object maxBucketThresholdStr = params.get("max_bucket_threshold");
        if (maxBucketThresholdStr instanceof String) {
            try {
                maxBucketThreshold = Integer.parseInt(maxBucketThresholdStr.toString());
            } catch (final NumberFormatException e) {
                throw new ElasticsearchException("Invalid value of max_bucket_threshold: " + maxBucketThresholdStr.toString(), e);
            }
        } else if (maxBucketThresholdStr instanceof Number) {
            maxBucketThreshold = ((Number) maxBucketThresholdStr).intValue();
        }

        if (logger.isDebugEnabled()) {
            logger.debug("searchHits: {}, minNumOfBuckets: {}, maxNumOfBuckets: {}, minBucketSize: {}, maxBucketThreshold: {}",
                    hits.length, minNumOfBuckets, maxNumOfBuckets, minBucketThreshold, maxBucketThreshold);
        }

        if ((minBucketThreshold > 0 && minBucketThreshold >= minNumOfBuckets)
                || (maxBucketThreshold > 0 && maxBucketThreshold >= maxNumOfBuckets)) {
            final Object shuffleSeed = params.get("shuffle_seed");
            if (shuffleSeed != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("minBucketSize: {}", shuffleSeed);
                }
                throw new RetrySearchException(new RetrySearchException.QueryRewriter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public SearchSourceBuilder rewrite(final SearchSourceBuilder source) {
                        float shuffleWeight = 1;
                        if (params.get("shuffle_weight") instanceof Number) {
                            shuffleWeight = ((Number) params.get("shuffle_weight")).floatValue();
                        }
                        final Object shuffleBoostMode = params.get("shuffle_boost_mode");

                        final FunctionScoreQueryBuilder functionScoreQuery = QueryBuilders.functionScoreQuery(source.query(),
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder[] { new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        ScoreFunctionBuilders.randomFunction().seed(shuffleSeed.toString()).setWeight(shuffleWeight)) });
                        if (shuffleBoostMode != null) {
                            functionScoreQuery.boostMode(CombineFunction.fromString(shuffleBoostMode.toString()));
                        }
                        source.query(functionScoreQuery);
                        return source;
                    }
                });
            }
        }

        return hits;
    }

    private Object getFieldValue(final SearchHit hit, final String fieldName) {
        final DocumentField field = hit.getFields().get(fieldName);
        if (field == null) {
            final Map<String, Object> source = hit.getSourceAsMap();
            // TODO nested
            final Object object = source.get(fieldName);
            if (object instanceof String) {
                return object;
            } else if (object instanceof Number) {
                return object;
            }
            return null;
        }
        final Object object = field.getValue();
        if (object instanceof BytesReference) {
            return BytesReference.toBytes((BytesReference) object);
        } else if (object instanceof String) {
            return object;
        } else if (object instanceof Number) {
            return object;
        } else if (object instanceof BytesArray) {
            return ((BytesArray) object).array();
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

    protected SearchHit[] createHits(final int size, final List<Bucket> bucketList) {
        if (logger.isDebugEnabled()) {
            logger.debug("{} docs -> {} buckets", size, bucketList.size());
            for (int i = 0; i < bucketList.size(); i++) {
                final Bucket bucket = bucketList.get(i);
                logger.debug(" bucket[{}] -> {} docs", i, bucket.size());
            }
        }

        int pos = 0;
        final SearchHit[] newSearchHits = new SearchHit[size];
        while (pos < size) {
            for (final Bucket bucket : bucketList) {
                final SearchHit hit = bucket.get();
                if (hit != null) {
                    newSearchHits[pos] = hit;
                    pos++;
                    bucket.consume();
                }
            }
        }

        return newSearchHits;
    }

    @SuppressWarnings("unchecked")
    private boolean isMinhashFields(Object sourceAsMap, String field) {
        if (sourceAsMap instanceof Map) {
            Object propertiesMap = ((Map<String, Object>) sourceAsMap).get("properties");
            if (propertiesMap instanceof Map) {
                Object fieldMap = ((Map<String, Object>) propertiesMap).get(field);
                if (fieldMap instanceof Map) {
                    Object fieldType = ((Map<String, Object>) fieldMap).get("type");
                    return fieldType != null && fieldType.toString().equals("minhash");
                }
            }
        }
        return false;
    }
}
