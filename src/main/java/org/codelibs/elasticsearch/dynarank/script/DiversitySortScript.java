package org.codelibs.elasticsearch.dynarank.script;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import org.codelibs.elasticsearch.dynarank.DynamicRankingException;
import org.codelibs.elasticsearch.dynarank.script.bucket.BucketFactory;
import org.codelibs.elasticsearch.dynarank.script.bucket.Buckets;
import org.codelibs.elasticsearch.dynarank.script.bucket.impl.StandardBucketFactory;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.AbstractExecutableScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

public class DiversitySortScript extends AbstractExecutableScript {

    private static ESLogger logger = ESLoggerFactory
            .getLogger("script.dynarank.sort");

    public static final String SCRIPT_NAME = "dynarank_diversity_sort";

    private static final String STANDARD = "standard";

    private Map<String, Object> params;

    private Map<String, BucketFactory> bucketFactories;

    public static class Factory implements NativeScriptFactory {
        private Map<String, BucketFactory> bucketFactories;

        @Inject
        public Factory(final Settings settings) {
            final Settings bucketSettings = settings
                    .getByPrefix("script.dynarank.bucket.");

            bucketFactories = new HashMap<>();
            bucketFactories.put(STANDARD, new StandardBucketFactory(settings));

            final ImmutableMap<String, String> bucketFactorySettings = bucketSettings
                    .getAsMap();
            for (final Map.Entry<String, String> entry : bucketFactorySettings
                    .entrySet()) {
                final String name = entry.getKey();
                try {
                    @SuppressWarnings("unchecked")
                    final Class<BucketFactory> clazz = (Class<BucketFactory>) Class
                            .forName(entry.getValue());
                    final Class<?>[] types = new Class<?>[] { Settings.class };
                    final Constructor<BucketFactory> constructor = clazz
                            .getConstructor(types);

                    final Object[] args = new Object[] { settings };
                    constructor.newInstance(args);
                } catch (final Exception e) {
                    logger.warn("BucketFactory {} is not found.", e, name);
                }
            }
        }

        @Override
        public ExecutableScript newScript(
                @Nullable final Map<String, Object> params) {
            return new DiversitySortScript(params, bucketFactories);
        }
    }

    public DiversitySortScript(final Map<String, Object> params,
            final Map<String, BucketFactory> bucketFactories) {
        this.params = params;
        this.bucketFactories = bucketFactories;
    }

    @Override
    public Object run() {
        Object bucketFactoryName = params.get("bucket_factory");
        if (bucketFactoryName == null) {
            bucketFactoryName = STANDARD;
        }
        final BucketFactory bucketFactory = bucketFactories
                .get(bucketFactoryName);
        if (bucketFactory == null) {
            throw new DynamicRankingException("bucket_factory is invalid: "
                    + bucketFactoryName);
        }

        final Buckets buckets = bucketFactory.createBucketList(params);
        return buckets.getHits();
    }

}
