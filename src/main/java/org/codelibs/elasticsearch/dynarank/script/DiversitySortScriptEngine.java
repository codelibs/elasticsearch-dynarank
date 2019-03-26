package org.codelibs.elasticsearch.dynarank.script;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.elasticsearch.dynarank.script.bucket.BucketFactory;
import org.codelibs.elasticsearch.dynarank.script.bucket.Buckets;
import org.codelibs.elasticsearch.dynarank.script.bucket.impl.StandardBucketFactory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.search.SearchHit;

public class DiversitySortScriptEngine implements ScriptEngine {
    private static final Logger logger = LogManager.getLogger(DiversitySortScriptEngine.class);

    public static final String SCRIPT_NAME = "dynarank_diversity_sort";

    private static final String STANDARD = "standard";

    public static final Setting<Settings> SETTING_SCRIPT_DYNARANK_BUCKET =
            Setting.groupSetting("script.dynarank.bucket.", Property.NodeScope);

    private Map<String, BucketFactory> bucketFactories;

    public DiversitySortScriptEngine(final Settings settings) {

        final Settings bucketSettings = SETTING_SCRIPT_DYNARANK_BUCKET.get(settings);

        bucketFactories = new HashMap<>();
        bucketFactories.put(STANDARD, new StandardBucketFactory(settings));

        for (final String name : bucketSettings.names()) {
            try {
                bucketFactories.put(name, AccessController.doPrivileged((PrivilegedAction<BucketFactory>) () -> {
                    try {
                        @SuppressWarnings("unchecked")
                        final Class<BucketFactory> clazz = (Class<BucketFactory>) Class.forName(bucketSettings.get(name));
                        final Class<?>[] types = new Class<?>[] { Settings.class };
                        final Constructor<BucketFactory> constructor = clazz.getConstructor(types);

                        final Object[] args = new Object[] { settings };
                        return constructor.newInstance(args);
                    } catch (final Exception e) {
                        throw new ElasticsearchException(e);
                    }
                }));
            } catch (final Exception e) {
                logger.warn("BucketFactory {} is not found.", e, name);
            }
        }
    }

    @Override
    public void close() throws IOException {
        // no-op
    }

    @Override
    public String getType() {
        return SCRIPT_NAME;
    }

    @Override
    public <T> T compile(String name, String code, ScriptContext<T> context, Map<String, String> options) {
        DynaRankScript.Factory compiled = params -> new DiversitySortExecutableScript(params, bucketFactories);
        return context.factoryClazz.cast(compiled);
    }

    private static class DiversitySortExecutableScript extends DynaRankScript {
        private final Map<String, BucketFactory> bucketFactories;

        public DiversitySortExecutableScript(final Map<String, Object> vars, final Map<String, BucketFactory> bucketFactories) {
            super(vars);
            this.bucketFactories = bucketFactories;
        }

        @Override
        public SearchHit[] execute() {
            if (logger.isDebugEnabled()) {
                logger.debug("Starting DiversitySortScript...");
            }
            Object bucketFactoryName = vars.get("bucket_factory");
            if (bucketFactoryName == null) {
                bucketFactoryName = STANDARD;
            }
            final BucketFactory bucketFactory = bucketFactories.get(bucketFactoryName);
            if (bucketFactory == null) {
                throw new ElasticsearchException("bucket_factory is invalid: " + bucketFactoryName);
            }

            final Buckets buckets = bucketFactory.createBucketList(vars);
            return buckets.getHits();
        }

    }
}
