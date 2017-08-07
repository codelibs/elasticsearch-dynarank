package org.codelibs.elasticsearch.dynarank.script;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.codelibs.elasticsearch.dynarank.script.bucket.BucketFactory;
import org.codelibs.elasticsearch.dynarank.script.bucket.Buckets;
import org.codelibs.elasticsearch.dynarank.script.bucket.impl.StandardBucketFactory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;

public class DiversitySortScriptEngineService extends AbstractComponent implements ScriptEngineService {

    public static final String SCRIPT_NAME = "dynarank_diversity_sort";

    private static final String STANDARD = "standard";

    public static final Setting<Settings> SETTING_SCRIPT_DYNARANK_BUCKET =
            Setting.groupSetting("script.dynarank.bucket.", Property.NodeScope);

    private Map<String, BucketFactory> bucketFactories;

    public DiversitySortScriptEngineService(final Settings settings) {
        super(settings);

        final Settings bucketSettings = SETTING_SCRIPT_DYNARANK_BUCKET.get(settings);

        bucketFactories = new HashMap<>();
        bucketFactories.put(STANDARD, new StandardBucketFactory(settings));

        final Map<String, String> bucketFactorySettings = bucketSettings.getAsMap();
        for (final Map.Entry<String, String> entry : bucketFactorySettings.entrySet()) {
            final String name = entry.getKey();
            try {
                bucketFactories.put(name, AccessController.doPrivileged(new PrivilegedAction<BucketFactory>() {
                    @Override
                    public BucketFactory run() {
                        try {
                            @SuppressWarnings("unchecked")
                            final Class<BucketFactory> clazz = (Class<BucketFactory>) Class.forName(entry.getValue());
                            final Class<?>[] types = new Class<?>[] { Settings.class };
                            final Constructor<BucketFactory> constructor = clazz.getConstructor(types);

                            final Object[] args = new Object[] { settings };
                            return constructor.newInstance(args);
                        } catch (final Exception e) {
                            throw new ElasticsearchException(e);
                        }
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
    public Object compile(String scriptName, String scriptSource, Map<String, String> params) {
        return scriptSource;
    }

    @Override
    public ExecutableScript executable(CompiledScript compiledScript, Map<String, Object> vars) {
        return new DiversitySortExecutableScript(vars, bucketFactories, logger);
    }

    @Override
    public SearchScript search(CompiledScript compiledScript, SearchLookup lookup, Map<String, Object> vars) {
        throw new UnsupportedOperationException();
    }

    private static class DiversitySortExecutableScript implements ExecutableScript {
        private Map<String, Object> vars;
        private Map<String, BucketFactory> bucketFactories;
        private Logger logger;

        public DiversitySortExecutableScript(Map<String, Object> vars, Map<String, BucketFactory> bucketFactories, Logger logger) {
            this.vars = vars;
            this.bucketFactories = bucketFactories;
            this.logger = logger;
        }

        @Override
        public void setNextVar(String name, Object value) {
        }

        @Override
        public Object run() {
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
