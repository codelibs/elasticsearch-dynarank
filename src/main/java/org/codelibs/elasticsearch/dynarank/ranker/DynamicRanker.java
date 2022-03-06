package org.codelibs.elasticsearch.dynarank.ranker;

import static org.elasticsearch.action.search.ShardSearchFailure.readShardSearchFailure;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.elasticsearch.dynarank.script.DiversitySortScriptEngine;
import org.codelibs.elasticsearch.dynarank.script.DynaRankScript;
import org.codelibs.elasticsearch.dynarank.script.DynaRankScript.Factory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponse.Clusters;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.threadpool.ThreadPool;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class DynamicRanker extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(DynamicRanker.class);

    private static DynamicRanker instance = null;

    public static final String DEFAULT_SCRIPT_TYPE = "inline";

    public static final String DEFAULT_SCRIPT_LANG = "painless";

    public static final Setting<String> SETTING_INDEX_DYNARANK_SCRIPT =
            Setting.simpleString("index.dynarank.script_sort.script", Property.IndexScope, Property.Dynamic);

    public static final Setting<String> SETTING_INDEX_DYNARANK_LANG =
            Setting.simpleString("index.dynarank.script_sort.lang", Property.IndexScope, Property.Dynamic);

    public static final Setting<String> SETTING_INDEX_DYNARANK_TYPE = new Setting<>("index.dynarank.script_sort.type",
            s -> DEFAULT_SCRIPT_TYPE, Function.identity(), Property.IndexScope, Property.Dynamic);

    public static final Setting<Settings> SETTING_INDEX_DYNARANK_PARAMS =
            Setting.groupSetting("index.dynarank.script_sort.params.", Property.IndexScope, Property.Dynamic);

    public static final Setting<Integer> SETTING_INDEX_DYNARANK_REORDER_SIZE =
            Setting.intSetting("index.dynarank.reorder_size", 100, Property.IndexScope, Property.Dynamic);

    public static final Setting<Integer> SETTING_INDEX_DYNARANK_KEEP_TOPN =
            Setting.intSetting("index.dynarank.keep_topn", 0, Property.IndexScope, Property.Dynamic);

    public static final Setting<TimeValue> SETTING_DYNARANK_CACHE_EXPIRE =
            Setting.timeSetting("dynarank.cache.expire", TimeValue.MINUS_ONE, Property.NodeScope);

    public static final Setting<TimeValue> SETTING_DYNARANK_CACHE_CLEAN_INTERVAL =
            Setting.timeSetting("dynarank.cache.clean_interval", TimeValue.timeValueSeconds(60), Property.NodeScope);

    public static final String DYNARANK_RERANK_ENABLE = "Dynarank-Rerank";

    public static final String DYNARANK_MIN_TOTAL_HITS = "Dynarank-Min-Total-Hits";

    private final ClusterService clusterService;

    private final ScriptService scriptService;

    private final Cache<String, ScriptInfo> scriptInfoCache;

    private final ThreadPool threadPool;

    private final NamedWriteableRegistry namedWriteableRegistry;

    private final TimeValue cleanInterval;

    private Reaper reaper;

    private final Client client;

    public static DynamicRanker getInstance() {
        return instance;
    }

    @Inject
    public DynamicRanker(final Settings settings, final Client client, final ClusterService clusterService,
            final ScriptService scriptService, final ThreadPool threadPool, final ActionFilters filters,
            final NamedWriteableRegistry namedWriteableRegistry) {
        this.client = client;
        this.clusterService = clusterService;
        this.scriptService = scriptService;
        this.threadPool = threadPool;
        this.namedWriteableRegistry = namedWriteableRegistry;

        logger.info("Initializing DynamicRanker");

        final TimeValue expire = SETTING_DYNARANK_CACHE_EXPIRE.get(settings);
        cleanInterval = SETTING_DYNARANK_CACHE_CLEAN_INTERVAL.get(settings);

        final CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder().concurrencyLevel(16);
        if (expire.millis() >= 0) {
            builder.expireAfterAccess(expire.millis(), TimeUnit.MILLISECONDS);
        }
        scriptInfoCache = builder.build();
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        instance = this;
        reaper = new Reaper();
        threadPool.schedule(reaper, cleanInterval, ThreadPool.Names.SAME);
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        reaper.close();
        scriptInfoCache.invalidateAll();
    }

    public <Response extends ActionResponse> ActionListener<Response> wrapActionListener(final String action, final SearchRequest request,
            final ActionListener<Response> listener) {
        switch (request.searchType()) {
        case DFS_QUERY_THEN_FETCH:
        case QUERY_THEN_FETCH:
            break;
        default:
            return null;
        }

        if (request.scroll() != null) {
            return null;
        }

        final ThreadContext threadContext = threadPool.getThreadContext();
        final String isRerank = threadContext.getHeader(DYNARANK_RERANK_ENABLE);
        if (isRerank != null && !Boolean.valueOf(isRerank)) {
            return null;
        }

        final SearchSourceBuilder source = request.source();
        if (source == null) {
            return null;
        }

        final String[] indices = request.indices();
        if (indices == null || indices.length != 1) {
            return null;
        }

        final String index = indices[0];
        final ScriptInfo scriptInfo = getScriptInfo(index);
        if (scriptInfo == null || scriptInfo.getScript() == null) {
            return null;
        }

        final long startTime = System.nanoTime();

        final int size = getInt(source.size(), 10);
        final int from = getInt(source.from(), 0);
        if (size < 0 || from < 0) {
            return null;
        }

        if (from >= scriptInfo.getReorderSize()) {
            return null;
        }

        int maxSize = scriptInfo.getReorderSize();
        if (from + size > scriptInfo.getReorderSize()) {
            maxSize = from + size;
        }
        source.size(maxSize);
        source.from(0);

        if (logger.isDebugEnabled()) {
            logger.debug("Rewrite query: from:{}->{} size:{}->{}", from, 0, size, maxSize);
        }

        final ActionListener<Response> searchResponseListener =
                createSearchResponseListener(request, listener, from, size,  startTime, scriptInfo);
        return new ActionListener<Response>() {
            @Override
            public void onResponse(final Response response) {
                try {
                    searchResponseListener.onResponse(response);
                } catch (final RetrySearchException e) {
                    threadPool.getThreadContext().putHeader(DYNARANK_RERANK_ENABLE, Boolean.FALSE.toString());
                    source.size(size);
                    source.from(from);
                    source.toString();
                    final SearchSourceBuilder newSource = e.rewrite(source);
                    if (newSource == null) {
                        throw new ElasticsearchException("Failed to rewrite source: " + source);
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("Original Query: \n{}\nRewrited Query: \n{}", source, newSource);
                    }
                    request.source(newSource);
                    @SuppressWarnings("unchecked")
                    final ActionListener<SearchResponse> actionListener = (ActionListener<SearchResponse>) listener;
                    client.search(request, actionListener);
                }
            }

            @Override
            public void onFailure(final Exception e) {
                searchResponseListener.onFailure(e);
            }
        };
    }

    public ScriptInfo getScriptInfo(final String index) {
        try {
            return scriptInfoCache.get(index, () -> {
                final Metadata metaData = clusterService.state().getMetadata();
                IndexAbstraction indexAbstraction = metaData.getIndicesLookup().get(index);
                if (indexAbstraction == null) {
                    return ScriptInfo.NO_SCRIPT_INFO;
                }

                final ScriptInfo[] scriptInfos = indexAbstraction.getIndices().stream()
                        .map(metaData::index)
                        .filter(idx -> SETTING_INDEX_DYNARANK_LANG.get(idx.getSettings()).length() > 0)
                        .map(idx ->
                            new ScriptInfo(SETTING_INDEX_DYNARANK_SCRIPT.get(idx.getSettings()), SETTING_INDEX_DYNARANK_LANG.get(idx.getSettings()),
                                    SETTING_INDEX_DYNARANK_TYPE.get(idx.getSettings()), SETTING_INDEX_DYNARANK_PARAMS.get(idx.getSettings()),
                                    SETTING_INDEX_DYNARANK_REORDER_SIZE.get(idx.getSettings()), SETTING_INDEX_DYNARANK_KEEP_TOPN.get(idx.getSettings()),
                                    idx.mapping())
                        )
                        .toArray(n -> new ScriptInfo[n]);

                if (scriptInfos.length == 0) {
                    return ScriptInfo.NO_SCRIPT_INFO;
                } else if (scriptInfos.length == 1) {
                    return scriptInfos[0];
                } else {
                    for (final ScriptInfo scriptInfo : scriptInfos) {
                        if (!scriptInfo.getLang().equals(DiversitySortScriptEngine.SCRIPT_NAME)) {
                            return ScriptInfo.NO_SCRIPT_INFO;
                        }
                    }
                    return scriptInfos[0];
                }
            });
        } catch (final Exception e) {
            logger.warn("Failed to load ScriptInfo for {}.", e, index);
            return null;
        }
    }

    private <Response extends ActionResponse> ActionListener<Response> createSearchResponseListener(final SearchRequest request,
            final ActionListener<Response> listener, final int from, final int size, final long startTime,
            final ScriptInfo scriptInfo) {
        return new ActionListener<Response>() {
            @Override
            public void onResponse(final Response response) {
                final SearchResponse searchResponse = (SearchResponse) response;
                final long totalHits = searchResponse.getHits().getTotalHits().value;
                if (totalHits == 0) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("totalHits is {}. No reranking results: {}", totalHits, searchResponse);
                    }
                    listener.onResponse(response);
                    return;
                }

                final String minTotalHitsValue = threadPool.getThreadContext().getHeader(DYNARANK_MIN_TOTAL_HITS);
                if (minTotalHitsValue != null) {
                    final long minTotalHits = Long.parseLong(minTotalHitsValue);
                    if (totalHits < minTotalHits) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("totalHits is {} < {}. No reranking results: {}", totalHits, minTotalHits, searchResponse);
                        }
                        listener.onResponse(response);
                        return;
                    }
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("Reranking results: {}", searchResponse);
                }

                try {
                    final BytesStreamOutput out = new BytesStreamOutput();
                    searchResponse.writeTo(out);

                    if (logger.isDebugEnabled()) {
                        logger.debug("Reading headers...");
                    }
                    final StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Reading hits...");
                    }
                    // BEGIN: SearchResponse#writeTo
                    // BEGIN: InternalSearchResponse#writeTo
                    final SearchHits hits = new SearchHits(in);
                    final SearchHits newHits = doReorder(hits, from, size, scriptInfo);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Reading aggregations...");
                    }
                    final InternalAggregations aggregations =  in.readBoolean() ? InternalAggregations.readFrom(in) : null;
                    if (logger.isDebugEnabled()) {
                        logger.debug("Reading suggest...");
                    }
                    final Suggest suggest = in.readBoolean() ? new Suggest(in) : null;
                    final boolean timedOut = in.readBoolean();
                    final Boolean terminatedEarly = in.readOptionalBoolean();
                    final SearchProfileResults profileResults = in.readOptionalWriteable(SearchProfileResults::new);
                    final int numReducePhases =  in.readVInt();

                    final SearchResponseSections internalResponse = new InternalSearchResponse(newHits, aggregations, suggest,
                            profileResults, timedOut, terminatedEarly, numReducePhases);
                    // END: InternalSearchResponse

                    final int totalShards = in.readVInt();
                    final int successfulShards = in.readVInt();
                    final int size = in.readVInt();
                    final ShardSearchFailure[] shardFailures;
                    if (size == 0) {
                        shardFailures = ShardSearchFailure.EMPTY_ARRAY;
                    } else {
                        shardFailures = new ShardSearchFailure[size];
                        for (int i = 0; i < shardFailures.length; i++) {
                            shardFailures[i] = readShardSearchFailure(in);
                        }
                    }
                    final Clusters clusters;
                    if (in.getVersion().onOrAfter(Version.V_6_1_0)) {
                        clusters = new Clusters(in.readVInt(), in.readVInt(), in.readVInt());
                    } else {
                        clusters = Clusters.EMPTY;
                    }
                    final String scrollId = in.readOptionalString();
                    /* tookInMillis = */ in.readVLong();
                    final int skippedShards = in.readVInt();
                    // END: SearchResponse

                    final long tookInMillis = (System.nanoTime() - startTime) / 1000000;

                    if (logger.isDebugEnabled()) {
                        logger.debug("Creating new SearchResponse...");
                    }
                    @SuppressWarnings("unchecked")
                    final Response newResponse = (Response) new SearchResponse(internalResponse, scrollId, totalShards, successfulShards,
                            skippedShards, tookInMillis, shardFailures, clusters);
                    listener.onResponse(newResponse);

                    if (logger.isDebugEnabled()) {
                        logger.debug("Rewriting overhead time: {} - {} = {}ms", tookInMillis, searchResponse.getTook().getMillis(),
                                tookInMillis - searchResponse.getTook().getMillis());
                    }
                } catch (final RetrySearchException e) {
                    throw e;
                } catch (final Exception e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Failed to parse a search response.", e);
                    }
                    throw new ElasticsearchException("Failed to parse a search response.", e);
                }
            }

            @Override
            public void onFailure(final Exception e) {
                listener.onFailure(e);
            }
        };
    }

    private SearchHits doReorder(final SearchHits hits, final int from, final int size,
            final ScriptInfo scriptInfo) {
        final SearchHit[] searchHits = hits.getHits();
        SearchHit[] newSearchHits;
        if (logger.isDebugEnabled()) {
            logger.debug("searchHits.length <= reorderSize: {}", searchHits.length <= scriptInfo.getReorderSize());
        }
        if (searchHits.length <= scriptInfo.getReorderSize()) {
            final SearchHit[] targets = onReorder(searchHits, scriptInfo);
            if (from >= targets.length) {
                newSearchHits = new SearchHit[0];
                if (logger.isDebugEnabled()) {
                    logger.debug("Invalid argument: {} >= {}", from, targets.length);
                }
            } else {
                int end = from + size;
                if (end > targets.length) {
                    end = targets.length;
                }
                newSearchHits = Arrays.copyOfRange(targets, from, end);
            }
        } else {
            SearchHit[] targets = Arrays.copyOfRange(searchHits, 0, scriptInfo.getReorderSize());
            targets = onReorder(targets, scriptInfo);
            final List<SearchHit> list = new ArrayList<>(size);
            for (int i = from; i < targets.length; i++) {
                list.add(targets[i]);
            }
            for (int i = targets.length; i < searchHits.length; i++) {
                list.add(searchHits[i]);
            }
            newSearchHits = list.toArray(new SearchHit[list.size()]);
        }
        return new SearchHits(newSearchHits, hits.getTotalHits(), hits.getMaxScore());
    }

    private SearchHit[] onReorder(final SearchHit[] searchHits,
            final ScriptInfo scriptInfo) {
        final int keepTopN = scriptInfo.getKeepTopN();
        if (searchHits.length <= keepTopN) {
            return searchHits;
        }
        final Factory factory = scriptService.compile(
                new Script(scriptInfo.getScriptType(), scriptInfo.getLang(),
                        scriptInfo.getScript(), scriptInfo.getSettings()),
                DynaRankScript.CONTEXT);
        if (keepTopN == 0) {
            return factory.newInstance(scriptInfo.getSettings())
                    .execute(searchHits);
        }
        final SearchHit[] hits = Arrays.copyOfRange(searchHits, keepTopN,
                searchHits.length);
        final SearchHit[] reordered = factory
                .newInstance(scriptInfo.getSettings()).execute(hits);
        for (int i = keepTopN; i < searchHits.length; i++) {
            searchHits[i] = reordered[i - keepTopN];
        }
        return searchHits;
    }

    private int getInt(final Object value, final int defaultValue) {
        if (value instanceof Number) {
            final int v = ((Number) value).intValue();
            if (v < 0) {
                return defaultValue;
            }
            return v;
        } else if (value instanceof String) {
            return Integer.parseInt(value.toString());
        }
        return defaultValue;
    }

    public static class ScriptInfo {
        protected final static ScriptInfo NO_SCRIPT_INFO = new ScriptInfo();

        private String script;

        private String lang;

        private ScriptType scriptType;

        private Map<String, Object> settings;

        private int reorderSize;

        private int keepTopN;

        ScriptInfo() {
            // nothing
        }

        ScriptInfo(final String script, final String lang, final String scriptType, final Settings settings, final int reorderSize, final int keepTopN, final MappingMetadata mappingMetadata) {
            this.script = script;
            this.lang = lang;
            this.reorderSize = reorderSize;
            this.keepTopN=keepTopN;
            this.settings = new HashMap<>();
            for (final String name : settings.keySet()) {
                final List<String> list = settings.getAsList(name);
                this.settings.put(name, list.toArray(new String[list.size()]));
            }
            this.settings.put("source_as_map", mappingMetadata.getSourceAsMap());
            if ("STORED".equalsIgnoreCase(scriptType)) {
                this.scriptType = ScriptType.STORED;
            } else {
                this.scriptType = ScriptType.INLINE;
            }
        }

        public String getScript() {
            return script;
        }

        public String getLang() {
            return lang;
        }

        public ScriptType getScriptType() {
            return scriptType;
        }

        public Map<String, Object> getSettings() {
            return settings;
        }


        public int getReorderSize() {
            return reorderSize;
        }

        public int getKeepTopN() {
            return keepTopN;
        }

        @Override
        public String toString() {
            return "ScriptInfo [script=" + script + ", lang=" + lang + ", scriptType=" + scriptType + ", settings=" + settings
                    + ", reorderSize=" + reorderSize   + ", keepTopN=" + keepTopN + "]";
        }
    }

    private class Reaper implements Runnable {
        private volatile boolean closed;

        void close() {
            closed = true;
        }

        @Override
        public void run() {
            if (closed) {
                return;
            }

            try {
                for (final Map.Entry<String, ScriptInfo> entry : scriptInfoCache.asMap().entrySet()) {
                    final String index = entry.getKey();

                    final IndexMetadata indexMD = clusterService.state().getMetadata().index(index);
                    if (indexMD == null) {
                        scriptInfoCache.invalidate(index);
                        if (logger.isDebugEnabled()) {
                            logger.debug("Invalidate cache for {}", index);
                        }
                        continue;
                    }

                    final Settings indexSettings = indexMD.getSettings();
                    final String script = SETTING_INDEX_DYNARANK_SCRIPT.get(indexSettings);
                    if (script == null || script.length() == 0) {
                        scriptInfoCache.invalidate(index);
                        if (logger.isDebugEnabled()) {
                            logger.debug("Invalidate cache for {}", index);
                        }
                        continue;
                    }

                    final ScriptInfo scriptInfo = new ScriptInfo(script, SETTING_INDEX_DYNARANK_LANG.get(indexSettings),
                            SETTING_INDEX_DYNARANK_TYPE.get(indexSettings), SETTING_INDEX_DYNARANK_PARAMS.get(indexSettings),
                            SETTING_INDEX_DYNARANK_REORDER_SIZE.get(indexSettings),   SETTING_INDEX_DYNARANK_KEEP_TOPN.get(indexSettings),
                            indexMD.mapping());
                    if (logger.isDebugEnabled()) {
                        logger.debug("Reload cache for {} => {}", index, scriptInfo);
                    }
                    scriptInfoCache.put(index, scriptInfo);
                }
            } catch (final Exception e) {
                logger.warn("Failed to update a cache for ScriptInfo.", e);
            } finally {
                threadPool.schedule(reaper, cleanInterval, ThreadPool.Names.GENERIC);
            }

        }

    }

}
