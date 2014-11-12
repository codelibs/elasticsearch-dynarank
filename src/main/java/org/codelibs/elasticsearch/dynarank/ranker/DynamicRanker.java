package org.codelibs.elasticsearch.dynarank.ranker;

import static org.elasticsearch.action.search.ShardSearchFailure.readShardSearchFailure;
import static org.elasticsearch.search.internal.InternalSearchHits.readSearchHits;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.codelibs.elasticsearch.dynarank.DynamicRankingException;
import org.codelibs.elasticsearch.dynarank.filter.SearchActionFilter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.facet.InternalFacets;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.threadpool.ThreadPool;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class DynamicRanker extends AbstractLifecycleComponent<DynamicRanker> {

    public static final String DEFAULT_SCRIPT_TYPE = "inline";

    public static final String DEFAULT_SCRIPT_LANG = "groovy";

    public static final String INDEX_DYNARANK_SCRIPT = "index.dynarank.script_sort.script";

    public static final String INDEX_DYNARANK_SCRIPT_LANG = "index.dynarank.script_sort.lang";

    public static final String INDEX_DYNARANK_SCRIPT_TYPE = "index.dynarank.script_sort.type";

    public static final String INDEX_DYNARANK_SCRIPT_PARAMS = "index.dynarank.script_sort.params.";

    public static final String INDEX_DYNARANK_REORDER_SIZE = "index.dynarank.reorder_size";

    public static final String INDICES_DYNARANK_REORDER_SIZE = "indices.dynarank.reorder_size";

    public static final String INDICES_DYNARANK_CACHE_EXPIRE = "indices.dynarank.cache.expire";

    public static final String INDICES_DYNARANK_CACHE_CLEAN_INTERVAL = "indices.dynarank.cache.clean_interval";

    private ESLogger logger = ESLoggerFactory.getLogger("script.dynarank.sort");

    private ClusterService clusterService;

    private Integer defaultReorderSize;

    private ScriptService scriptService;

    private Cache<String, ScriptInfo> scriptInfoCache;

    private ThreadPool threadPool;

    private TimeValue cleanInterval;

    private Reaper reaper;

    @Inject
    public DynamicRanker(final Settings settings,
            final ClusterService clusterService,
            final ScriptService scriptService, final ThreadPool threadPool,
            final ActionFilters filters) {
        super(settings);
        this.clusterService = clusterService;
        this.scriptService = scriptService;
        this.threadPool = threadPool;

        logger.info("Initializing DynamicRanker");

        defaultReorderSize = settings.getAsInt(INDICES_DYNARANK_REORDER_SIZE,
                100);
        final TimeValue expire = settings.getAsTime(
                INDICES_DYNARANK_CACHE_EXPIRE, null);
        cleanInterval = settings.getAsTime(
                INDICES_DYNARANK_CACHE_CLEAN_INTERVAL,
                TimeValue.timeValueSeconds(60));

        final CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder()
                .concurrencyLevel(16);
        if (expire != null) {
            builder.expireAfterAccess(expire.millis(), TimeUnit.MILLISECONDS);
        }
        scriptInfoCache = builder.build();

        for (final ActionFilter filter : filters.filters()) {
            if (filter instanceof SearchActionFilter) {
                ((SearchActionFilter) filter).setDynamicRanker(this);
                if (logger.isDebugEnabled()) {
                    logger.debug("Set DynamicRanker to " + filter);
                }
            }
        }

    }

    @Override
    protected void doStart() throws ElasticsearchException {
        reaper = new Reaper();
        threadPool.schedule(cleanInterval, ThreadPool.Names.SAME, reaper);
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        reaper.close();
        scriptInfoCache.invalidateAll();
    }

    public ActionListener<SearchResponse> wrapActionListener(
            final String action, final SearchRequest request,
            final ActionListener<SearchResponse> listener) {
        switch (request.searchType()) {
        case DFS_QUERY_AND_FETCH:
        case QUERY_AND_FETCH:
        case QUERY_THEN_FETCH:
            break;
        default:
            return null;
        }

        final Object isRerank = request.getHeader("_rerank");
        if (isRerank instanceof Boolean && !((Boolean) isRerank).booleanValue()) {
            return null;
        }

        final BytesReference source = request.source();
        if (source == null) {
            return null;
        }

        final String[] indices = request.indices();
        if (indices == null || indices.length != 1) {
            return null;
        }

        final String index = indices[0];
        final ScriptInfo scriptInfo = getScriptInfo(index);
        if (scriptInfo == null) {
            return null;
        }

        final long startTime = System.nanoTime();

        try {
            final Map<String, Object> sourceAsMap = SourceLookup
                    .sourceAsMap(source);
            final int size = getInt(sourceAsMap.get("size"), 10);
            final int from = getInt(sourceAsMap.get("from"), 0);
            if (from >= scriptInfo.getReorderSize()) {
                return null;
            }

            int maxSize = scriptInfo.getReorderSize();
            if (from + size > scriptInfo.getReorderSize()) {
                maxSize = from + size;
            }
            sourceAsMap.put("size", maxSize);
            sourceAsMap.put("from", 0);

            if (logger.isDebugEnabled()) {
                logger.debug("Rewrite query: from:{}->{} size:{}->{}", from, 0,
                        size, maxSize);
            }

            final XContentBuilder builder = XContentFactory
                    .contentBuilder(Requests.CONTENT_TYPE);
            builder.map(sourceAsMap);
            request.source(builder.bytes(), true);

            return createSearchResponseListener(listener, from, size,
                    scriptInfo.getReorderSize(), startTime, scriptInfo);
        } catch (final IOException e) {
            throw new DynamicRankingException("Failed to parse a source.", e);
        }
    }

    public ScriptInfo getScriptInfo(final String index) {
        try {
            return scriptInfoCache.get(index, new Callable<ScriptInfo>() {
                @Override
                public ScriptInfo call() throws Exception {
                    final IndexMetaData indexMD = clusterService.state()
                            .getMetaData().index(index);
                    if (indexMD == null) {
                        return null;
                    }

                    final Settings indexSettings = indexMD.settings();
                    final String script = indexSettings
                            .get(INDEX_DYNARANK_SCRIPT);
                    if (script == null || script.length() == 0) {
                        return null;
                    }

                    return new ScriptInfo(script, indexSettings.get(
                            INDEX_DYNARANK_SCRIPT_LANG, DEFAULT_SCRIPT_LANG),
                            indexSettings.get(INDEX_DYNARANK_SCRIPT_TYPE,
                                    DEFAULT_SCRIPT_TYPE), indexSettings
                                    .getByPrefix(INDEX_DYNARANK_SCRIPT_PARAMS),
                            indexSettings.getAsInt(INDEX_DYNARANK_REORDER_SIZE,
                                    defaultReorderSize));
                }
            });
        } catch (final ExecutionException e) {
            throw new DynamicRankingException("Failed to load ScriptInfo for "
                    + index, e);
        }
    }

    private ActionListener<SearchResponse> createSearchResponseListener(
            final ActionListener<SearchResponse> listener, final int from,
            final int size, final int reorderSize, final long startTime,
            final ScriptInfo scriptInfo) {
        return new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(final SearchResponse response) {
                if (response.getHits().getTotalHits() == 0) {
                    listener.onResponse(response);
                }

                try {
                    final BytesStreamOutput out = new BytesStreamOutput();
                    response.writeTo(out);

                    final BytesStreamInput in = new BytesStreamInput(
                            out.bytes());
                    Map<String, Object> headers = null;
                    if (in.readBoolean()) {
                        headers = in.readMap();
                    }
                    final InternalSearchHits hits = readSearchHits(in);
                    final InternalSearchHits newHits = doReorder(hits, from,
                            size, reorderSize, scriptInfo);
                    InternalFacets facets = null;
                    if (in.readBoolean()) {
                        facets = InternalFacets.readFacets(in);
                    }
                    InternalAggregations aggregations = null;
                    if (in.readBoolean()) {
                        aggregations = InternalAggregations
                                .readAggregations(in);
                    }
                    Suggest suggest = null;
                    if (in.readBoolean()) {
                        suggest = Suggest.readSuggest(Suggest.Fields.SUGGEST,
                                in);
                    }
                    final boolean timedOut = in.readBoolean();
                    Boolean terminatedEarly = null;
                    if (in.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
                        terminatedEarly = in.readOptionalBoolean();
                    }
                    final InternalSearchResponse internalResponse = new InternalSearchResponse(
                            newHits, facets, aggregations, suggest, timedOut,
                            terminatedEarly);
                    final int totalShards = in.readVInt();
                    final int successfulShards = in.readVInt();
                    final int size = in.readVInt();
                    ShardSearchFailure[] shardFailures;
                    if (size == 0) {
                        shardFailures = ShardSearchFailure.EMPTY_ARRAY;
                    } else {
                        shardFailures = new ShardSearchFailure[size];
                        for (int i = 0; i < shardFailures.length; i++) {
                            shardFailures[i] = readShardSearchFailure(in);
                        }
                    }
                    final String scrollId = in.readOptionalString();
                    final long tookInMillis = (System.nanoTime() - startTime) / 1000000;

                    final SearchResponse newResponse = new SearchResponse(
                            internalResponse, scrollId, totalShards,
                            successfulShards, tookInMillis, shardFailures);
                    if (headers != null) {
                        for (final Map.Entry<String, Object> entry : headers
                                .entrySet()) {
                            newResponse.putHeader(entry.getKey(),
                                    entry.getValue());
                        }
                    }
                    listener.onResponse(newResponse);

                    if (logger.isDebugEnabled()) {
                        logger.debug("Rewriting overhead time: {} - {} = {}ms",
                                tookInMillis, response.getTookInMillis(),
                                tookInMillis - response.getTookInMillis());
                    }

                } catch (final Exception e) {
                    throw new DynamicRankingException(
                            "Failed to parse a search response.", e);
                }
            }

            @Override
            public void onFailure(final Throwable e) {
                listener.onFailure(e);
            }
        };
    }

    private InternalSearchHits doReorder(final InternalSearchHits hits,
            final int from, final int size, final int reorderSize,
            final ScriptInfo scriptInfo) {
        final InternalSearchHit[] searchHits = hits.internalHits();
        InternalSearchHit[] newSearchHits;
        if (searchHits.length <= reorderSize) {
            final InternalSearchHit[] targets = onReorder(searchHits,
                    scriptInfo);
            if (from >= targets.length) {
                throw new DynamicRankingException("Invalid argument: " + from
                        + " >= " + targets.length);
            }
            int end = from + size;
            if (end > targets.length) {
                end = targets.length;
            }
            newSearchHits = Arrays.copyOfRange(targets, from, end);
        } else {
            InternalSearchHit[] targets = Arrays.copyOfRange(searchHits, 0,
                    reorderSize);
            targets = onReorder(targets, scriptInfo);
            final List<SearchHit> list = new ArrayList<>(size);
            for (int i = from; i < targets.length; i++) {
                list.add(targets[i]);
            }
            for (int i = targets.length; i < searchHits.length; i++) {
                list.add(searchHits[i]);
            }
            newSearchHits = list.toArray(new InternalSearchHit[list.size()]);
        }
        return new InternalSearchHits(newSearchHits, hits.totalHits(),
                hits.maxScore());
    }

    private InternalSearchHit[] onReorder(final InternalSearchHit[] searchHits,
            final ScriptInfo scriptInfo) {
        final Map<String, Object> vars = new HashMap<String, Object>();
        final InternalSearchHit[] hits = searchHits;
        vars.clear();
        vars.put("searchHits", hits);
        vars.putAll(scriptInfo.getSettings());
        final CompiledScript compiledScript = scriptService.compile(
                scriptInfo.getLang(), scriptInfo.getScript(),
                scriptInfo.getScriptType());
        return (InternalSearchHit[]) scriptService.executable(compiledScript,
                vars).run();
    }

    private int getInt(final Object value, final int defaultValue) {
        if (value instanceof Number) {
            return ((Number) value).intValue();
        } else if (value instanceof String) {
            return Integer.parseInt(value.toString());
        }
        return defaultValue;
    }

    public static class ScriptInfo {
        private String script;

        private String lang;

        private ScriptType scriptType;

        private Map<String, Object> settings;

        private int reorderSize;

        ScriptInfo(final String script, final String lang,
                final String scriptType, final Settings settings,
                final int reorderSize) {
            this.script = script;
            this.lang = lang;
            this.reorderSize = reorderSize;
            this.settings = new HashMap<>();
            for (final String name : settings.names()) {
                final String value = settings.get(name);
                if (value != null) {
                    this.settings.put(name, value);
                } else {
                    this.settings.put(name, settings.getAsArray(name));
                }
            }
            if ("INDEXED".equalsIgnoreCase(scriptType)) {
                this.scriptType = ScriptType.INDEXED;
            } else if ("FILE".equalsIgnoreCase(scriptType)) {
                this.scriptType = ScriptType.FILE;
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

        @Override
        public String toString() {
            return "ScriptInfo [script=" + script + ", lang=" + lang
                    + ", scriptType=" + scriptType + ", settings=" + settings
                    + ", reorderSize=" + reorderSize + "]";
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
                for (final Map.Entry<String, ScriptInfo> entry : scriptInfoCache
                        .asMap().entrySet()) {
                    final String index = entry.getKey();

                    final IndexMetaData indexMD = clusterService.state()
                            .getMetaData().index(index);
                    if (indexMD == null) {
                        scriptInfoCache.invalidate(index);
                        if (logger.isDebugEnabled()) {
                            logger.debug("Invalidate cache for " + index);
                        }
                        continue;
                    }

                    final Settings indexSettings = indexMD.settings();
                    final String script = indexSettings
                            .get(INDEX_DYNARANK_SCRIPT);
                    if (script == null || script.length() == 0) {
                        scriptInfoCache.invalidate(index);
                        if (logger.isDebugEnabled()) {
                            logger.debug("Invalidate cache for " + index);
                        }
                        continue;
                    }

                    final ScriptInfo scriptInfo = new ScriptInfo(script,
                            indexSettings.get(INDEX_DYNARANK_SCRIPT_LANG,
                                    DEFAULT_SCRIPT_LANG), indexSettings.get(
                                    INDEX_DYNARANK_SCRIPT_TYPE,
                                    DEFAULT_SCRIPT_TYPE),
                            indexSettings
                                    .getByPrefix(INDEX_DYNARANK_SCRIPT_PARAMS),
                            indexSettings.getAsInt(INDEX_DYNARANK_REORDER_SIZE,
                                    defaultReorderSize));
                    if (logger.isDebugEnabled()) {
                        logger.debug("Reload cache for " + index + " => "
                                + scriptInfo);
                    }
                    scriptInfoCache.put(index, scriptInfo);
                }
            } catch (final Exception e) {
                logger.warn("Failed to update a cache for ScriptInfo.", e);
            } finally {
                threadPool.schedule(cleanInterval, ThreadPool.Names.GENERIC,
                        reaper);
            }

        }

    }

}