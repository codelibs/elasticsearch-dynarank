package org.codelibs.dynarank.ranker;

import static org.elasticsearch.action.search.ShardSearchFailure.readShardSearchFailure;
import static org.elasticsearch.search.internal.InternalSearchHits.readSearchHits;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.dynarank.DynamicRankingException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.settings.DynamicSettings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
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

public class DynamicRanker extends AbstractComponent {

    public static final String DEFAULT_SCRIPT_TYPE = "inline";

    public static final String DEFAULT_SCRIPT_LANG = "groovy";

    public static final String INDEX_DYNARANK_SCRIPT = "index.dynarank.script_sort.script";

    public static final String INDEX_DYNARANK_SCRIPT_LANG = "index.dynarank.script_sort.lang";

    public static final String INDEX_DYNARANK_SCRIPT_TYPE = "index.dynarank.script_sort.type";

    public static final String INDEX_DYNARANK_SCRIPT_PARAMS = "index.dynarank.script_sort.params.";

    public static final String INDEX_DYNARANK_REORDER_SIZE = "index.dynarank.reorder_size";

    public static final String INDICES_DYNARANK_REORDER_SIZE = "indices.dynarank.reorder_size";

    private ClusterService clusterService;

    private Integer defaultReorderSize;

    private ScriptService scriptService;

    private static DynamicRanker INSTANCE;

    @Inject
    public DynamicRanker(final Settings settings,
            final DynamicSettings dynamicSettings,
            final ClusterService clusterService,
            final ScriptService scriptService) {
        super(settings);
        this.clusterService = clusterService;
        this.scriptService = scriptService;

        logger.info("Initializing DynamicRankingService");

        defaultReorderSize = settings.getAsInt(INDICES_DYNARANK_REORDER_SIZE,
                200);

        dynamicSettings.addDynamicSettings("index.dynarank.*");

        INSTANCE = this;
    }

    public static DynamicRanker get() {
        return INSTANCE;
    }

    public ActionListener<SearchResponse> wrapActionListener(
            final SearchRequest request,
            final ActionListener<SearchResponse> listener) {
        switch (request.searchType()) {
        case DFS_QUERY_AND_FETCH:
        case QUERY_AND_FETCH:
        case QUERY_THEN_FETCH:
            break;
        default:
            return null;
        }

        final BytesReference source = request.source();
        if (source == null) {
            return null;
        }

        final String[] indices = request.indices();
        if (indices == null || indices.length == 0) {
            return null;
        }
        final ScriptInfo[] scriptInfos = new ScriptInfo[indices.length];
        int maxReorderSize = -1;
        for (int i = 0; i < indices.length; i++) {
            final IndexMetaData index = clusterService.state().getMetaData()
                    .index(indices[i]);
            if (index == null) {
                return null;
            }

            final Settings indexSettings = index.settings();
            final String script = indexSettings.get(INDEX_DYNARANK_SCRIPT);
            if (script == null) {
                return null;
            }
            scriptInfos[i] = new ScriptInfo(script, indexSettings.get(
                    INDEX_DYNARANK_SCRIPT_LANG, DEFAULT_SCRIPT_LANG),
                    indexSettings.get(INDEX_DYNARANK_SCRIPT_TYPE,
                            DEFAULT_SCRIPT_TYPE),
                    indexSettings.getByPrefix(INDEX_DYNARANK_SCRIPT_PARAMS));

            final Integer size = indexSettings.getAsInt(
                    INDEX_DYNARANK_REORDER_SIZE, -1);
            if (size > maxReorderSize) {
                maxReorderSize = size;
            }
        }

        if (maxReorderSize == -1) {
            maxReorderSize = defaultReorderSize;
        }

        final long startTime = System.nanoTime();

        try {
            final Map<String, Object> sourceAsMap = SourceLookup
                    .sourceAsMap(source);
            final int size = getInt(sourceAsMap.get("size"), 10);
            final int from = getInt(sourceAsMap.get("from"), 0);
            if (from >= maxReorderSize) {
                return null;
            }

            int maxSize = maxReorderSize;
            if (from + size > maxReorderSize) {
                maxSize = from + size;
            }
            sourceAsMap.put("size", maxSize);
            sourceAsMap.put("from", 0);

            final XContentBuilder builder = XContentFactory
                    .contentBuilder(Requests.CONTENT_TYPE);
            builder.map(sourceAsMap);
            request.source(builder.bytes(), true);

            return createSearchResponseListener(listener, from, size,
                    maxReorderSize, startTime, scriptInfos);
        } catch (final IOException e) {
            throw new DynamicRankingException("Failed to parse a source.", e);
        }
    }

    private ActionListener<SearchResponse> createSearchResponseListener(
            final ActionListener<SearchResponse> listener, final int from,
            final int size, final int reorderSize, final long startTime,
            final ScriptInfo[] scriptInfos) {
        return new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(final SearchResponse response) {
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
                            size, reorderSize, scriptInfos);
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
                    final InternalSearchResponse internalResponse = new InternalSearchResponse(
                            newHits, facets, aggregations, suggest, timedOut);
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
                        newResponse.getHeaders().putAll(headers);
                    }
                    listener.onResponse(newResponse);
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
            final ScriptInfo[] scriptInfos) {
        final InternalSearchHit[] searchHits = hits.internalHits();
        InternalSearchHit[] newSearchHits;
        if (searchHits.length <= reorderSize) {
            final InternalSearchHit[] targets = onReorder(searchHits,
                    scriptInfos);
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
            targets = onReorder(targets, scriptInfos);
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
            final ScriptInfo[] scriptInfos) {
        final Map<String, Object> vars = new HashMap<String, Object>();
        InternalSearchHit[] hits = searchHits;
        for (final ScriptInfo scriptInfo : scriptInfos) {
            vars.clear();
            vars.put("searchHits", hits);
            vars.putAll(scriptInfo.getSettings().getAsMap());
            final CompiledScript compiledScript = scriptService.compile(
                    scriptInfo.getLang(), scriptInfo.getScript(),
                    scriptInfo.getScriptType());
            hits = (InternalSearchHit[]) scriptService.execute(compiledScript,
                    vars);
        }
        return hits;
    }

    private int getInt(final Object value, final int defaultValue) {
        if (value instanceof Number) {
            return ((Number) value).intValue();
        } else if (value instanceof String) {
            return Integer.parseInt(value.toString());
        }
        return defaultValue;
    }

    private static class ScriptInfo {
        private String script;

        private String lang;

        private ScriptType scriptType;

        private Settings settings;

        ScriptInfo(final String script, final String lang,
                final String scriptType, final Settings settings) {
            this.script = script;
            this.lang = lang;
            this.settings = settings;
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

        public Settings getSettings() {
            return settings;
        }
    }
}
