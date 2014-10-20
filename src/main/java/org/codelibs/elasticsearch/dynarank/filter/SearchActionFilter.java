package org.codelibs.elasticsearch.dynarank.filter;

import org.codelibs.elasticsearch.dynarank.ranker.DynamicRanker;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptService;

public class SearchActionFilter extends AbstractComponent implements
        ActionFilter {

    private int order;

    private DynamicRanker dynamicRanker;

    private ThreadLocal<SearchType> currentSearchType = new ThreadLocal<>();

    @Inject
    public SearchActionFilter(final Settings settings,
            final ClusterService clusterService,
            final ScriptService scriptService) {
        super(settings);
        dynamicRanker = new DynamicRanker(settings, clusterService,
                scriptService);

        order = settings.getAsInt("indices.dynarank.filter.order", 10);
    }

    @Override
    public int order() {
        return order;
    }

    @Override
    public void apply(final String action, final ActionRequest request,
            final ActionListener listener, final ActionFilterChain chain) {
        if (!SearchAction.INSTANCE.name().equals(action)) {
            chain.proceed(action, request, listener);
            return;
        }

        final SearchRequest searchRequest = (SearchRequest) request;
        final SearchType searchType = currentSearchType.get();
        if (searchType == null) {
            try {
                currentSearchType.set(searchRequest.searchType());
                chain.proceed(action, request, listener);
            } finally {
                currentSearchType.remove();
            }
        } else {
            final ActionListener<SearchResponse> wrappedListener = dynamicRanker
                    .wrapActionListener(action, searchRequest, listener);
            chain.proceed(action, request, wrappedListener == null ? listener
                    : wrappedListener);
        }
    }

    @Override
    public void apply(final String action, final ActionResponse response,
            final ActionListener listener, final ActionFilterChain chain) {
        chain.proceed(action, response, listener);
    }

}
