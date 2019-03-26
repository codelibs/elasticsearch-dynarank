package org.codelibs.elasticsearch.dynarank.filter;

import org.codelibs.elasticsearch.dynarank.ranker.DynamicRanker;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;

public class SearchActionFilter implements ActionFilter {

    public static Setting<Integer> SETTING_DYNARANK_FILTER_ORDER = Setting.intSetting("dynarank.filter.order", 10, Property.NodeScope);

    private final int order;

    public SearchActionFilter(final Settings settings) {
        order = SETTING_DYNARANK_FILTER_ORDER.get(settings);
    }

    @Override
    public int order() {
        return order;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(final Task task, final String action,
            final Request request, final ActionListener<Response> listener, final ActionFilterChain<Request, Response> chain) {
        if (!SearchAction.INSTANCE.name().equals(action)) {
            chain.proceed(task, action, request, listener);
            return;
        }

        final SearchRequest searchRequest = (SearchRequest) request;
        final ActionListener<Response> wrappedListener = DynamicRanker.getInstance().wrapActionListener(action, searchRequest, listener);
        chain.proceed(task, action, request, wrappedListener == null ? listener : wrappedListener);
    }
}
