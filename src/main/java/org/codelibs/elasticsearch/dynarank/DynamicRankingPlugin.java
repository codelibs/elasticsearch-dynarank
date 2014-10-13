package org.codelibs.elasticsearch.dynarank;

import org.codelibs.elasticsearch.dynarank.filter.SearchActionFilter;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.index.settings.IndexDynamicSettingsModule;
import org.elasticsearch.plugins.AbstractPlugin;

public class DynamicRankingPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "DynamicRankingPlugin";
    }

    @Override
    public String description() {
        return "This plugin re-orders top N documents in a search results.";
    }

    public void onModule(final ActionModule module) {
        module.registerFilter(SearchActionFilter.class);
    }

    public void onModule(final IndexDynamicSettingsModule module) {
        module.addDynamicSettings("index.dynarank.*");
    }

}
