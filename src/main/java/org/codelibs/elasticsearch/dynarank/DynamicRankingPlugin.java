package org.codelibs.elasticsearch.dynarank;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.codelibs.elasticsearch.dynarank.filter.SearchActionFilter;
import org.codelibs.elasticsearch.dynarank.ranker.DynamicRanker;
import org.codelibs.elasticsearch.dynarank.script.DiversitySortScriptEngineService;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ScriptEngineService;

public class DynamicRankingPlugin extends Plugin implements ActionPlugin, ScriptPlugin {

    @Override
    public ScriptEngineService getScriptEngineService(Settings settings) {
        return new DiversitySortScriptEngineService(settings);
    }

    @Override
    public List<Class<? extends ActionFilter>> getActionFilters() {
        return Arrays.asList(SearchActionFilter.class);
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        Collection<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        services.add(DynamicRanker.class);
        return services;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(//
                DynamicRanker.SETTING_INDEX_DYNARANK_SCRIPT, //
                DynamicRanker.SETTING_INDEX_DYNARANK_LANG, //
                DynamicRanker.SETTING_INDEX_DYNARANK_TYPE, //
                DynamicRanker.SETTING_INDEX_DYNARANK_PARAMS, //
                DynamicRanker.SETTING_INDEX_DYNARANK_REORDER_SIZE, //
                DynamicRanker.SETTING_DYNARANK_CACHE_CLEAN_INTERVAL, //
                DynamicRanker.SETTING_DYNARANK_CACHE_EXPIRE //
        );
    }
}
