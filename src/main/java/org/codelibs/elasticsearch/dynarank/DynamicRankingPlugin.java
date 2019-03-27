package org.codelibs.elasticsearch.dynarank;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.codelibs.elasticsearch.dynarank.filter.SearchActionFilter;
import org.codelibs.elasticsearch.dynarank.ranker.DynamicRanker;
import org.codelibs.elasticsearch.dynarank.script.DiversitySortScriptEngine;
import org.codelibs.elasticsearch.dynarank.script.DynaRankScript;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;

public class DynamicRankingPlugin extends Plugin implements ActionPlugin, ScriptPlugin {

    private Settings settings;

    public DynamicRankingPlugin(final Settings settings) {
        this.settings = settings;
    }

    @Override
    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
        return new DiversitySortScriptEngine(settings);
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        return Arrays.asList(new SearchActionFilter(settings));
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        final Collection<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        services.add(DynamicRanker.class);
        return services;
    }

    @Override
    public List<ScriptContext<?>> getContexts() {
        return Arrays.asList(DynaRankScript.CONTEXT);
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
