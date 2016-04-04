package org.codelibs.elasticsearch.dynarank;

import java.util.ArrayList;
import java.util.Collection;

import org.codelibs.elasticsearch.dynarank.filter.SearchActionFilter;
import org.codelibs.elasticsearch.dynarank.module.DynamicRankerModule;
import org.codelibs.elasticsearch.dynarank.ranker.DynamicRanker;
import org.codelibs.elasticsearch.dynarank.script.DiversitySortScript;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.settings.Validator;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptModule;

public class DynamicRankingPlugin extends Plugin {

    @Override
    public String name() {
        return "dynarank";
    }

    @Override
    public String description() {
        return "This plugin re-orders top N documents in a search results.";
    }

    public void onModule(final ScriptModule module) {
        module.registerScript(DiversitySortScript.SCRIPT_NAME,
                DiversitySortScript.Factory.class);
    }

    public void onModule(final ActionModule module) {
        module.registerFilter(SearchActionFilter.class);
    }

    public void onModule(final ClusterModule module) {
        module.registerIndexDynamicSetting(DynamicRanker.INDEX_DYNARANK_SCRIPT, Validator.EMPTY);
        module.registerIndexDynamicSetting(DynamicRanker.INDEX_DYNARANK_SCRIPT_LANG, Validator.EMPTY);
        module.registerIndexDynamicSetting(DynamicRanker.INDEX_DYNARANK_SCRIPT_TYPE, Validator.EMPTY);
        module.registerIndexDynamicSetting(DynamicRanker.INDEX_DYNARANK_SCRIPT_PARAMS + "*", Validator.EMPTY);
        module.registerIndexDynamicSetting(DynamicRanker.INDEX_DYNARANK_REORDER_SIZE, Validator.POSITIVE_INTEGER);
        module.registerClusterDynamicSetting(DynamicRanker.INDICES_DYNARANK_REORDER_SIZE, Validator.POSITIVE_INTEGER);
        module.registerClusterDynamicSetting(DynamicRanker.INDICES_DYNARANK_CACHE_EXPIRE, Validator.TIME);
        module.registerClusterDynamicSetting(DynamicRanker.INDICES_DYNARANK_CACHE_CLEAN_INTERVAL, Validator.TIME);
    }

    // for Service
    @Override
    public Collection<Module> nodeModules() {
        final Collection<Module> modules = new ArrayList<>();
        modules.add(new DynamicRankerModule());
        return modules;
    }

    // for Service
    @SuppressWarnings("rawtypes")
    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        final Collection<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        services.add(DynamicRanker.class);
        return services;
    }
}
