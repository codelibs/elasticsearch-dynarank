package org.codelibs.dynarank;

import java.util.Collection;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;

import org.codelibs.dynarank.module.DynamicRankingModule;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.index.settings.IndexDynamicSettingsModule;
import org.elasticsearch.plugins.AbstractPlugin;

public class DynamicRankingPlugin extends AbstractPlugin {
    public DynamicRankingPlugin() throws Exception {
        final String transportSearchActionClsName = "org.elasticsearch.action.search.TransportSearchAction";
        final String searchRequestClsName = "org.elasticsearch.action.search.SearchRequest";
        final String actionListenerClsName = "org.elasticsearch.action.ActionListener";
        final String dynamicRankerClsName = "org.codelibs.dynarank.ranker.DynamicRanker";

        final ClassPool classPool = ClassPool.getDefault();
        final CtClass cc = classPool.get(transportSearchActionClsName);

        final CtMethod createAndPutContextMethod = cc.getDeclaredMethod(
                "doExecute",
                new CtClass[] { classPool.get(searchRequestClsName),
                        classPool.get(actionListenerClsName) });
        createAndPutContextMethod.insertBefore(//
                actionListenerClsName + " newListener=" + dynamicRankerClsName
                        + ".get().wrapActionListener($1,$2);"//
                        + "if(newListener!=null){$2=newListener;}"//
                );

        final ClassLoader classLoader = this.getClass().getClassLoader();
        cc.toClass(classLoader, this.getClass().getProtectionDomain());

    }

    @Override
    public String name() {
        return "DynamicRankingPlugin";
    }

    @Override
    public String description() {
        return "This plugin re-orders top N documents in a search results.";
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        final Collection<Class<? extends Module>> modules = Lists
                .newArrayList();
        modules.add(DynamicRankingModule.class);
        return modules;
    }

    public void onModule(final IndexDynamicSettingsModule module) {
        module.addDynamicSettings("index.dynarank.*");
    }

}
