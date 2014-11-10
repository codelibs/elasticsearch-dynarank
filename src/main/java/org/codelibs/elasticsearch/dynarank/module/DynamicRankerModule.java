package org.codelibs.elasticsearch.dynarank.module;

import org.codelibs.elasticsearch.dynarank.ranker.DynamicRanker;
import org.elasticsearch.common.inject.AbstractModule;

public class DynamicRankerModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(DynamicRanker.class).asEagerSingleton();
    }

}
