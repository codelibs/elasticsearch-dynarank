package org.codelibs.dynarank.module;

import org.codelibs.dynarank.ranker.DynamicRanker;
import org.elasticsearch.common.inject.AbstractModule;

public class DynamicRankingModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(DynamicRanker.class).asEagerSingleton();
    }
}