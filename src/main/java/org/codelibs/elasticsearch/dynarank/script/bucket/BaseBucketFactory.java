package org.codelibs.elasticsearch.dynarank.script.bucket;

import org.elasticsearch.common.settings.Settings;

public abstract class BaseBucketFactory implements BucketFactory {

    protected Settings settings;

    public BaseBucketFactory(final Settings settings) {
        this.settings = settings;
    }
}
