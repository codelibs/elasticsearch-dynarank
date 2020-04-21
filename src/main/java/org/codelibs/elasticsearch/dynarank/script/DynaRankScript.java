package org.codelibs.elasticsearch.dynarank.script;

import java.util.Map;

import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.SearchHit;

public abstract class DynaRankScript {

    protected final Map<String, Object> params;

    public DynaRankScript(final Map<String, Object> params) {
        this.params = params;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public abstract SearchHit[] execute(final SearchHit[] searchHits);

    public interface Factory {
        DynaRankScript newInstance(final Map<String, Object> params);
    }

    public static final String[] PARAMETERS = {"searchHits"};
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("dynarank", Factory.class);
}
