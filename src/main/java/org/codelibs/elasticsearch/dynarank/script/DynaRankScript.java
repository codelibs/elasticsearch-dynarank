package org.codelibs.elasticsearch.dynarank.script;

import java.util.Map;

import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.SearchHit;

public abstract class DynaRankScript {
    protected final Map<String, Object> vars;

    public DynaRankScript(final Map<String, Object> vars) {
        this.vars = vars;
    }

    public abstract SearchHit[] execute();

    public interface Factory {
        DynaRankScript newInstance(Map<String, Object> params);
    }

    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("dynarank", Factory.class);
}
