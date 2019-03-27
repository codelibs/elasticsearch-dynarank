package org.codelibs.elasticsearch.dynarank.painless;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.codelibs.elasticsearch.dynarank.script.DynaRankScript;
import org.elasticsearch.painless.spi.PainlessExtension;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;

public class DynaRankWhitelistExtension implements PainlessExtension {

    private static final Whitelist WHITELIST =
            WhitelistLoader.loadFromResourceFiles(DynaRankWhitelistExtension.class, "dynarank_whitelist.txt");

    @Override
    public Map<ScriptContext<?>, List<Whitelist>> getContextWhitelists() {
        return Collections.singletonMap(DynaRankScript.CONTEXT, Collections.singletonList(WHITELIST));
    }

}
