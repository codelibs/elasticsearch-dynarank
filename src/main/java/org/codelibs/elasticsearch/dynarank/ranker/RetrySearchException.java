package org.codelibs.elasticsearch.dynarank.ranker;

import org.elasticsearch.search.builder.SearchSourceBuilder;

public class RetrySearchException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private QueryRewriter rewriter;

    public RetrySearchException(QueryRewriter rewriter) {
        super();
        this.rewriter = rewriter;
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return null;
    }

    public SearchSourceBuilder rewrite(SearchSourceBuilder source) {
        return rewriter.rewrite(source);
    }

    public interface QueryRewriter {
        SearchSourceBuilder rewrite(SearchSourceBuilder source);
    }
}
