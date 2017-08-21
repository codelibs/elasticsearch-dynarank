package org.codelibs.elasticsearch.dynarank.ranker;

import java.io.Serializable;

import org.elasticsearch.search.builder.SearchSourceBuilder;

public class RetrySearchException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final QueryRewriter rewriter;

    public RetrySearchException(final QueryRewriter rewriter) {
        super();
        this.rewriter = rewriter;
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return null;
    }

    public SearchSourceBuilder rewrite(final SearchSourceBuilder source) {
        return rewriter.rewrite(source);
    }

    public interface QueryRewriter extends Serializable {
        SearchSourceBuilder rewrite(SearchSourceBuilder source);
    }
}
