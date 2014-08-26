package org.codelibs.dynarank;

public class DynamicRankingException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public DynamicRankingException(final String message) {
        super(message);
    }

    public DynamicRankingException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
