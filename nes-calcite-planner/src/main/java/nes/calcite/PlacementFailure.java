package nes.calcite;

/**
 * The plan cannot be placed on the topology, e.g. a source on a worker that is not in it, or too little capacity. The
 * frontend reports it as our PlacementFailure (2300), as our own placement does.
 */
public final class PlacementFailure extends RuntimeException {
    public PlacementFailure(String message) {
        super(message);
    }

    /** Whether the error, or one of its causes, is a placement failure. */
    static boolean in(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof PlacementFailure) {
                return true;
            }
        }
        return false;
    }
}
