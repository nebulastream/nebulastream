package nes.calcite.physical;

/** An end of a network channel between two workers (NetworkSink sends, NetworkSource receives); see Placement. */
public interface ChannelEnd extends NesOperator {
    String channel();

    /** Whether it sends over the channel (else it receives). */
    boolean sends();
}
