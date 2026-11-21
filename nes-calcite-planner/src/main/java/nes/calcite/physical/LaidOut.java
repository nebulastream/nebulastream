package nes.calcite.physical;

import nes.calcite.proto.Layout;

/** An operator that writes or reads buffers between pipelines in a layout (Scan, Emit, NetworkSink), set by Layouts. */
public interface LaidOut extends NesOperator {
    /** The layout, null before Layouts set it. */
    Layout layout();

    void layOut(Layout layout);

    /** The layout, which Layouts has set. */
    default Layout requireLayout() {
        if (layout() == null) {
            throw new IllegalStateException(getRelTypeName() + " has no layout: Layouts.assign did not reach it");
        }
        return layout();
    }
}
