package nes.calcite.physical;

import nes.calcite.proto.Layout;

/** An operator whose buffers have a given layout (a source's format, the sink's), which its edge takes (Layouts). */
public interface LayoutSource extends NesOperator {
    /** sinkFormat: the format of the query's sink (empty or NATIVE: our row layout). */
    Layout layout(String sinkFormat);
}
