package nes.calcite.physical;

import java.util.List;
import java.util.function.Consumer;
import nes.calcite.proto.PhysicalOperator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

/**
 * An engine operator (NesPhysical): what the pipelining (Pipelines), the memory layouts (Layouts) and the plan's output
 * (NesConverterProvider) need to know about it. A new operator implements this, and RowOperator, LaidOut, LayoutSource
 * or ChannelEnd where they apply; those classes need no change.
 */
public interface NesOperator extends NesPhysical.NesRel {
    /** Whether it starts a pipeline: a source, the reader of buffers (Scan), a probe, or a sink. */
    default boolean startsPipeline() {
        return false;
    }

    /** Whether it reads the buffers that its inputs write (a Scan, a sink): they share one layout (Layouts). */
    default boolean readsInputBuffers() {
        return false;
    }

    /**
     * An operator that continues a pipeline: the same operator over input, a copy of its input whose field i is field
     * fields[i] (Pipelines.finish resolves the helpers below it).
     */
    default NesOperator over(NesOperator input, int[] fields) {
        throw new UnsupportedOperationException(getRelTypeName() + " starts a pipeline");
    }

    /** Where each field of its row is in the row of copy (made by over): by default its own row, field i at i. */
    default int[] fields(int[] inputFields, NesOperator copy) {
        return java.util.stream.IntStream.range(0, copy.getRowType().getFieldCount()).toArray();
    }

    /** Its payload: sets its part of the PhysicalOperator. */
    void describe(PhysicalOperator.Builder operator, Serialization serialization);

    /** What describe needs from the plan's output (NesConverterProvider). */
    interface Serialization {
        /** A new origin id, which the operator's rows have (a source or a probe). */
        long newOrigin();

        /** The origins of the rows of its inputs. */
        List<Long> inputOrigins();

        /** The rows sent over the channel (by a NetworkSink): the origins of its inputs. */
        void sends(String channel);

        /** The rows received from the channel (by a NetworkSource): they keep the origins they were sent with. */
        List<Long> receives(String channel);

        /** The shared state of a window (its handler id and parameters). */
        PhysicalOperator.Window window(NesPhysical.WindowState state);

        /** The shared state of a join. */
        PhysicalOperator.Join join(NesPhysical.JoinState state);

        /** The fields of a row type, for a schema. */
        List<PhysicalOperator.Field> fields(RelDataType rowType);

        /** Sets the expression when the plan is written, which declares its functions. */
        void expression(RexNode expression, Consumer<io.substrait.proto.Expression> set);
    }
}
