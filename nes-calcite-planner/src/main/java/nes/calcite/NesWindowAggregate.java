package nes.calcite;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import nes.calcite.proto.WindowAggregate;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A windowed aggregation that NebulaStream can execute (WindowAggregateRule, GlobalWindowRules made it). It is a Calcite
 * Aggregate, so that Calcite's rules and metadata handle it as an aggregation (e.g. its unique keys are its group keys).
 * Windows.lower turns it into NesLogical.WindowAggregation for the placement and the physical operators.
 *
 * <p>Either a TUMBLE/HOP window: the input is the window table function, whose input are the rows (only columns, as our
 * aggregations read fields) and whose last two columns are window_start and window_end. Both are group keys, as in
 * Calcite's plan of the query; so the output is: the other group keys, window_start, window_end, the aggregates.
 *
 * <p>Or a GLOBAL window (only for finite inputs): one window over the whole input, which closes at the end of the stream.
 * The input are the rows, with a constant event time as their last column (GlobalWindowRules.withConstantTime). The
 * output is: the group keys, the aggregates (an aggregation without group keys returns one row, as in SQL).
 */
public final class NesWindowAggregate extends Aggregate {
    final boolean global;

    NesWindowAggregate(RelOptCluster cluster, RelTraitSet traits, RelNode input, ImmutableBitSet groupSet,
                       List<AggregateCall> aggCalls, boolean global) {
        super(cluster, traits, List.of(), input, groupSet, null, aggCalls);
        this.global = global;
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
                          @Nullable List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        if (groupSets != null && !groupSets.equals(List.of(groupSet))) {
            throw new UnsupportedOperationException("a window aggregation has no grouping sets");
        }
        return new NesWindowAggregate(getCluster(), traitSet, input, groupSet, aggCalls, global);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).itemIf("window", "GLOBAL", global);
    }

    /** The window table function (TUMBLE or HOP) below; null for a GLOBAL window. */
    @Nullable TableFunctionScan windowFunction() {
        if (global) {
            return null;
        }
        if (!(strip(getInput()) instanceof TableFunctionScan function)) {
            throw new IllegalStateException("a window aggregation reads its window table function directly, not " + getInput());
        }
        return function;
    }

    /** The rows that the window aggregates: the input of TUMBLE/HOP, or the input of a GLOBAL window. */
    RelNode rows() {
        TableFunctionScan function = windowFunction();
        return function == null ? getInput() : strip(function.getInputs().get(0));
    }

    /** The same aggregation over other rows (with the same columns). */
    NesWindowAggregate withRows(RelNode rows) {
        TableFunctionScan function = windowFunction();
        RelNode input = function == null ? rows : function.copy(function.getTraitSet(), List.of(rows));
        return (NesWindowAggregate) copy(getTraitSet(), List.of(input));
    }

    /** The group keys that are columns of the rows, in output order (without window_start and window_end). */
    List<Integer> keys() {
        int width = rows().getRowType().getFieldCount();
        return groupSet.asList().stream().filter(key -> key < width).toList();
    }

    /** The output column of window_start (window_end is the next); -1 for a GLOBAL window. */
    int startColumn() {
        return global ? -1 : keys().size();
    }

    /** NebulaStream's description of the window, its group keys and aggregations; its fields are columns of rows(). */
    WindowAggregate spec() {
        WindowAggregate.Builder spec = WindowAggregate.newBuilder();
        TableFunctionScan function = windowFunction();
        if (function == null) {
            spec.setKind(WindowAggregate.Kind.GLOBAL).setTimeField(rows().getRowType().getFieldCount() - 1)
                .setEmitOnEmptyInput(groupSet.isEmpty());
        } else {
            int width = rows().getRowType().getFieldCount();
            if (!(function.getCall() instanceof RexCall call) || !WindowAggregateRule.parseWindow(call, spec)
                || !groupSet.get(width) || !groupSet.get(width + 1)) {
                throw new IllegalStateException("not a TUMBLE/HOP window grouped by window_start and window_end: " + this);
            }
        }
        spec.addAllGroupKeys(keys());
        List<String> names = getRowType().getFieldNames();
        for (int i = 0; i < aggCalls.size(); i++) {
            AggregateCall call = aggCalls.get(i);
            String function1 = WindowAggregateRule.toFunction(call);
            if (function1 == null) {
                throw new IllegalStateException("NebulaStream has no aggregation for " + call);
            }
            WindowAggregate.Aggregate.Builder aggregate = WindowAggregate.Aggregate.newBuilder().setFunction(function1);
            if (!call.getArgList().isEmpty()) {
                aggregate.setField(call.getArgList().get(0));
            }
            spec.addAggregates(WindowAggregateRule.withType(aggregate, call.getType()).setName(names.get(groupSet.cardinality() + i)));
        }
        /// Our aggregation needs at least one aggregate: one without (Calcite's deduplication for IN/EXISTS) gets a
        /// COUNT(*), which nothing reads.
        if (spec.getAggregatesCount() == 0) {
            RelDataType count = getCluster().getTypeFactory().createSqlType(org.apache.calcite.sql.type.SqlTypeName.BIGINT);
            spec.addAggregates(WindowAggregateRule.withType(WindowAggregate.Aggregate.newBuilder().setFunction("COUNT"), count)
                .setName("$f_count"));
        }
        return spec.build();
    }

    /**
     * The window table function over these rows: as the given one (TUMBLE or HOP, same parameters), with the event time
     * in column timeField of the rows, and these names of window_start and window_end. It passes every column of the rows
     * on (Calcite's column origins see that).
     */
    static TableFunctionScan windowFunction(TableFunctionScan template, RelNode rows, int timeField, String startName, String endName) {
        RexCall call = (RexCall) template.getCall();
        RexCall descriptor = (RexCall) call.getOperands().get(0);
        List<RexNode> operands = new ArrayList<>(call.getOperands());
        operands.set(0, descriptor.clone(descriptor.getType(), List.of(RexInputRef.of(timeField, rows.getRowType()))));
        List<RelDataTypeField> fields = template.getRowType().getFieldList();
        RelDataTypeFactory.Builder rowType = template.getCluster().getTypeFactory().builder().addAll(rows.getRowType().getFieldList());
        rowType.add(startName, fields.get(fields.size() - 2).getType()).add(endName, fields.get(fields.size() - 1).getType());
        RelDataType type = rowType.uniquify().build();
        Set<RelColumnMapping> mappings = new java.util.LinkedHashSet<>();
        for (int i = 0; i < rows.getRowType().getFieldCount(); i++) {
            mappings.add(new RelColumnMapping(i, 0, i, false));
        }
        return LogicalTableFunctionScan.create(template.getCluster(), List.of(rows), call.clone(type, operands), null, type, mappings);
    }

    /** Our names of the window columns (NesNames). */
    static final List<String> WINDOW_COLUMNS = List.of("START", "END");

    /** The row type with these names, made unique (a later duplicate gets a suffix). */
    static RelDataType withNames(RelDataTypeFactory types, RelDataType rowType, List<String> names) {
        return types.createStructType(rowType.getFieldList().stream().map(RelDataTypeField::getType).toList(),
            SqlValidatorUtil.uniquify(names, true));
    }

    private static RelNode strip(RelNode node) {
        return node instanceof HepRelVertex vertex ? vertex.getCurrentRel() : node;
    }
}
