package nes.calcite.physical;

import nes.calcite.sql.NesFunctions;
import nes.calcite.sql.Unsigned;
import com.google.protobuf.Any;
import io.substrait.isthmus.ConverterProvider;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.expression.Expression;
import io.substrait.relation.Extension;
import io.substrait.relation.ExtensionLeaf;
import io.substrait.relation.ExtensionMulti;
import io.substrait.relation.ExtensionSingle;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.Type;
import java.util.List;
import nes.calcite.proto.FieldType;
import nes.calcite.proto.PhysicalOperator;
import nes.calcite.proto.WindowAggregate;
import nes.calcite.proto.WindowJoin;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

/** Isthmus converter that additionally emits {@link NesWindowAggregate} and {@link NesWindowJoin} as Substrait extension relations. */
public final class NesConverterProvider extends ConverterProvider {
    private final List<nes.calcite.proto.Function> functions;

    /** functions: the engine's functions (PlanRequest.functions). */
    public NesConverterProvider(List<nes.calcite.proto.Function> functions) {
        this(functions, Unsigned.EXTENSIONS.merge(NesFunctions.extension(functions)));
    }

    private NesConverterProvider(List<nes.calcite.proto.Function> functions, io.substrait.extension.SimpleExtension.ExtensionCollection extensions) {
        /// Unsigned integers as Substrait's u8..u64, see Unsigned.
        super(ConverterProvider.builder().extensions(extensions).typeConverter(Unsigned.TYPES)
            .typeFactory(io.substrait.isthmus.SubstraitTypeSystem.TYPE_FACTORY)
            .scalarFunctionConverter(new ScalarFunctions(extensions, NesFunctions.substraitNames(functions))));
        this.functions = functions;
    }

    /**
     * Isthmus's conversion of scalar functions with our names (NesFunctions). Substrait's EXTRACT of a unit from QUARTER to
     * DAY also takes the indexing of the unit, which isthmus gives for a date only: for a timestamp, it is given here (ONE).
     */
    private static final class ScalarFunctions extends io.substrait.isthmus.expression.ScalarFunctionConverter {
        private static final java.util.Set<org.apache.calcite.avatica.util.TimeUnitRange> INDEXED = java.util.EnumSet.of(
            org.apache.calcite.avatica.util.TimeUnitRange.QUARTER, org.apache.calcite.avatica.util.TimeUnitRange.MONTH,
            org.apache.calcite.avatica.util.TimeUnitRange.DAY, org.apache.calcite.avatica.util.TimeUnitRange.DOY,
            org.apache.calcite.avatica.util.TimeUnitRange.WEEK, org.apache.calcite.avatica.util.TimeUnitRange.DOW);

        private final io.substrait.extension.SimpleExtension.ScalarFunctionVariant extract;

        ScalarFunctions(io.substrait.extension.SimpleExtension.ExtensionCollection extensions, List<io.substrait.isthmus.expression.FunctionMappings.Sig> names) {
            super(extensions.scalarFunctions(), names, io.substrait.isthmus.SubstraitTypeSystem.TYPE_FACTORY, Unsigned.TYPES);
            extract = extensions.scalarFunctions().stream().filter(function -> function.key().equals("extract:req_req_pts")).findFirst()
                .orElseThrow();
        }

        @Override
        public java.util.Optional<Expression> convert(org.apache.calcite.rex.RexCall call, java.util.function.Function<RexNode, Expression> topLevelConverter) {
            if (call.getKind() == org.apache.calcite.sql.SqlKind.EXTRACT && call.getOperands().size() == 2
                && call.getOperands().get(0) instanceof org.apache.calcite.rex.RexLiteral unit
                && call.getOperands().get(1).getType().getSqlTypeName() == org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP
                && INDEXED.contains(unit.getValueAs(org.apache.calcite.avatica.util.TimeUnitRange.class))) {
                return java.util.Optional.of(Expression.ScalarFunctionInvocation.builder().declaration(extract)
                    .addArguments(io.substrait.expression.EnumArg.of(unit.getValueAs(org.apache.calcite.avatica.util.TimeUnitRange.class).name()))
                    .addArguments(io.substrait.expression.EnumArg.of("ONE"))
                    .addArguments(topLevelConverter.apply(call.getOperands().get(1)))
                    .outputType(Unsigned.TYPES.toSubstrait(call.getType())).build());
            }
            return super.convert(call, topLevelConverter);
        }
    }

    /** The layout of a Scan or Emit, which the memory layout phase (Layouts) sets on every one. */
    private static nes.calcite.proto.Layout laidOut(nes.calcite.proto.Layout layout, RelNode node) {
        if (layout == null) {
            throw new IllegalStateException("no memory layout for " + node.getRelTypeName() + " (Layouts.assign has not run)");
        }
        return layout;
    }

    /** Calcite's operators and ours (NesFunctions). */
    @Override
    public org.apache.calcite.sql.SqlOperatorTable getSqlOperatorTable() {
        /// Ours first: where a name is also isthmus's (e.g. FROM_BASE64), our definition is the one the query calls.
        return org.apache.calcite.sql.util.SqlOperatorTables.chain(NesFunctions.table(functions), super.getSqlOperatorTable());
    }

    public RelDataTypeFactory typeFactory() {
        return typeFactory;
    }

    /**
     * A placed physical plan: one root per part that runs on one worker (Placement.Fragment). Pipeline and origin ids
     * are unique across all parts, as a worker may run several of them as one plan. host: null if not placed.
     */
    public List<io.substrait.plan.Plan.Root> convertPhysical(List<Placement.Fragment> fragments) {
        Ids ids = new Ids();
        return upstreamFirst(fragments).stream().map(fragment -> {
            Visitor visitor = new Visitor(this, fragment.host(), ids);
            Rel rel = visitor.apply(fragment.root());
            return (io.substrait.plan.Plan.Root) io.substrait.plan.Plan.Root.builder().input(rel)
                .names(visitor.names(fragment.root())).build();
        }).toList();
    }

    /**
     * The parts in an order in which each part that sends over a channel comes before the part that receives it: rows
     * keep their origins over the network, so a NetworkSource has the origins of the channel's NetworkSink.
     */
    private static List<Placement.Fragment> upstreamFirst(List<Placement.Fragment> fragments) {
        List<Placement.Fragment> ordered = new java.util.ArrayList<>();
        java.util.Set<String> sent = new java.util.HashSet<>();
        List<Placement.Fragment> pending = new java.util.ArrayList<>(fragments);
        while (!pending.isEmpty()) {
            Placement.Fragment next = pending.stream().filter(f -> sent.containsAll(received(f.root()))).findFirst()
                .orElseThrow(() -> new IllegalStateException("the parts of the plan form a cycle"));
            pending.remove(next);
            ordered.add(next);
            if (next.root() instanceof NesPhysical.NetworkSink sink) {
                sent.add(sink.channel);
            }
        }
        return ordered;
    }

    private static List<String> received(RelNode node) {
        RelNode stripped = Pipelines.strip(node);
        if (stripped instanceof NesPhysical.NetworkSource source) {
            return List.of(source.channel);
        }
        return stripped.getInputs().stream().flatMap(input -> received(input).stream()).toList();
    }

    /** The next pipeline and origin id of a physical plan. */
    private static final class Ids {
        int nextPipeline = 0;
        long nextOriginId = 1;
        /// The origins of the rows sent over each channel.
        final java.util.Map<String, List<Long>> channels = new java.util.HashMap<>();
        /// The handler id of each window's state.
        final java.util.IdentityHashMap<NesPhysical.WindowState, Integer> windows = new java.util.IdentityHashMap<>();
        /// The handler id of each join's state.
        final java.util.IdentityHashMap<NesPhysical.JoinState, Integer> joins = new java.util.IdentityHashMap<>();
    }

    private static final class Visitor extends SubstraitRelVisitor {
        /// Physical plans: the pipeline of each operator, the worker that runs them, and the next ids.
        private final java.util.IdentityHashMap<RelNode, Integer> pipelines = new java.util.IdentityHashMap<>();
        /// The origins of the rows each operator receives (a source's or a window probe's).
        private final java.util.IdentityHashMap<RelNode, List<Long>> origins = new java.util.IdentityHashMap<>();
        private final String host;
        private final Ids ids;

        Visitor(ConverterProvider provider, String host, Ids ids) {
            super(provider);
            this.host = host;
            this.ids = ids;
        }

        List<String> names(RelNode root) {
            return typeConverter.toNamedStruct(root.getRowType()).names();
        }

        @Override
        public Rel apply(RelNode node) {
            RelNode stripped = Pipelines.strip(node);
            if (!(stripped instanceof NesPhysical.NesRel)) {
                throw new IllegalStateException("not a physical operator: " + stripped);
            }
            return physical(stripped);
        }

        /// A physical operator as an extension relation whose detail is its PhysicalOperator. Its inputs are written
        /// first, so that each pipeline is known before the operators that continue it.
        private Rel physical(RelNode node) {
            List<Rel> inputs = node.getInputs().stream().map(this::apply).toList();
            boolean startsPipeline = node instanceof NesPhysical.SourceScan || node instanceof NesPhysical.Scan
                || node instanceof NesPhysical.Sink || node instanceof NesPhysical.NetworkSource
                || node instanceof NesPhysical.NetworkSink || node instanceof NesPhysical.WindowProbe
                || node instanceof NesPhysical.JoinProbe;
            int pipeline = startsPipeline ? ids.nextPipeline++ : pipelines.get(Pipelines.strip(node.getInput(0)));
            pipelines.put(node, pipeline);
            List<Long> inputOrigins = node.getInputs().stream().flatMap(input -> origins.get(Pipelines.strip(input)).stream())
                .distinct().toList();

            Type.Struct outputType = (Type.Struct) typeConverter.toSubstrait(node.getRowType());
            PhysicalOperator.Builder operator = PhysicalOperator.newBuilder().setPipeline(pipeline);
            if (host != null) {
                operator.setHost(host);
            }
            for (RelDataTypeField field : node.getRowType().getFieldList()) {
                operator.addOutputSchema(PhysicalOperator.Field.newBuilder().setName(field.getName())
                    .setType(fieldType(field.getType())).setNullable(field.getType().isNullable()));
            }
            java.util.function.Function<RelProtoConverter, PhysicalOperator> build;
            if (node instanceof NesPhysical.SourceScan scan) {
                long origin = ids.nextOriginId++;
                origins.put(node, List.of(origin));
                PhysicalOperator.SourceScan.Builder sourceScan = PhysicalOperator.SourceScan.newBuilder().setOriginId(origin);
                if (scan.anonymous != null) {
                    sourceScan.setAnonymous(scan.anonymous);
                } else {
                    sourceScan.setPhysicalSourceId(scan.physicalSource.getId());
                }
                operator.setSourceScan(sourceScan);
                build = converter -> operator.build();
            } else if (node instanceof NesPhysical.Scan scan) {
                operator.setScan(PhysicalOperator.Scan.newBuilder().setLayout(laidOut(scan.layout, scan)));
                build = converter -> operator.build();
            } else if (node instanceof NesPhysical.Selection selection) {
                Expression predicate = toExpression(selection.condition);
                build = converter -> operator.setSelection(PhysicalOperator.Selection.newBuilder()
                    .setPredicate(converter.getExpressionProtoConverter().toProto(predicate))).build();
            } else if (node instanceof NesPhysical.Map map) {
                List<Expression> expressions = map.expressions.stream().map(this::toExpression).toList();
                build = converter -> {
                    PhysicalOperator.Map.Builder spec = PhysicalOperator.Map.newBuilder();
                    for (int i = 0; i < map.fields.size(); i++) {
                        spec.addAssignments(PhysicalOperator.Map.Assignment.newBuilder().setField(map.fields.get(i))
                            .setExpression(converter.getExpressionProtoConverter().toProto(expressions.get(i))));
                    }
                    return operator.setMap(spec).build();
                };
            } else if (node instanceof NesPhysical.InferModel infer) {
                operator.setInferModel(PhysicalOperator.InferModel.newBuilder().setModel(infer.model.getName())
                    .addAllInputFields(infer.inputFields));
                build = converter -> operator.build();
            } else if (node instanceof NesPhysical.Emit emit) {
                operator.setEmit(PhysicalOperator.Emit.newBuilder().setLayout(laidOut(emit.layout, emit)));
                build = converter -> operator.build();
            } else if (node instanceof NesPhysical.NetworkSource source) {
                /// The rows keep the origins (and sequence numbers and watermarks) they had on the sending worker.
                List<Long> sent = ids.channels.get(source.channel);
                origins.put(node, sent);
                operator.setNetworkSource(PhysicalOperator.NetworkSource.newBuilder().setChannel(source.channel)
                    .setUpstreamHost(source.upstreamHost).setOriginId(sent.get(0)));
                build = converter -> operator.build();
            } else if (node instanceof NesPhysical.WatermarkAssigner assigner) {
                operator.setWatermarkAssigner(PhysicalOperator.WatermarkAssigner.newBuilder().setTimeField(assigner.timeField));
                build = converter -> operator.build();
            } else if (node instanceof NesPhysical.WindowBuild window) {
                PhysicalOperator.WindowBuild.Builder spec = PhysicalOperator.WindowBuild.newBuilder()
                    .setWindow(window(window.window)).setTimeField(window.timeField).addAllKeyFields(window.keyFields)
                    .addAllAggregations(buildAggregations(window))
                    .addAllProbeSchema(fields(window.probeRowType));
                operator.setWindowBuild(spec);
                build = converter -> operator.build();
            } else if (node instanceof NesPhysical.WindowProbe probe) {
                long origin = ids.nextOriginId++;
                origins.put(node, List.of(origin));
                /// The aggregations as in its first build, whose input the engine reads the argument types from.
                NesPhysical.WindowBuild first = (NesPhysical.WindowBuild) Pipelines.strip(probe.getInputs().get(0));
                operator.setWindowProbe(PhysicalOperator.WindowProbe.newBuilder().setWindow(window(probe.window))
                    .setOutputOriginId(origin).addAllInputOriginIds(inputOrigins)
                    .addAllAggregations(buildAggregations(first)));
                build = converter -> operator.build();
            } else if (node instanceof NesPhysical.JoinBuild joinBuild) {
                operator.setJoinBuild(PhysicalOperator.JoinBuild.newBuilder().setJoin(join(joinBuild.join)).setRight(joinBuild.right)
                    .setTimeField(joinBuild.timeField).addAllStoredFields(joinBuild.storedFields));
                build = converter -> operator.build();
            } else if (node instanceof NesPhysical.JoinProbe probe) {
                long origin = ids.nextOriginId++;
                origins.put(node, List.of(origin));
                Expression condition = probe.condition == null ? null : toExpression(probe.condition);
                List<Expression> residual = probe.residual.stream().map(this::toExpression).toList();
                PhysicalOperator.JoinProbe.Builder spec = PhysicalOperator.JoinProbe.newBuilder().setJoin(join(probe.join))
                    .setOutputOriginId(origin).addAllInputOriginIds(inputOrigins);
                build = converter -> {
                    if (condition != null) {
                        spec.setCondition(converter.getExpressionProtoConverter().toProto(condition));
                    }
                    residual.forEach(r -> spec.addResidual(converter.getExpressionProtoConverter().toProto(r)));
                    return operator.setJoinProbe(spec).build();
                };
            } else if (node instanceof NesPhysical.NetworkSink sink) {
                ids.channels.put(sink.channel, inputOrigins);
                operator.setNetworkSink(PhysicalOperator.NetworkSink.newBuilder().setChannel(sink.channel).setLayout(laidOut(sink.layout, sink))
                    .setDownstreamHost(sink.downstreamHost));
                build = converter -> operator.build();
            } else if (node instanceof NesPhysical.Sink) {
                operator.setSink(PhysicalOperator.Sink.getDefaultInstance());
                build = converter -> operator.build();
            } else {
                throw new UnsupportedOperationException("no physical operator for " + node.getRelTypeName());
            }
            origins.putIfAbsent(node, inputOrigins);
            PhysicalPayload payload = new PhysicalPayload(build, outputType);
            return switch (inputs.size()) {
                case 0 -> ExtensionLeaf.from(payload).build();
                case 1 -> ExtensionSingle.from(payload, inputs.get(0)).build();
                default -> ExtensionMulti.from(payload, inputs).build();
            };
        }

        private PhysicalOperator.Join join(NesPhysical.JoinState state) {
            int handler = ids.joins.computeIfAbsent(state, s -> ids.joins.size());
            WindowJoin spec = state.spec;
            return PhysicalOperator.Join.newBuilder().setHandler(handler).setKind(spec.getKind()).setSizeMs(spec.getSizeMs())
                .setSlideMs(spec.getSlideMs()).setType(spec.getType()).setImplementation(spec.getImplementation())
                .addAllLeftSchema(fields(state.leftStored)).addAllRightSchema(fields(state.rightStored))
                .addAllLeftKeyFields(state.leftKeys).addAllRightKeyFields(state.rightKeys).build();
        }

        private PhysicalOperator.Window window(NesPhysical.WindowState state) {
            int handler = ids.windows.computeIfAbsent(state, s -> ids.windows.size());
            WindowAggregate spec = state.spec;
            boolean global = spec.getKind() == WindowAggregate.Kind.GLOBAL;
            return PhysicalOperator.Window.newBuilder().setHandler(handler).setKind(spec.getKind())
                .setSizeMs(global ? NesPhysical.GLOBAL_WINDOW_SIZE_MS : spec.getSizeMs())
                .setSlideMs(global ? NesPhysical.GLOBAL_WINDOW_SIZE_MS : spec.getSlideMs())
                .setEmitOnEmptyInput(spec.getEmitOnEmptyInput()).build();
        }

        /// arguments: per aggregation, its field in the build's input (-1: COUNT(*), which counts all rows).
        private static List<PhysicalOperator.WindowAggregation> aggregations(NesPhysical.WindowState state, List<Integer> arguments) {
            List<PhysicalOperator.WindowAggregation> result = new java.util.ArrayList<>();
            for (int i = 0; i < arguments.size(); i++) {
                PhysicalOperator.WindowAggregation.Builder aggregation = PhysicalOperator.WindowAggregation.newBuilder()
                    .setFunction(state.spec.getAggregates(i).getFunction());
                if (arguments.get(i) >= 0) {
                    aggregation.setField(arguments.get(i));
                } else {
                    aggregation.setIncludeNulls(true);
                }
                result.add(aggregation.build());
            }
            return result;
        }

        /// The aggregations of a build; COUNT(*) counts its time field, NULLs included.
        private static List<PhysicalOperator.WindowAggregation> buildAggregations(NesPhysical.WindowBuild build) {
            return aggregations(build.window, build.aggregationFields).stream()
                .map(a -> a.hasField() ? a : a.toBuilder().setField(build.timeField).build()).toList();
        }

        private static List<PhysicalOperator.Field> fields(RelDataType rowType) {
            return rowType.getFieldList().stream().map(field -> PhysicalOperator.Field.newBuilder().setName(field.getName())
                .setType(fieldType(field.getType())).setNullable(field.getType().isNullable()).build()).toList();
        }

        private static FieldType fieldType(RelDataType type) {
            return switch (type.getSqlTypeName()) {
                case TINYINT -> FieldType.INT8;
                case SMALLINT -> FieldType.INT16;
                case INTEGER -> FieldType.INT32;
                case BIGINT -> FieldType.INT64;
                case UTINYINT -> FieldType.UINT8;
                case USMALLINT -> FieldType.UINT16;
                case UINTEGER -> FieldType.UINT32;
                case UBIGINT -> FieldType.UINT64;
                case REAL -> FieldType.FLOAT32;
                case FLOAT, DOUBLE -> FieldType.FLOAT64;
                case BOOLEAN -> FieldType.BOOLEAN;
                case CHAR -> type.getPrecision() == 1 ? FieldType.CHAR : FieldType.VARSIZED;
                case VARCHAR, VARBINARY, BINARY -> FieldType.VARSIZED; // bytes are variable-sized data to us, as text is
                case TIMESTAMP -> FieldType.TIMESTAMP_MS;
                default -> throw new UnsupportedOperationException("no field type for " + type.getFullTypeString());
            };
        }
    }

    /** The PhysicalOperator detail of a physical operator; it is built when the plan is written, with its expressions. */
    private record PhysicalPayload(java.util.function.Function<RelProtoConverter, PhysicalOperator> build, Type.Struct outputType)
        implements Extension.LeafRelDetail, Extension.SingleRelDetail, Extension.MultiRelDetail {
        @Override
        public Type.Struct deriveRecordType() {
            return outputType;
        }

        @Override
        public Type.Struct deriveRecordType(Rel input) {
            return outputType;
        }

        @Override
        public Type.Struct deriveRecordType(List<Rel> inputs) {
            return outputType;
        }

        @Override
        public Any toProto(RelProtoConverter converter) {
            return Any.pack(build.apply(converter));
        }
    }
}
