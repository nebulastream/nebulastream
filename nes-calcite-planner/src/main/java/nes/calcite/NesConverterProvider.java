package nes.calcite;

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
final class NesConverterProvider extends ConverterProvider {
    NesConverterProvider() {
        /// Unsigned integers as Substrait's u8..u64, see Unsigned.
        super(ConverterProvider.builder().extensions(Unsigned.EXTENSIONS).typeConverter(Unsigned.TYPES));
    }

    RelDataTypeFactory typeFactory() {
        return typeFactory;
    }

    @Override
    public SubstraitRelVisitor getSubstraitRelVisitor() {
        return new Visitor(this);
    }

    /**
     * A placed physical plan: one root per part that runs on one worker (Placement.Fragment). Pipeline and origin ids
     * are unique across all parts, as a worker may run several of them as one plan. host: null if not placed.
     */
    List<io.substrait.plan.Plan.Root> convertPhysical(List<Placement.Fragment> fragments) {
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
    }

    private static final class Visitor extends SubstraitRelVisitor {
        /// Physical plans: the pipeline of each operator, the worker that runs them, and the next ids.
        private final java.util.IdentityHashMap<RelNode, Integer> pipelines = new java.util.IdentityHashMap<>();
        /// The origins of the rows each operator receives (a source's or a window probe's).
        private final java.util.IdentityHashMap<RelNode, List<Long>> origins = new java.util.IdentityHashMap<>();
        private final String host;
        private final Ids ids;

        Visitor(ConverterProvider provider) {
            this(provider, null, new Ids());
        }

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
            return stripped instanceof NesPhysical.NesRel ? physical(stripped) : super.apply(node);
        }

        /// A physical operator as an extension relation whose detail is its PhysicalOperator. Its inputs are written
        /// first, so that each pipeline is known before the operators that continue it.
        private Rel physical(RelNode node) {
            List<Rel> inputs = node.getInputs().stream().map(this::apply).toList();
            boolean startsPipeline = node instanceof NesPhysical.SourceScan || node instanceof NesPhysical.Scan
                || node instanceof NesPhysical.Sink || node instanceof NesPhysical.NetworkSource
                || node instanceof NesPhysical.NetworkSink || node instanceof NesPhysical.WindowProbe;
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
                operator.setSourceScan(PhysicalOperator.SourceScan.newBuilder()
                    .setPhysicalSourceId(scan.physicalSource.getId()).setOriginId(origin));
                build = converter -> operator.build();
            } else if (node instanceof NesPhysical.Scan scan) {
                operator.setScan(PhysicalOperator.Scan.newBuilder().setParsesSource(scan.parsesSource()));
                build = converter -> operator.build();
            } else if (node instanceof NesPhysical.Selection selection) {
                Expression predicate = toExpression(selection.condition);
                build = converter -> operator.setSelection(PhysicalOperator.Selection.newBuilder()
                    .setPredicate(converter.getExpressionProtoConverter().toProto(predicate))).build();
            } else if (node instanceof NesPhysical.Map map) {
                Expression expression = toExpression(map.expression);
                build = converter -> operator.setMap(PhysicalOperator.Map.newBuilder().setField(map.field)
                    .setExpression(converter.getExpressionProtoConverter().toProto(expression))).build();
            } else if (node instanceof NesPhysical.Emit emit) {
                operator.setEmit(PhysicalOperator.Emit.newBuilder().setForSink(emit.forSink));
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
            } else if (node instanceof NesPhysical.NetworkSink sink) {
                ids.channels.put(sink.channel, inputOrigins);
                operator.setNetworkSink(PhysicalOperator.NetworkSink.newBuilder().setChannel(sink.channel)
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

        private PhysicalOperator.Window window(NesPhysical.WindowState state) {
            int handler = ids.windows.computeIfAbsent(state, s -> ids.windows.size());
            WindowAggregate spec = state.spec;
            return PhysicalOperator.Window.newBuilder().setHandler(handler).setKind(spec.getKind())
                .setSizeMs(spec.getSizeMs()).setSlideMs(spec.getSlideMs()).build();
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
                case VARCHAR -> FieldType.VARSIZED;
                case TIMESTAMP -> FieldType.TIMESTAMP_MS;
                default -> throw new UnsupportedOperationException("no field type for " + type.getFullTypeString());
            };
        }

        @Override
        public Rel visitOther(RelNode node) {
            if (node instanceof NesWindowAggregate window) {
                Type.Struct outputType = (Type.Struct) typeConverter.toSubstrait(window.getRowType());
                return ExtensionSingle.from(new Payload(Any.pack(window.spec), outputType), apply(window.getInput())).build();
            }
            if (node instanceof NesWindowJoin join) {
                Type.Struct outputType = (Type.Struct) typeConverter.toSubstrait(join.getRowType());
                Expression condition = join.condition == null ? null : toExpression(join.condition);
                List<Expression> residual = join.residual.stream().map(this::toExpression).toList();
                return ExtensionMulti.from(
                        new JoinPayload(join.spec, condition, residual, outputType), apply(join.getLeft()), apply(join.getRight()))
                    .build();
            }
            return super.visitOther(node);
        }
    }

    /** The condition is serialized with the plan's converter, so its functions are declared in the plan's extensions. */
    private record JoinPayload(WindowJoin spec, Expression condition, List<Expression> residual, Type.Struct outputType)
        implements Extension.MultiRelDetail {
        @Override
        public Type.Struct deriveRecordType(List<Rel> inputs) {
            return outputType;
        }

        @Override
        public Any toProto(RelProtoConverter converter) {
            WindowJoin.Builder builder = spec.toBuilder();
            if (condition != null) {
                builder.setCondition(converter.getExpressionProtoConverter().toProto(condition));
            }
            residual.forEach(expression -> builder.addResidual(converter.getExpressionProtoConverter().toProto(expression)));
            return Any.pack(builder.build());
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

    private record Payload(Any detail, Type.Struct outputType) implements Extension.SingleRelDetail {
        @Override
        public Type.Struct deriveRecordType(Rel input) {
            return outputType;
        }

        @Override
        public Any toProto(RelProtoConverter converter) {
            return detail;
        }
    }
}
