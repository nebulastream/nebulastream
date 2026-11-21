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
            if (next.root() instanceof ChannelEnd end && end.sends()) {
                sent.add(end.channel());
            }
        }
        return ordered;
    }

    private static List<String> received(RelNode node) {
        RelNode stripped = Pipelines.strip(node);
        if (stripped instanceof ChannelEnd end && !end.sends()) {
            return List.of(end.channel());
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
            if (!(stripped instanceof NesOperator operator)) {
                throw new IllegalStateException("not an engine operator: " + stripped);
            }
            return physical(operator);
        }

        /// A physical operator as an extension relation whose detail is its PhysicalOperator. Its inputs are written
        /// first, so that each pipeline is known before the operators that continue it.
        private Rel physical(NesOperator node) {
            List<Rel> inputs = node.getInputs().stream().map(this::apply).toList();
            int pipeline = node.startsPipeline() ? ids.nextPipeline++ : pipelines.get(Pipelines.strip(node.getInput(0)));
            pipelines.put(node, pipeline);
            List<Long> inputOrigins = node.getInputs().stream().flatMap(input -> origins.get(Pipelines.strip(input)).stream())
                .distinct().toList();

            Type.Struct outputType = (Type.Struct) typeConverter.toSubstrait(node.getRowType());
            PhysicalOperator.Builder operator = PhysicalOperator.newBuilder().setPipeline(pipeline);
            if (host != null) {
                operator.setHost(host);
            }
            operator.addAllOutputSchema(fields(node.getRowType()));
            /// The expressions are written with the plan's converter, which declares their functions in the plan.
            List<java.util.function.Consumer<RelProtoConverter>> expressions = new java.util.ArrayList<>();
            node.describe(operator, new NesOperator.Serialization() {
                @Override
                public long newOrigin() {
                    long origin = ids.nextOriginId++;
                    origins.put(node, List.of(origin));
                    return origin;
                }

                @Override
                public List<Long> inputOrigins() {
                    return inputOrigins;
                }

                @Override
                public void sends(String channel) {
                    ids.channels.put(channel, inputOrigins);
                }

                @Override
                public List<Long> receives(String channel) {
                    List<Long> sent = ids.channels.get(channel);
                    origins.put(node, sent);
                    return sent;
                }

                @Override
                public PhysicalOperator.Window window(NesPhysical.WindowState state) {
                    return Visitor.this.window(state);
                }

                @Override
                public PhysicalOperator.Join join(NesPhysical.JoinState state) {
                    return Visitor.this.join(state);
                }

                @Override
                public List<PhysicalOperator.Field> fields(RelDataType rowType) {
                    return Visitor.fields(rowType);
                }

                @Override
                public void expression(RexNode rex, java.util.function.Consumer<io.substrait.proto.Expression> set) {
                    Expression expression = toExpression(rex);
                    expressions.add(converter -> set.accept(converter.getExpressionProtoConverter().toProto(expression)));
                }
            });
            origins.putIfAbsent(node, inputOrigins);
            PhysicalPayload payload = new PhysicalPayload(converter -> {
                expressions.forEach(set -> set.accept(converter));
                return operator.build();
            }, outputType);
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
