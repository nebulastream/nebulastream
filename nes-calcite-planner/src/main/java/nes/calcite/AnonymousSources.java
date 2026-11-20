package nes.calcite;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import nes.calcite.proto.AnonymousSource;
import nes.calcite.proto.Field;
import nes.calcite.proto.FieldType;
import nes.calcite.proto.SourceType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Anonymous sources in FROM, like DuckDB's read_csv: a table function per source type with named arguments, e.g.
 * {@code TABLE(FILE(schema => 'id UINT64 NOT NULL, val INT32', file_path => 'data.csv', format => 'CSV'))}.
 * <ul>
 *   <li>schema (required): the rows, as in CREATE LOGICAL SOURCE: name type [NOT NULL], separated by commas;</li>
 *   <li>host: the worker that runs the source;</li>
 *   <li>format: the input formatter type (e.g. CSV), format_xxx: its option xxx (e.g. format_field_delimiter);</li>
 *   <li>every option of the source type under its name (e.g. file_path, generator_schema).</li>
 * </ul>
 * Calcite checks the argument names; the engine checks the values.
 */
final class AnonymousSources {
    private AnonymousSources() {}

    /// Parameter names are upper case, as the parser turns unquoted names (schema => ...) into.
    private static final String SCHEMA = "SCHEMA";
    private static final String HOST = "HOST";
    private static final String FORMAT = "FORMAT";
    private static final String FORMAT_PREFIX = "FORMAT_";

    /** The table functions of the request's source types. */
    static List<SqlOperator> functions(List<SourceType> types, List<String> formatOptions) {
        return types.stream().map(type -> (SqlOperator) new Function(type, formatOptions)).toList();
    }

    /** The table function of one source type. */
    static final class Function extends SqlFunction implements SqlTableFunction {
        final String type;
        final List<String> parameters;

        Function(SourceType type, List<String> formatOptions) {
            this(type.getName(), parameters(type, formatOptions));
        }

        private Function(String type, List<String> parameters) {
            super(type.toUpperCase(Locale.ROOT), SqlKind.OTHER_FUNCTION, ReturnTypes.CURSOR, null,
                OperandTypes.operandMetadata(parameters.stream().map(p -> SqlTypeFamily.ANY).toList(),
                    factory -> parameters.stream().map(p -> factory.createSqlType(SqlTypeName.ANY)).toList(),
                    parameters::get, i -> !parameters.get(i).equals(SCHEMA)),
                SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
            this.type = type;
            this.parameters = parameters;
        }

        private static List<String> parameters(SourceType type, List<String> formatOptions) {
            LinkedHashSet<String> names = new LinkedHashSet<>(List.of(SCHEMA, HOST, FORMAT));
            type.getOptionsList().forEach(option -> names.add(option.toUpperCase(Locale.ROOT)));
            formatOptions.forEach(option -> {
                if (!option.equalsIgnoreCase("TYPE")) {
                    names.add(FORMAT_PREFIX + option.toUpperCase(Locale.ROOT));
                }
            });
            return List.copyOf(names);
        }

        @Override
        public SqlReturnTypeInference getRowTypeInference() {
            return this::rowType;
        }

        /// Only the schema is required; any of the named arguments may follow.
        @Override
        public org.apache.calcite.sql.SqlOperandCountRange getOperandCountRange() {
            return org.apache.calcite.sql.type.SqlOperandCountRanges.between(1, parameters.size());
        }

        private RelDataType rowType(SqlOperatorBinding binding) {
            String schema = binding.getOperandLiteralValue(parameters.indexOf(SCHEMA), String.class);
            if (schema == null) {
                throw new IllegalArgumentException(getName() + " needs a schema => '...' argument");
            }
            return AnonymousSources.rowType(binding.getTypeFactory(), parseSchema(schema));
        }

        @Override
        public boolean argumentMustBeScalar(int ordinal) {
            return true;
        }
    }

    /**
     * The fields of a schema as in CREATE LOGICAL SOURCE, e.g. {@code id UINT64 NOT NULL, "name" VARSIZED}: unquoted names
     * upper case, quoted ones as written; nullable unless NOT NULL.
     */
    static List<Field> parseSchema(String schema) {
        List<Field> fields = new ArrayList<>();
        for (String definition : splitTopLevel(schema)) {
            String text = definition.trim();
            if (text.isEmpty()) {
                continue;
            }
            String name;
            String rest;
            if (text.startsWith("\"")) {
                int end = text.indexOf('"', 1);
                if (end < 0) {
                    throw new IllegalArgumentException("unterminated quoted field name in schema: " + definition);
                }
                name = text.substring(1, end);
                rest = text.substring(end + 1).trim();
            } else {
                String[] parts = text.split("\\s+", 2);
                name = parts[0].toUpperCase(Locale.ROOT);
                rest = parts.length > 1 ? parts[1].trim() : "";
            }
            String[] typeAndConstraint = rest.split("\\s+", 2);
            FieldType type;
            try {
                type = FieldType.valueOf(typeAndConstraint[0].toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("unknown type '" + typeAndConstraint[0] + "' in schema: " + definition, e);
            }
            boolean notNull = typeAndConstraint.length > 1 && typeAndConstraint[1].trim().equalsIgnoreCase("NOT NULL");
            if (typeAndConstraint.length > 1 && !notNull) {
                throw new IllegalArgumentException("expected NOT NULL or nothing after the type in schema: " + definition);
            }
            fields.add(Field.newBuilder().setName(name).setType(type).setNullable(!notNull).build());
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("the schema of an anonymous source has no fields");
        }
        return fields;
    }

    private static List<String> splitTopLevel(String text) {
        List<String> parts = new ArrayList<>();
        boolean quoted = false;
        int start = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '"') {
                quoted = !quoted;
            } else if (c == ',' && !quoted) {
                parts.add(text.substring(start, i));
                start = i + 1;
            }
        }
        parts.add(text.substring(start));
        return parts;
    }

    static RelDataType rowType(RelDataTypeFactory types, List<Field> fields) {
        RelDataTypeFactory.Builder row = types.builder();
        for (Field field : fields) {
            row.add(field.getName(),
                types.createTypeWithNullability(Planner.relType(types, field.getType(), field.getName()), field.getNullable()));
        }
        return row.build();
    }

    /** Reads an anonymous source; see AnonymousSource. */
    static final class NesAnonymousSource extends AbstractRelNode {
        final AnonymousSource spec;

        NesAnonymousSource(RelOptCluster cluster, RelTraitSet traits, AnonymousSource spec, RelDataType rowType) {
            super(cluster, traits);
            this.spec = spec;
            this.rowType = rowType;
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new NesAnonymousSource(getCluster(), traitSet, spec, rowType);
        }

        /** The worker that runs the source (the HOST argument). */
        String host() {
            String host = spec.getSourceOptionsMap().get(HOST);
            if (host == null || host.isEmpty()) {
                throw new IllegalArgumentException("the anonymous " + spec.getType() + " source has no host => '...' argument");
            }
            return host;
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            return super.explainTerms(pw).item("type", spec.getType()).item("options", spec.getSourceOptionsMap())
                .item("format", spec.getFormatOptionsMap());
        }
    }

    /** Turns the table function into the anonymous source. */
    static final RelOptRule RULE = new RelOptRule(RelOptRule.operand(TableFunctionScan.class, RelOptRule.none()), "NesAnonymousSourceRule") {
        @Override
        public void onMatch(RelOptRuleCall call) {
            TableFunctionScan scan = call.rel(0);
            if (!(scan.getCall() instanceof RexCall invocation) || !(invocation.getOperator() instanceof Function function)) {
                return;
            }
            AnonymousSource.Builder spec = AnonymousSource.newBuilder().setType(function.type);
            Map<String, String> sourceOptions = new LinkedHashMap<>();
            Map<String, String> formatOptions = new LinkedHashMap<>();
            List<RexNode> operands = invocation.getOperands();
            for (int i = 0; i < operands.size(); i++) {
                String parameter = function.parameters.get(i);
                RexNode operand = operands.get(i);
                if (operand.getKind() == SqlKind.DEFAULT) {
                    continue; // not given
                }
                if (!(operand instanceof RexLiteral literal)) {
                    throw new IllegalArgumentException(function.getName() + ": " + parameter + " must be a literal, not " + operand);
                }
                if (parameter.equals(SCHEMA)) {
                    spec.addAllSchema(parseSchema(literal.getValueAs(String.class)));
                } else if (parameter.equals(HOST)) {
                    sourceOptions.put("HOST", text(literal, false));
                } else if (parameter.equals(FORMAT)) {
                    formatOptions.put("TYPE", text(literal, false));
                } else if (parameter.startsWith(FORMAT_PREFIX)) {
                    formatOptions.put(parameter.substring(FORMAT_PREFIX.length()).toUpperCase(Locale.ROOT), text(literal, true));
                } else {
                    sourceOptions.put(parameter.toUpperCase(Locale.ROOT), text(literal, true));
                }
            }
            spec.putAllSourceOptions(sourceOptions).putAllFormatOptions(formatOptions);
            call.transformTo(new NesAnonymousSource(scan.getCluster(), scan.getTraitSet(), spec.build(), scan.getRowType()));
        }
    };

    /** A config literal: a string in single quotes (unless unquoted), a number or boolean as it is. */
    private static String text(RexLiteral literal, boolean quoteStrings) {
        if (literal.getTypeName().getFamily() == org.apache.calcite.sql.type.SqlTypeFamily.CHARACTER) {
            String value = literal.getValueAs(String.class);
            return quoteStrings ? "'" + value.replace("'", "''") + "'" : value;
        }
        Object value = literal.getValue();
        return value instanceof java.math.BigDecimal number ? number.stripTrailingZeros().toPlainString() : String.valueOf(literal.getValue2());
    }
}
