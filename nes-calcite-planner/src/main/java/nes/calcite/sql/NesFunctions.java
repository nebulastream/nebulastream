package nes.calcite.sql;

import io.substrait.isthmus.expression.FunctionMappings;
import java.util.List;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.util.Optionality;

/**
 * Functions that NebulaStream has and Calcite's standard operator table lacks: the engine's own (PlanRequest.functions,
 * e.g. FROM_BASE64 and MEDIAN) and functions of Calcite's libraries.
 */
public final class NesFunctions {
    private NesFunctions() {}

    /** An aggregation of the engine (PlanRequest.functions), which a window computes under its name. */
    public static final class Aggregation extends SqlAggFunction {
        Aggregation(String name, org.apache.calcite.sql.type.SqlReturnTypeInference result,
                    org.apache.calcite.sql.type.SqlOperandTypeChecker operands) {
            super(name, null, SqlKind.OTHER_FUNCTION, result, null, operands, SqlFunctionCategory.USER_DEFINED_FUNCTION, false, false,
                Optionality.FORBIDDEN);
        }
    }

    /** The engine's functions and those of Calcite's libraries (MySQL, BigQuery) that the engine has. */
    public static SqlOperatorTable table(List<nes.calcite.proto.Function> functions) {
        List<org.apache.calcite.sql.SqlOperator> operators = new java.util.ArrayList<>(List.of(SqlLibraryOperators.TO_BASE64,
            SqlLibraryOperators.IS_NAN));
        operators.addAll(operators(functions));
        return SqlOperatorTables.of(operators);
    }

    private static List<org.apache.calcite.sql.SqlOperator> operators(List<nes.calcite.proto.Function> functions) {
        return functions.stream().map(function -> {
            org.apache.calcite.sql.type.SqlReturnTypeInference result = ReturnTypes.explicit(
                org.apache.calcite.sql.type.SqlTypeName.valueOf(function.getResult()))
                .andThen(org.apache.calcite.sql.type.SqlTypeTransforms.TO_NULLABLE);
            org.apache.calcite.sql.type.SqlOperandTypeChecker operands = OperandTypes.family(function.getOperandsList().stream()
                .map(org.apache.calcite.sql.type.SqlTypeFamily::valueOf).toList());
            return function.getAggregation() ? (org.apache.calcite.sql.SqlOperator) new Aggregation(function.getName(), result, operands)
                : new org.apache.calcite.sql.SqlFunction(function.getName(), SqlKind.OTHER_FUNCTION, result, null, operands,
                    SqlFunctionCategory.USER_DEFINED_FUNCTION);
        }).toList();
    }

    /** Substrait names of Calcite operators that isthmus does not map itself (declared in nes_functions.yaml). */
    private static final List<FunctionMappings.Sig> SUBSTRAIT_NAMES = List.of(
        FunctionMappings.s(SqlLibraryOperators.TIMESTAMP_MILLIS, "timestamp_millis"),
        FunctionMappings.s(SqlLibraryOperators.UNIX_MILLIS, "unix_millis"),
        FunctionMappings.s(SqlLibraryOperators.FORMAT_DATETIME, "format_datetime"),
        FunctionMappings.s(SqlLibraryOperators.TIMESTAMP, "timestamp"),
        FunctionMappings.s(SqlStdOperatorTable.OCTET_LENGTH, "octet_length"),
        FunctionMappings.s(SqlLibraryOperators.IS_NAN, "is_nan"),
        FunctionMappings.s(SqlLibraryOperators.TO_BASE64, "to_base64"));

    /**
     * The Substrait names of the operators: also the engine's scalar functions, under their lower-case names. (Calcite's
     * operators are equal if their class, name and kind are, so these match the operators of table.)
     */
    public static List<FunctionMappings.Sig> substraitNames(List<nes.calcite.proto.Function> functions) {
        List<FunctionMappings.Sig> names = new java.util.ArrayList<>(SUBSTRAIT_NAMES);
        for (org.apache.calcite.sql.SqlOperator operator : operators(functions)) {
            if (!(operator instanceof Aggregation)) {
                names.add(FunctionMappings.s(operator, operator.getName().toLowerCase(java.util.Locale.ROOT)));
            }
        }
        return names;
    }

    /** A Substrait extension that declares the engine's scalar functions (the engine checks the argument types itself). */
    public static io.substrait.extension.SimpleExtension.ExtensionCollection extension(List<nes.calcite.proto.Function> functions) {
        StringBuilder yaml = new StringBuilder("%YAML 1.2\n---\nurn: extension:nebulastream:engine_functions\nscalar_functions:\n");
        for (nes.calcite.proto.Function function : functions) {
            if (function.getAggregation()) {
                continue;
            }
            yaml.append("  - name: \"").append(function.getName().toLowerCase(java.util.Locale.ROOT)).append("\"\n    impls:\n      - args: [");
            for (int i = 0; i < function.getOperandsCount(); i++) {
                yaml.append(i == 0 ? "" : ", ").append("{value: any").append(i + 1).append("}");
            }
            yaml.append("]\n        return: ").append(substraitType(function.getResult())).append("\n");
        }
        return io.substrait.extension.SimpleExtension.load(
            new java.io.ByteArrayInputStream(yaml.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }

    private static String substraitType(String sqlType) {
        return switch (org.apache.calcite.sql.type.SqlTypeName.valueOf(sqlType)) {
            case BOOLEAN -> "boolean";
            case TINYINT -> "i8";
            case SMALLINT -> "i16";
            case INTEGER -> "i32";
            case BIGINT -> "i64";
            case REAL, FLOAT -> "fp32";
            case DOUBLE -> "fp64";
            case CHAR, VARCHAR -> "string";
            case BINARY, VARBINARY -> "binary";
            default -> throw new IllegalArgumentException("the result type " + sqlType + " of a function is not supported");
        };
    }

    /**
     * Calls that our engine computes differently: Calcite expands SQRT(x) to POWER(x, 0.5) with a DECIMAL 0.5, which we
     * cannot represent, so it becomes SQRT(x) again; ABS of an unsigned value is the value.
     */
    public static org.apache.calcite.rel.RelNode normalize(org.apache.calcite.rel.RelNode rel) {
        org.apache.calcite.rex.RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        org.apache.calcite.rex.RexShuttle shuttle = new org.apache.calcite.rex.RexShuttle() {
            @Override
            public org.apache.calcite.rex.RexNode visitCall(org.apache.calcite.rex.RexCall original) {
                org.apache.calcite.rex.RexCall call = (org.apache.calcite.rex.RexCall) super.visitCall(original);
                List<org.apache.calcite.rex.RexNode> operands = call.getOperands();
                if (call.getOperator() == SqlStdOperatorTable.POWER && operands.get(1) instanceof org.apache.calcite.rex.RexLiteral exponent
                    && exponent.getValueAs(java.math.BigDecimal.class) != null
                    && exponent.getValueAs(java.math.BigDecimal.class).compareTo(new java.math.BigDecimal("0.5")) == 0) {
                    return rexBuilder.makeCall(call.getType(), SqlStdOperatorTable.SQRT, List.of(operands.get(0)));
                }
                /// OCTET_LENGTH of a string: Calcite casts it to VARBINARY, which our engine does not have; ours takes the string.
                if (call.getOperator() == SqlStdOperatorTable.OCTET_LENGTH && operands.get(0) instanceof org.apache.calcite.rex.RexCall cast
                    && cast.getKind() == SqlKind.CAST && org.apache.calcite.sql.type.SqlTypeUtil.isCharacter(cast.getOperands().get(0).getType())) {
                    call = (org.apache.calcite.rex.RexCall) rexBuilder.makeCall(call.getType(), SqlStdOperatorTable.OCTET_LENGTH,
                        List.of(cast.getOperands().get(0)));
                }
                if (call.getOperator() == SqlStdOperatorTable.ABS && Unsigned.isUnsigned(operands.get(0).getType())) {
                    return rexBuilder.makeAbstractCast(call.getType(), operands.get(0), false);
                }
                return call;
            }
        };
        return rel.accept(new org.apache.calcite.rel.RelHomogeneousShuttle() {
            @Override
            public org.apache.calcite.rel.RelNode visit(org.apache.calcite.rel.RelNode other) {
                return super.visit(other).accept(shuttle);
            }
        });
    }
}
