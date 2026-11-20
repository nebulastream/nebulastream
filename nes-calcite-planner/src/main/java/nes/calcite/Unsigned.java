package nes.calcite;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.UserTypeMapper;
import io.substrait.type.Type;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Unsigned integers, our UINT8..UINT64, as Calcite's UTINYINT..UBIGINT.
 *
 * In Substrait they are the user-defined types u8..u64 of extension:io.substrait:unsigned_integers, which also declares
 * their arithmetic and sum/min/max; nes_unsigned_integers.yaml adds comparisons and modulus.
 *
 * A mix of signed and unsigned values follows Calcite's types, which {@link #prepare} makes explicit for the engine: an
 * arithmetic operation computes in its result type (Calcite: the wider type, at equal width the unsigned one), so every
 * operand is cast to it; a comparison compares in the type Calcite cast the operands to (for U = I: CAST(U AS BIGINT) =
 * I). Casts between signed and unsigned types wrap, as in C (and our engine). Unsigned values of different widths are
 * widened, and a comparison of an unsigned value with non-negative literals stays exact (the literals are cast).
 */
final class Unsigned {
    static final String URN = "extension:io.substrait:unsigned_integers";
    private static final Map<SqlTypeName, String> NAMES = Map.of(
        SqlTypeName.UTINYINT, "u8", SqlTypeName.USMALLINT, "u16", SqlTypeName.UINTEGER, "u32", SqlTypeName.UBIGINT, "u64");

    static final TypeConverter TYPES = new TypeConverter(new UserTypeMapper() {
        @Override
        public Type toSubstrait(RelDataType type) {
            String name = NAMES.get(type.getSqlTypeName());
            return name == null ? null : Type.UserDefined.builder().urn(URN).name(name).nullable(type.isNullable()).build();
        }

        @Override
        public RelDataType toCalcite(Type.UserDefined type) {
            return null; // we only convert Calcite -> Substrait
        }
    });

    static final SimpleExtension.ExtensionCollection EXTENSIONS;

    static {
        try (InputStream unsigned = Unsigned.class.getResourceAsStream("/nes_unsigned_integers.yaml");
             InputStream functions = Unsigned.class.getResourceAsStream("/nes_functions.yaml")) {
            /// Also our functions for argument types Substrait's standard extensions lack (see NesConverterProvider).
            EXTENSIONS = DefaultExtensionCatalog.DEFAULT_COLLECTION.merge(SimpleExtension.load(unsigned))
                .merge(SimpleExtension.load(functions));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Operations whose operands we align; others with unsigned operands are left to the conversion (and fail there). */
    private static final Set<SqlKind> ALIGNED = EnumSet.of(SqlKind.PLUS, SqlKind.MINUS, SqlKind.TIMES, SqlKind.DIVIDE, SqlKind.MOD,
        SqlKind.EQUALS, SqlKind.NOT_EQUALS, SqlKind.LESS_THAN, SqlKind.LESS_THAN_OR_EQUAL, SqlKind.GREATER_THAN,
        SqlKind.GREATER_THAN_OR_EQUAL);

    private Unsigned() {}

    static boolean isUnsigned(RelDataType type) {
        return NAMES.containsKey(type.getSqlTypeName());
    }

    private static int bits(RelDataType type) {
        return switch (type.getSqlTypeName()) {
            case UTINYINT, TINYINT -> 8;
            case USMALLINT, SMALLINT -> 16;
            case UINTEGER, INTEGER -> 32;
            case UBIGINT, BIGINT -> 64;
            default -> Integer.MAX_VALUE;
        };
    }

    private static BigDecimal max(RelDataType unsigned) {
        return BigDecimal.valueOf(2).pow(bits(unsigned)).subtract(BigDecimal.ONE);
    }

    private static UnsupportedOperationException mixed(RexNode expression) {
        return new UnsupportedOperationException(
            "this mix of signed and unsigned values is not supported: "
                + expression);
    }

    /** A non-negative integer literal that fits the unsigned type, cast to it exactly; null for anything else. */
    private static RexNode castLiteral(RexBuilder rexBuilder, RexNode operand, RelDataType unsigned) {
        if (operand instanceof RexLiteral literal && SqlTypeFamily.EXACT_NUMERIC.contains(literal.getType())) {
            BigDecimal value = literal.getValueAs(BigDecimal.class);
            if (value != null && value.signum() >= 0 && value.stripTrailingZeros().scale() <= 0 && value.compareTo(max(unsigned)) <= 0) {
                return rexBuilder.makeAbstractCast(
                    rexBuilder.getTypeFactory().createTypeWithNullability(unsigned, literal.getType().isNullable()), literal, false);
            }
        }
        return null;
    }

    static RelNode prepare(RelNode rel) {
        RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        RexShuttle shuttle = new RexShuttle() {
            @Override
            public RexNode visitLiteral(RexLiteral literal) {
                if (!isUnsigned(literal.getType()) || literal.isNull()) {
                    return literal;
                }
                /// E.g. a folded CAST(7 AS BIGINT UNSIGNED): our conversion only knows the cast, not unsigned literals.
                BigDecimal value = literal.getValueAs(BigDecimal.class);
                if (value.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0) {
                    throw new UnsupportedOperationException("unsigned literals above 2^63 - 1 are not supported: " + literal);
                }
                RexNode signed = rexBuilder.makeExactLiteral(value, rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT));
                return rexBuilder.makeAbstractCast(literal.getType(), signed, false);
            }

            @Override
            public RexNode visitCall(RexCall original) {
                if (original.getKind() == SqlKind.CAST) {
                    checkCast(original);
                    return super.visitCall(original);
                }
                RexCall call = (RexCall) super.visitCall(uncastUnsigned(original));
                List<RexNode> operands = call.getOperands();
                if (!ALIGNED.contains(call.getKind()) || operands.stream().noneMatch(o -> isUnsigned(o.getType()))) {
                    return call;
                }
                if (operands.stream().anyMatch(o -> !isUnsigned(o.getType()) && castLiteral(rexBuilder, o,
                        operands.stream().map(RexNode::getType).filter(Unsigned::isUnsigned).findFirst().orElseThrow()) == null)) {
                    return castTo(call, commonType(call));
                }
                RelDataType widest = operands.stream().map(RexNode::getType).filter(Unsigned::isUnsigned)
                    .max((a, b) -> Integer.compare(bits(a), bits(b))).orElseThrow();
                List<RexNode> aligned = new ArrayList<>();
                for (RexNode operand : operands) {
                    RelDataType target = rexBuilder.getTypeFactory().createTypeWithNullability(widest, operand.getType().isNullable());
                    if (isUnsigned(operand.getType())) {
                        aligned.add(bits(operand.getType()) == bits(widest) ? operand : rexBuilder.makeAbstractCast(target, operand, false));
                        continue;
                    }
                    aligned.add(castLiteral(rexBuilder, operand, widest));
                }
                return call.clone(call.getType(), aligned);
            }

            /**
             * Calcite compares an unsigned value with a BIGINT literal as CAST(u AS BIGINT) < literal, which is wrong for
             * u >= 2^63. With only non-negative literals on the other side, the unsigned value is compared as it is
             * (the literals are cast to its type below).
             */
            private RexCall uncastUnsigned(RexCall call) {
                if (!ALIGNED.contains(call.getKind())) {
                    return call;
                }
                List<RexNode> operands = new ArrayList<>(call.getOperands());
                for (int i = 0; i < operands.size(); i++) {
                    if (operands.get(i) instanceof RexCall cast && cast.getKind() == SqlKind.CAST
                        && isUnsigned(cast.getOperands().get(0).getType()) && !isUnsigned(cast.getType())) {
                        RexNode unsigned = cast.getOperands().get(0);
                        boolean othersAreLiterals = true;
                        for (int j = 0; j < operands.size(); j++) {
                            othersAreLiterals &= j == i || castLiteral(rexBuilder, operands.get(j), unsigned.getType()) != null;
                        }
                        if (othersAreLiterals) {
                            operands.set(i, unsigned);
                        }
                    }
                }
                return operands.equals(call.getOperands()) ? call : (RexCall) call.clone(call.getType(), operands);
            }

            /** Calcite's type for a mix of signed and unsigned operands: the arithmetic's result, or their common type. */
            private RelDataType commonType(RexCall call) {
                if (SqlKind.BINARY_ARITHMETIC.contains(call.getKind())) {
                    return call.getType();
                }
                RelDataType common = rexBuilder.getTypeFactory().leastRestrictive(call.getOperands().stream().map(RexNode::getType).toList());
                if (common == null) {
                    throw mixed(call);
                }
                return common;
            }

            /**
             * The call with every operand of another type cast to the type (values wrap, as in C). Our engine computes an
             * arithmetic of narrow integers in a wider type (C's integer promotion); its result is cast to Calcite's type,
             * so that it wraps there as Calcite defines.
             */
            private RexNode castTo(RexCall call, RelDataType type) {
                List<RexNode> operands = new ArrayList<>();
                for (RexNode operand : call.getOperands()) {
                    RelDataType target = rexBuilder.getTypeFactory().createTypeWithNullability(type, operand.getType().isNullable());
                    operands.add(operand.getType().getSqlTypeName() == type.getSqlTypeName() ? operand
                        : rexBuilder.makeAbstractCast(target, operand, false));
                }
                RexNode result = call.clone(call.getType(), operands);
                return SqlKind.BINARY_ARITHMETIC.contains(call.getKind()) ? rexBuilder.makeAbstractCast(call.getType(), result, false) : result;
            }

            /** Casts between signed and unsigned types wrap, as in C; only DECIMAL stays unsupported. */
            private void checkCast(RexCall cast) {
                RelDataType from = cast.getOperands().get(0).getType();
                if ((isUnsigned(from) || isUnsigned(cast.getType()))
                    && (from.getSqlTypeName() == SqlTypeName.DECIMAL || cast.getType().getSqlTypeName() == SqlTypeName.DECIMAL)) {
                    throw mixed(cast);
                }
            }
        };
        return rel.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                return super.visit(other).accept(shuttle);
            }
        });
    }
}
