package nes.calcite.sql;

import java.math.BigDecimal;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * Checks number literals before Calcite validates the query. An integer above BIGINT that fits 64 bits would be a BIGINT
 * UNSIGNED, which the engine cannot receive as a literal yet: it is rejected with that reason (instead of as a DECIMAL).
 * Fractional literals stay DECIMAL, which we reject on purpose (no exact decimal type): write 1.5E0 for a DOUBLE.
 */
public final class NumericLiterals {
    private NumericLiterals() {}

    private static final BigDecimal MAX_UNSIGNED = BigDecimal.valueOf(2).pow(64).subtract(BigDecimal.ONE);

    public static SqlNode check(SqlNode query) {
        return query.accept(new SqlShuttle() {
            @Override
            public SqlNode visit(SqlLiteral literal) {
                if (!(literal instanceof SqlNumericLiteral number) || !number.isExact()) {
                    return literal;
                }
                BigDecimal value = number.getValueAs(BigDecimal.class);
                if (value.scale() <= 0 && value.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0 && value.compareTo(MAX_UNSIGNED) <= 0) {
                    /// A BIGINT UNSIGNED, but literals cross into the engine as a cast of a signed BIGINT (see Unsigned).
                    throw new UnsupportedOperationException("unsigned literals above 2^63 - 1 are not supported: " + value);
                }
                return literal;
            }
        });
    }
}
