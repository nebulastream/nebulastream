package nes.calcite.sql;

import java.util.Locale;
import java.util.Set;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * Works around Calcite resolving DESCRIPTOR(col) of TUMBLE/HOP over a subquery against the subquery's FROM item instead of
 * the subquery's own columns: e.g. TUMBLE((SELECT a, ts FROM t ...), DESCRIPTOR(ts)) points at ts's position in t. The
 * subquery becomes {@code SELECT * FROM (subquery) AS window_input}, whose FROM item has exactly its columns.
 */
public final class WindowInputs {
    private WindowInputs() {}

    private static final Set<String> WINDOWS = Set.of("TUMBLE", "HOP", "SESSION");

    public static SqlNode wrapSubqueries(SqlNode query) {
        return query.accept(new SqlShuttle() {
            @Override
            public SqlNode visit(SqlCall call) {
                SqlCall visited = (SqlCall) super.visit(call);
                if (visited == null || !WINDOWS.contains(visited.getOperator().getName().toUpperCase(Locale.ROOT))
                    || visited.operandCount() == 0) {
                    return visited;
                }
                SqlNode input = visited.operand(0);
                boolean subquery = input.isA(SqlKind.QUERY) && input.getKind() != SqlKind.EXPLICIT_TABLE;
                if (subquery) {
                    SqlParserPos pos = input.getParserPosition();
                    SqlNode aliased = SqlStdOperatorTable.AS.createCall(pos, input, new SqlIdentifier("window_input", pos));
                    SqlSelect wrapper = new SqlSelect(pos, null, SqlNodeList.SINGLETON_STAR, aliased,
                        null, null, null, null, null, null, null, null, null);
                    visited.setOperand(0, wrapper);
                }
                return visited;
            }
        });
    }
}
