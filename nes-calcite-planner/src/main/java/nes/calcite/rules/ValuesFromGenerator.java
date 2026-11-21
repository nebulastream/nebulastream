package nes.calcite.rules;

import nes.calcite.logical.AnonymousSources;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import nes.calcite.proto.AnonymousSource;
import nes.calcite.proto.Field;
import nes.calcite.proto.FieldType;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * Constant rows (VALUES) from a generator source, as we have no operator for them. Calcite makes them mostly of queries it
 * proves empty (e.g. WHERE a = NULL), and of single dummy rows. For n rows, an anonymous generator source emits the row
 * indexes 0 .. n-1 and ends; a projection picks each row's constants by its index. For no rows, the generator emits one
 * row, which a filter on FALSE drops. The generator runs on `host`.
 */
public final class ValuesFromGenerator {
    private ValuesFromGenerator() {}

    public static RelNode apply(RelNode plan, String host) {
        return plan.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                RelNode node = super.visit(other);
                return node instanceof Values values ? generate(values, host) : node;
            }
        });
    }

    private static RelNode generate(Values values, String host) {
        if (host == null || host.isEmpty()) {
            throw new UnsupportedOperationException("constant rows (VALUES) need a worker that generates them: the query reads no source"
                + " and has no sink worker");
        }
        RexBuilder rexBuilder = values.getCluster().getRexBuilder();
        int rows = values.getTuples().size();
        RelNode generator = generator(values, Math.max(rows, 1), host);
        RexNode index = RexInputRef.of(0, generator.getRowType());
        if (rows == 0) {
            generator = LogicalFilter.create(generator, rexBuilder.makeLiteral(false));
        }

        List<RexNode> columns = new ArrayList<>();
        List<RelDataTypeField> fields = values.getRowType().getFieldList();
        for (int column = 0; column < fields.size(); column++) {
            RelDataType type = fields.get(column).getType();
            if (rows == 0) {
                /// Of the column's type, nullability included (a zero literal is never NULL).
                columns.add(rexBuilder.makeAbstractCast(type, rexBuilder.makeZeroLiteral(type), false));
                continue;
            }
            /// CASE WHEN index = 0 THEN row 0's value WHEN index = 1 THEN row 1's ... ELSE the last row's.
            List<RexNode> cases = new ArrayList<>();
            for (int row = 0; row < rows - 1; row++) {
                cases.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, index, rexBuilder.makeBigintLiteral(BigDecimal.valueOf(row))));
                cases.add(rexBuilder.makeAbstractCast(type, values.getTuples().get(row).get(column), false));
            }
            RexLiteral last = values.getTuples().get(rows - 1).get(column);
            cases.add(rexBuilder.makeAbstractCast(type, last, false));
            columns.add(cases.size() == 1 ? cases.get(0) : rexBuilder.makeCall(type, SqlStdOperatorTable.CASE, cases));
        }
        return LogicalProject.create(generator, List.of(), columns, values.getRowType().getFieldNames(), java.util.Set.of());
    }

    /** An anonymous generator source of one column, the row index 0 .. rows - 1. */
    private static RelNode generator(Values values, int rows, String host) {
        AnonymousSource spec = AnonymousSource.newBuilder()
            .setType("Generator")
            .addSchema(Field.newBuilder().setName("ROW_INDEX").setType(FieldType.INT64).setNullable(false))
            /// As the anonymous source function writes its options: strings quoted, HOST and the format TYPE as they are.
            .putSourceOptions("HOST", host)
            .putSourceOptions("GENERATOR_SCHEMA", "'SEQUENCE INT64 0 " + rows + " 1'")
            .putSourceOptions("STOP_GENERATOR_WHEN_SEQUENCE_FINISHES", "'ALL'")
            .putSourceOptions("SEED", "1")
            .putFormatOptions("TYPE", "CSV")
            .build();
        RelDataType rowType = AnonymousSources.rowType(values.getCluster().getTypeFactory(), spec.getSchemaList());
        return new AnonymousSources.NesAnonymousSource(values.getCluster(), values.getTraitSet(), spec, rowType);
    }
}
