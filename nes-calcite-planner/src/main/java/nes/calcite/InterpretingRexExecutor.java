package nes.calcite;

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.metadata.NullSentinel;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexInterpreter;
import org.apache.calcite.rex.RexNode;

/**
 * Folds constant expressions by interpreting them. Calcite's default (RexExecutorImpl) generates Java code and compiles
 * it with Janino at runtime, which a native image cannot load; there, folding silently did nothing. Expressions the
 * interpreter cannot evaluate stay as they are.
 */
final class InterpretingRexExecutor implements RexExecutor {
    static final InterpretingRexExecutor INSTANCE = new InterpretingRexExecutor();

    private InterpretingRexExecutor() {}

    @Override
    public void reduce(RexBuilder rexBuilder, List<RexNode> constExps, List<RexNode> reducedValues) {
        for (RexNode expression : constExps) {
            reducedValues.add(evaluate(rexBuilder, expression));
        }
    }

    private static RexNode evaluate(RexBuilder rexBuilder, RexNode expression) {
        try {
            Comparable<?> value = RexInterpreter.evaluate(expression, Map.of());
            return value == NullSentinel.INSTANCE
                ? rexBuilder.makeNullLiteral(expression.getType())
                : rexBuilder.makeLiteral(value, expression.getType(), true);
        } catch (RuntimeException e) {
            return expression;
        }
    }
}
