package nes.calcite.rules;

import org.apache.calcite.plan.RelRule;
import nes.calcite.logical.NesModelInference;
import nes.calcite.Planner;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import nes.calcite.proto.Field;
import nes.calcite.proto.Model;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;

/**
 * Model inference: {@code TABLE(MODEL_INFERENCE(TABLE input, 'model'))} runs a registered model on each row of the input
 * (a table or a subquery). Its rows are the input's columns followed by the model's outputs. The model reads its inputs
 * from the columns with the names of its input fields.
 */
public final class ModelInference {
    private ModelInference() {}

    /** The table function, for the models of one request. */
    public static final class Function extends SqlFunction implements SqlTableFunction {
        private final Map<String, Model> models;

        public Function(List<Model> models) {
            super("MODEL_INFERENCE", SqlKind.OTHER_FUNCTION, ReturnTypes.CURSOR, null,
                OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER), SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
            this.models = models.stream().collect(Collectors.toMap(m -> m.getName().toUpperCase(Locale.ROOT), m -> m, (a, b) -> a));
        }

        Model model(String name) {
            Model model = models.get(name.toUpperCase(Locale.ROOT));
            if (model == null) {
                throw new IllegalArgumentException("unknown model " + name + "; registered: " + models.keySet());
            }
            return model;
        }

        @Override
        public SqlReturnTypeInference getRowTypeInference() {
            return this::rowType;
        }

        private RelDataType rowType(SqlOperatorBinding binding) {
            RelDataTypeFactory types = binding.getTypeFactory();
            Model model = model(binding.getOperandLiteralValue(1, String.class));
            RelDataTypeFactory.Builder row = types.builder().addAll(binding.getOperandType(0).getFieldList());
            for (Field output : model.getOutputsList()) {
                row.add(output.getName(), types.createTypeWithNullability(
                    Planner.relType(types, output.getType(), model.getName() + "." + output.getName()), output.getNullable()));
            }
            return row.build();
        }

        /** The first argument is a table (TABLE t or a subquery), the second the model's name. */
        @Override
        public boolean argumentMustBeScalar(int ordinal) {
            return ordinal != 0;
        }
    }

    /** Turns the table function into the inference operator. */
    public static final RelOptRule RULE = RuleConfig.of("NesModelInferenceRule",
        b -> b.operand(TableFunctionScan.class).anyInputs(), Rule::new).toRule();

    private static final class Rule extends RelRule<RuleConfig> {
        Rule(RuleConfig config) {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            TableFunctionScan scan = call.rel(0);
            if (!(scan.getCall() instanceof RexCall invocation) || !(invocation.getOperator() instanceof Function)
                || scan.getInputs().size() != 1) {
                return;
            }
            /// The model as registered (the query may spell its name in another case).
            Model model = ((Function) invocation.getOperator()).model(((RexLiteral) invocation.getOperands().get(1)).getValueAs(String.class));
            NesModelInference inference = new NesModelInference(scan.getCluster(), scan.getTraitSet(), scan.getInputs().get(0), model,
                scan.getRowType());
            inference.inputColumns(); /// fails here if the input lacks a column that the model reads
            call.transformTo(inference);
        }
    }
}
