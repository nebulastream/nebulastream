package nes.calcite.physical;

/**
 * An operator that keeps the fields of its input row, maybe appending fields (Selection, Map, InferModel): a watermark
 * assigner may run before it (Pipelines), as it does not change the time field.
 */
public interface RowOperator extends NesOperator {
    @Override
    default int[] fields(int[] inputFields, NesOperator copy) {
        int appended = copy.getRowType().getFieldCount() - copy.getInput(0).getRowType().getFieldCount();
        int[] fields = java.util.Arrays.copyOf(inputFields, inputFields.length + appended);
        for (int i = 0; i < appended; i++) {
            fields[inputFields.length + i] = copy.getRowType().getFieldCount() - appended + i;
        }
        return fields;
    }
}
