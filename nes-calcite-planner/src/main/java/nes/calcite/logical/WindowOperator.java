package nes.calcite.logical;

/**
 * An operator over windows of the event time of its inputs (NesLogical.WindowAggregation, NesWindowJoin). Its rows carry
 * the ends of its windows as watermarks; Watermarks traces the time field of each input to its sources.
 */
public interface WindowOperator extends CarriesWatermarks {
    /** The time field of the input. */
    int timeField(int input);

    @Override
    default boolean watermarked() {
        return true;
    }
}
