package stream.nebula;

import java.io.Serializable;
import java.util.Collection;

/**
 * This interface represents a user-defined function (UDF) for implementing a flat map operation.
 * It provides a mechanism for transforming an input value (possibly a tuple) into a collection of output values.
 * In contrast to a standard map function, a flat map can transform single inputs to multiple outputs.
 *
 * @param <IN>  the type of the input elements
 * @param <OUT> the type of the output elements
 */
public interface FlatMapFunction<IN, OUT> extends Serializable {

    /**
     * Apply the flat map function to an input value.
     * This function transforms the input into a collection of output values.
     *
     * @param value the input value to the function, which could be a tuple of values.
     * @return a collection containing the output values produced by the flat map operation.
     */
    Collection<OUT> flatMap(IN value);

}
