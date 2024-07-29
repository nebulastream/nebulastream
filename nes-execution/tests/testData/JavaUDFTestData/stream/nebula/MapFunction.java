package stream.nebula;
// IMPORTANT: If you make changes to this file, be sure to run buildJar.sh _and_ reload the cmake project to update the JAR file.
import java.io.Serializable;

/**
 * Base interface for UDFs that implement a tuple-at-a-time map function.
 *
 * @param <IN> Input data type of the UDF.
 * @param <OUT> Output data type of the UDF.
 */
public interface MapFunction<IN, OUT> extends Serializable {

    /**
     * Apply the map UDF to an input value.
     *
     * @param value The input value. Can also be a tuple of values.
     * @return The transformed output.
     */
    OUT map(IN value);
}
