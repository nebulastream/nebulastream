package stream.nebula;
// IMPORTANT: If you make changes to this file, be sure to run buildJar.sh _and_ reload the cmake project to update the JAR file.

import java.io.Serializable;

/**
 * A {@link MapFunction} implementation that adds an instance variable to the input Integer value.
 */
public class IntegerMapFunction implements MapFunction<Integer, Integer> {

    /**
     * This field is used to verify that we store the actual instance in the UDF descriptor.
     */
    public int instanceVariable = 0;

    /**
     * Constructs a new IntegerMapFunction instance with a default instance variable value of 10.
     */
    public IntegerMapFunction() {
        this.instanceVariable = 10;
    }

    /**
     * Constructs a new IntegerMapFunction instance with a specified instance variable value.
     *
     * @param instanceVariable The value to be used as the instance variable.
     */
    public IntegerMapFunction(int instanceVariable) {
        this.instanceVariable = instanceVariable;
    }

    /**
     * Adds the instance variable to the input Integer value.
     *
     * @param value The input Integer value to which the instance variable is added.
     * @return The result of adding the instance variable to the input Integer value.
     */
    @Override
    public Integer map(Integer value) {
        return (int)value + (int)10;
    }
}