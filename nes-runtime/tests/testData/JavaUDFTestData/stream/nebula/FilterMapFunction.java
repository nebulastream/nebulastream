package stream.nebula;
// IMPORTANT: If you make changes to this file, be sure to run buildJar.sh _and_ reload the cmake project to update the JAR file.

import java.io.Serializable;

/**
 * A {@link MapFunction} implementation that adds an instance variable to the input Integer value.
 */
public class FilterMapFunction implements MapFunction<Integer, Boolean> {

      /**
     * Adds the instance variable to the input Integer value.
     *
     * @param value The input Integer value to which the instance variable is added.
     * @return The result of adding the instance variable to the input Integer value.
     */
    @Override
    public Boolean map(Integer value) {
        return value == 42;
    }
}