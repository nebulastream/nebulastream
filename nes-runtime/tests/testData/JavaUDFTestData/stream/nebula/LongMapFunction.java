package stream.nebula;
// IMPORTANT: If you make changes to this file, be sure to run buildJar.sh _and_ reload the cmake project to update the JAR file.
import java.io.Serializable;

/**
 * A MapFunction that adds a given long value to each input long value.
 */
public class LongMapFunction implements MapFunction<Long, Long>, Serializable {

    /**
     * This field is used to verify that we store the actual instance in the UDF descriptor.
     */
    public long instanceVariable = 10;

    /**
     * Creates a new LongMapFunction with a default instance variable of 10.
     */
    public LongMapFunction() {
        this.instanceVariable = 10;
    }

    /**
     * Creates a new LongMapFunction with a given instance variable.
     *
     * @param instanceVariable the value to add to each input long value
     */
    public LongMapFunction(long instanceVariable) {
        this.instanceVariable = instanceVariable;
    }

    /**
     * Adds the instance variable to the given value.
     *
     * @param value the input long value
     * @return the result of adding the instance variable to the input value
     */
    @Override
    public Long map(Long value) {
        return value + instanceVariable;
    }
}