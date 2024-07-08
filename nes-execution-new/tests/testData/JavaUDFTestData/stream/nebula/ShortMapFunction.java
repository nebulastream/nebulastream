package stream.nebula;
// IMPORTANT: If you make changes to this file, be sure to run buildJar.sh _and_ reload the cmake project to update the JAR file.
import java.io.Serializable;

/**
 * A map function that adds a fixed short value to the input Short value.
 */
public class ShortMapFunction implements MapFunction<Short, Short>, Serializable {

    // This field is used to verify that we store the actual instance in the UDF descriptor.
    public short instanceVariable = 10;

    /**
     * Constructs a new ShortMapFunction with a default instance variable value of 10.
     */
    public ShortMapFunction() {
        this.instanceVariable = 10;
    }

    /**
     * Constructs a new ShortMapFunction with the specified instance variable value.
     *
     * @param instanceVariable the value to set the instance variable to
     */
    public ShortMapFunction(short instanceVariable) {
        this.instanceVariable = instanceVariable;
    }

    /**
     * Adds the instance variable value to the input Short value and returns the result.
     *
     * @param value the input Short value to map
     * @return the result of adding the instance variable value to the input value
     */
    @Override
    public Short map(Short value) {
        Short val = (short)(instanceVariable + value);
        return val;
    }
}