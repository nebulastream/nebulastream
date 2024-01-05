package stream.nebula;
// IMPORTANT: If you make changes to this file, be sure to run buildJar.sh _and_ reload the cmake project to update the JAR file.
import java.io.Serializable;

/**
 * A {@link MapFunction} implementation that performs a logical AND operation between the input Boolean value and an instance variable.
 */
public class BooleanMapFunction implements MapFunction<Boolean, Boolean> {

    /**
     * This field is used to verify that we store the actual instance in the UDF descriptor.
     */
    public boolean instanceVariable = false;

    /**
     * Constructs a new BooleanMapFunction instance with a default instance variable value of false.
     */
    public BooleanMapFunction(){
        this.instanceVariable = false;
    }

    /**
     * Constructs a new BooleanMapFunction instance with a specified instance variable value.
     *
     * @param instanceVariable The value to be used as the instance variable.
     */
    public BooleanMapFunction(boolean instanceVariable) {
        this.instanceVariable = instanceVariable;
    }

    /**
     * Performs a logical AND operation between the input Boolean value and the instance variable.
     *
     * @param value The input Boolean value to which the instance variable is applied.
     * @return The result of performing a logical AND operation between the input Boolean value and the instance variable.
     */
    @Override
    public Boolean map(Boolean value) {
        return value && instanceVariable;
    }
}