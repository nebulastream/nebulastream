package stream.nebula;
// IMPORTANT: If you make changes to this file, be sure to run buildJar.sh _and_ reload the cmake project to update the JAR file.
import java.io.Serializable;

/**
 * This class implements the MapFunction interface to add a double value to a Double input value.
 */
public class DoubleMapFunction implements MapFunction<Double, Double> {

    // This field is used to verify that we store the actual instance in the UDF descriptor.
    public double instanceVariable = 10;

    /**
     * Default constructor sets instanceVariable to 10.0.
     */
    public DoubleMapFunction(){
        this.instanceVariable = 10.0;
    }

    /**
     * Constructor to set the value of instanceVariable.
     *
     * @param instanceVariable the value to set the instanceVariable to.
     */
    public DoubleMapFunction(double instanceVariable) {
        this.instanceVariable = instanceVariable;
    }

    /**
     * Adds the instanceVariable to the given value and returns the result.
     *
     * @param value the value to add the instanceVariable to.
     * @return the result of adding the instanceVariable to the given value.
     */
    @Override
    public Double map(Double value) {
        Double result = (double)(value + instanceVariable);
        return result;
    }
}