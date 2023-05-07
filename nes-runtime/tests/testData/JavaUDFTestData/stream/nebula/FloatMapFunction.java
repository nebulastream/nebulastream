package stream.nebula;
// IMPORTANT: If you make changes to this file, be sure to run buildJar.sh _and_ reload the cmake project to update the JAR file.
import java.io.Serializable;

/**
 * This class represents a MapFunction that adds a fixed float value to the input value.
 *
 * <p>This field is used to verify that we store the actual instance in the UDF descriptor.</p>
 */
public class FloatMapFunction implements MapFunction<Float, Float> {
    
    /**
     * The fixed float value added to the input value.
     */
    public float instanceVariable = 0;

    /**
     * Constructs a new FloatMapFunction with a default instance variable of 10.
     */
    public FloatMapFunction(){
        this.instanceVariable = 10;
    }

    /**
     * Constructs a new FloatMapFunction with the given instance variable.
     *
     * @param instanceVariable the fixed float value added to the input value.
     */
    public FloatMapFunction(float instanceVariable) {
        this.instanceVariable = instanceVariable;
    }

    /**
     * Applies this MapFunction to the given input value, adding the instance variable to the input value.
     *
     * @param value the input value
     * @return the result of adding the instance variable to the input value
     */
    @Override
    public Float map(Float value) {
        return value + instanceVariable;
    }
}