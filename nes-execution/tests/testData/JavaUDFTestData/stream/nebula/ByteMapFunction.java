package stream.nebula;
// IMPORTANT: If you make changes to this file, be sure to run buildJar.sh _and_ reload the cmake project to update the JAR file.
import java.io.Serializable;

/**
 * A {@link MapFunction} implementation that adds an instance variable to the input Byte value.
 */
public class ByteMapFunction implements MapFunction<Byte, Byte>, Serializable {

    /**
     * This field is used to verify that we store the actual instance in the UDF descriptor.
     */
    public byte instanceVariable = 0;

    /**
     * Constructs a new ByteMapFunction instance with a default instance variable value of 10.
     */
    public ByteMapFunction(){
        this.instanceVariable = 10;
    }

    /**
     * Constructs a new ByteMapFunction instance with a specified instance variable value.
     *
     * @param instanceVariable The value to be used as the instance variable.
     */
    public ByteMapFunction(byte instanceVariable) {
        this.instanceVariable = instanceVariable;
    }

    /**
     * Adds the instance variable to the input Byte value.
     *
     * @param value The input Byte value to which the instance variable is added.
     * @return The result of adding the instance variable to the input Byte value.
     */
    @Override
    public Byte map(Byte value) {
        Byte val = (byte)(instanceVariable + value);
        return val;
    }
}