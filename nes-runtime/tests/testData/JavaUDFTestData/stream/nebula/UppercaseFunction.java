package stream.nebula;
// IMPORTANT: If you make changes to this file, be sure to run buildJar.sh _and_ reload the cmake project to update the JAR file.
import java.io.Serializable;

/**
 * This class implements the MapFunction interface to add a double value to a Double input value.
 */
public class UppercaseFunction implements MapFunction<String, String> {

    /**
     * Adds the instanceVariable to the given value and returns the result.
     *
     * @param value the value to add the instanceVariable to.
     * @return the result of adding the instanceVariable to the given value.
     */
    @Override
    public String map(String value) {
        return value.toUpperCase();
    }
}