package stream.nebula;
// IMPORTANT: If you make changes to this file, be sure to run buildJar.sh _and_ reload the cmake project to update the JAR file.
import java.io.Serializable;
import java.util.Arrays;
import java.lang.String;
import java.util.Collection;

/**
 * A map function that appends a given string to the input string and returns the resulting string.
 */
public class StringFlatMapFunction implements FlatMapFunction<String, String>, Serializable {

    /**
     * Appends the input sting to the instanceVariable and returns the resulting string.
     *
     * @param value The input string.
     * @return The instance variable with the input string appended to it.
     */
    @Override
    public Collection<String> flatMap(String value) {
        return Arrays.asList( value.split(" "));
    }

}