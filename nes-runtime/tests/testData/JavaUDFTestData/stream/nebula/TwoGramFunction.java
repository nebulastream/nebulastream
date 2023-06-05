package stream.nebula;
// IMPORTANT: If you make changes to this file, be sure to run buildJar.sh _and_ reload the cmake project to update the JAR file.
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
/**
 * This class implements the MapFunction interface to add a double value to a Double input value.
 */
public class TwoGramFunction implements FlatMapFunction<String, String> {

    /**
     * Adds the instanceVariable to the given value and returns the result.
     *
     * @param value the value to add the instanceVariable to.
     * @return the result of adding the instanceVariable to the given value.
     */
    @Override
    public Collection<String> flatMap(String text) {
        var splits = text.split("\\W+");
        int length = splits.length;
        var result = new ArrayList<String>();
        int index = 1;
        while (index < length) {
            var twoWord = splits[index -1] + splits[index];
            result.add(twoWord);
            index++;
        }
        return result;
    }
}