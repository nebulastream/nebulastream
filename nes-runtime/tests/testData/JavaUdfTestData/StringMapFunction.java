import java.io.Serializable;

/**
 * A map function that appends a given string to the input string and returns the resulting string.
 */
public class StringMapFunction implements MapFunction<String, String>, Serializable {

    // This field is used to verify that we store the actual instance in the UDF descriptor.
    public String instanceVariable;

    /**
     * Constructs a new instance of StringMapFunction with the default appended string.
     */
    public StringMapFunction(){
        this.instanceVariable = "_appended";
    }

    /**
     * Constructs a new instance of StringMapFunction with the specified appended string.
     *
     * @param instanceVariable The string to append to the input string.
     */
    public StringMapFunction(String instanceVariable) {
        this.instanceVariable = instanceVariable;
    }

    /**
     * Appends the instance variable to the input string and returns the resulting string.
     *
     * @param value The input string.
     * @return The input string with the instance variable appended to it.
     */
    @Override
    public String map(String value) {
        String result = value + instanceVariable;
        System.out.println("Result: " + result);
        return result;
    }

}