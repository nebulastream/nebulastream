package stream.nebula;
// IMPORTANT: If you make changes to this file, be sure to run buildJar.sh _and_ reload the cmake project to update the JAR file.
import java.io.Serializable;

/**
 * A dummy map function used by UdfDescriptorBuilderTest.
 * <p>
 * This is a top-level class to prevent the inclusion of UdfDescriptorBuilderTest in the class dependencies.
 * It contains the nested classes {@link DependentClass} and {@link RecursiveDependentClass} to test the computation of the transitive closure of class dependencies.
 */
public class DummyRichMapFunction implements MapFunction<Integer, Integer> {

    // This field makes DependentClass a direct dependency of DummyRichMapFunction and RecursiveDependentClass an indirect dependency.
    public DependentClass dependentClass = new DependentClass();
    // This field is used to verify that we store the actual instance in the UDF descriptor.
    public int instanceVariable = 0;

    /**
     * Creates a new instance of DummyRichMapFunction with a default value of 10 for the instance variable.
     */
    public DummyRichMapFunction() {
        this.instanceVariable = 10;
    }

    /**
     * Creates a new instance of DummyRichMapFunction with the specified value for the instance variable.
     *
     * @param instanceVariable the value to set for the instance variable
     */
    public DummyRichMapFunction(int instanceVariable) {
        this.instanceVariable = instanceVariable;
    }

    /**
     * Maps the input value to a new Integer value by adding the instance variable to it.
     *
     * @param value the input value to be mapped
     * @return the result of adding the instance variable to the input value
     */
    @Override
    public Integer map(Integer value) {
        return value + instanceVariable;
    }


    /**
     * DependentClass is a direct dependency of {@link DummyRichMapFunction} because it is the type of the declared field `dependentClass'.
     */
    public static class DependentClass implements Serializable {
        // This field makes RecursiveDependentClass a direct dependency of DependentClass and therefore an indirect dependency of MapFunction.
        public RecursiveDependentClass recursiveDependentClass = new RecursiveDependentClass();
    }

    //

    /**
     * RecursiveDependentClass is an indirect dependency of {@link DummyRichMapFunction} because it is a dependency of {@link DependentClass}.
     */
    public static class RecursiveDependentClass implements Serializable {
    }
}
