//package stream.nebula.udf;

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

    public DummyRichMapFunction(){
        this.instanceVariable = 10;
    }

    public DummyRichMapFunction(int instanceVariable) {
        this.instanceVariable = instanceVariable;
    }

    @Override
    public Integer map(Integer value) {
        return value + instanceVariable;
    }

    //

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
