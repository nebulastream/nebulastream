//package stream.nebula.udf;

import java.io.Serializable;


public class IntegerMapFunction implements MapFunction<Integer, Integer> {
    
    // This field is used to verify that we store the actual instance in the UDF descriptor.
    public int instanceVariable = 0;

    public IntegerMapFunction() {
        this.instanceVariable = 10;
    }

    public IntegerMapFunction(int instanceVariable) {
        this.instanceVariable = instanceVariable;
    }

    @Override
    public Integer map(Integer value) {
        return value + instanceVariable;
    }
   
}
