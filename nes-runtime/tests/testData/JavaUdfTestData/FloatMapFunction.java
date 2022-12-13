//package stream.nebula.udf;

import java.io.Serializable;

public class FloatMapFunction implements MapFunction<Float, Float> {
    
    // This field is used to verify that we store the actual instance in the UDF descriptor.
    public float instanceVariable = 0;

    public FloatMapFunction(){
        this.instanceVariable = 10;
    }

    public FloatMapFunction(float instanceVariable) {
        this.instanceVariable = instanceVariable;
    }

    @Override
    public Float map(Float value) {
        return value + instanceVariable;
    }
   
}
