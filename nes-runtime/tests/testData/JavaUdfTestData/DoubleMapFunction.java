//package stream.nebula.udf;

import java.io.Serializable;


public class DoubleMapFunction implements MapFunction<Double, Double> {

    // This field is used to verify that we store the actual instance in the UDF descriptor.
    public double instanceVariable = 10;

    public DoubleMapFunction(){
        this.instanceVariable = 10.0;
    }

    public DoubleMapFunction(double instanceVariable) {
        this.instanceVariable = instanceVariable;
    }

    @Override
    public Double map(Double value) {
        Double result = (double)(value + instanceVariable);
        return result;
    }

}