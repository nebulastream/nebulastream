//package stream.nebula.udf;

import java.io.Serializable;


public class ShortMapFunction implements MapFunction<Short, Short> {

    // This field is used to verify that we store the actual instance in the UDF descriptor.
    public Short instanceVariable = 10;

    public ShortMapFunction(){
        this.instanceVariable = 10;
    }

    public ShortMapFunction(short instanceVariable) {
        this.instanceVariable = instanceVariable;
    }

    public Integer map(Integer value) {
        return value + instanceVariable;
    }

    @Override
    public Short map(Short value) {
        Short val = (short)(instanceVariable + value);
        return val;
    }

}