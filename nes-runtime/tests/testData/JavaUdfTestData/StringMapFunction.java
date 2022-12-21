//package stream.nebula.udf;

import java.io.Serializable;

public class StringMapFunction implements MapFunction<String, String> {
    
    // This field is used to verify that we store the actual instance in the UDF descriptor.
    public String instanceVariable;

    public StringMapFunction(){
        this.instanceVariable = "_appended";
    }

    public StringMapFunction(String instanceVariable) {
        this.instanceVariable = instanceVariable;
    }

    @Override
    public String map(String value) {
        return value += instanceVariable;
    }
   
}
