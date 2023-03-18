import java.io.Serializable;


public class BooleanMapFunction implements MapFunction<Boolean, Boolean> {
    
    // This field is used to verify that we store the actual instance in the UDF descriptor.
    public boolean instanceVariable = false;

    public BooleanMapFunction(){
        this.instanceVariable = false;
    }

    public BooleanMapFunction(boolean instanceVariable) {
        this.instanceVariable = instanceVariable;
    }

    @Override
    public Boolean map(Boolean value) {
        return value && instanceVariable;
    }
   
}
