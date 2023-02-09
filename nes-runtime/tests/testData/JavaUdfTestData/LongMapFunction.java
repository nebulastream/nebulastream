import java.io.Serializable;


public class LongMapFunction implements MapFunction<Long, Long> {

    // This field is used to verify that we store the actual instance in the UDF descriptor.
    public long instanceVariable = 10;

    public LongMapFunction(){
        this.instanceVariable = 10;
    }

    public LongMapFunction(long instanceVariable) {
        this.instanceVariable = instanceVariable;
    }

    @Override
    public Long map(Long value) {
        return value + instanceVariable;
    }

}