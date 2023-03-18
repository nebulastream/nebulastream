import java.io.Serializable;

public class ByteMapFunction implements MapFunction<Byte, Byte> {

    // This field is used to verify that we store the actual instance in the UDF descriptor.
    public byte instanceVariable = 0;

    public ByteMapFunction(){
        this.instanceVariable = 10;
    }

    public ByteMapFunction(byte instanceVariable) {
        this.instanceVariable = instanceVariable;
    }

    @Override
    public Byte map(Byte value) {
        Byte val = (byte)(instanceVariable + value);
        return val;
    }

}
