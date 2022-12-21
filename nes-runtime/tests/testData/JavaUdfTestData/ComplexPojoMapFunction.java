//package stream.nebula.udf;

import java.io.Serializable;

public class ComplexPojoMapFunction implements MapFunction<ComplexPojo, ComplexPojo> {
    
    // This field is used to verify that we store the actual instance in the UDF descriptor.
    //public String stringVariable = "_appended";
    public float floatVariable = 10;
    public int intVariable = 10;
    public boolean booleanVariable = false;

    public ComplexPojoMapFunction(){
    }

    public ComplexPojoMapFunction(ComplexPojo value) {
        //this.stringVariable = value.stringVariable;
        this.floatVariable = value.floatVariable;
        this.intVariable = value.intVariable;
        this.booleanVariable = value.booleanVariable;
    }

    @Override
    public ComplexPojo map(ComplexPojo value) {
        //value.stringVariable += stringVariable;
        value.floatVariable += floatVariable;
        value.intVariable += intVariable;
        value.booleanVariable = value.booleanVariable && booleanVariable;
        return value;
    }
   
}
