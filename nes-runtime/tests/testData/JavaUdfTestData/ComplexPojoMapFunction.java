import java.io.Serializable;

/**
 * A {@link MapFunction} implementation that appends fixed values to various instance variables of a {@link ComplexPojo}.
 */
public class ComplexPojoMapFunction implements MapFunction<ComplexPojo, ComplexPojo>, Serializable {

    /**
     * This field is used to verify that we store the actual instance in the UDF descriptor.
     */
    public String stringVariable = "_appended";
    public float floatVariable = 10;
    public int intVariable = 10;
    public long longVariable = 10;
    public short shortVariable = 10;
    public byte byteVariable = 10;
    public double doubleVariable = 10;
    public boolean booleanVariable = false;

    /**
     * Constructs a new ComplexPojoMapFunction instance.
     */
    public ComplexPojoMapFunction(){
    }

    /**
     * Constructs a new ComplexPojoMapFunction instance with the specified {@link ComplexPojo} value.
     *
     * @param value The {@link ComplexPojo} value to be used for initializing the instance variables.
     */
    public ComplexPojoMapFunction(ComplexPojo value) {
        this.stringVariable = value.stringVariable;
        this.floatVariable = value.floatVariable;
        this.intVariable = value.intVariable;
        this.booleanVariable = value.booleanVariable;
        this.longVariable = value.longVariable;
        this.shortVariable = value.shortVariable;
        this.byteVariable = value.byteVariable;
        this.doubleVariable = value.doubleVariable;
    }

    /**
     * Appends fixed values to various instance variables of the input {@link ComplexPojo} value.
     *
     * @param value The {@link ComplexPojo} value to which the fixed values are appended.
     * @return The updated {@link ComplexPojo} value.
     */
    @Override
    public ComplexPojo map(ComplexPojo value) {
        value.stringVariable += stringVariable;
        value.floatVariable += floatVariable;
        value.intVariable += intVariable;
        value.shortVariable += shortVariable;
        value.longVariable += longVariable;
        value.doubleVariable += doubleVariable;
        value.byteVariable += byteVariable;
        value.booleanVariable = value.booleanVariable && booleanVariable;
        return value;
    }

}