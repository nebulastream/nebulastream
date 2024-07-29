package stream.nebula;
// IMPORTANT: If you make changes to this file, be sure to run buildJar.sh _and_ reload the cmake project to update the JAR file.
import java.io.Serializable;

/**
 * A {@link MapFunction} implementation that appends fixed values to various instance variables of a {@link ComplexPojo}.
 */
public class ComplexPojoMapFunction implements MapFunction<ComplexPojo, ComplexPojo>, Serializable {

    /**
     * This field is used to verify that we store the actual instance in the UDF descriptor.
     */
    ComplexPojo pojo;

    /**
     * Constructs a new ComplexPojoMapFunction instance.
     */
    public ComplexPojoMapFunction(){
        this.pojo = new ComplexPojo();
        pojo.stringVariable = "Appended String:";
        pojo.floatVariable = 10;
        pojo.intVariable = 10;
        pojo.longVariable = 10;
        pojo.unsignedLongVariable = 10;
        pojo.shortVariable = 10;
        pojo.byteVariable = 10;
        pojo.doubleVariable = 10;
        pojo.booleanVariable = false;
    }

    /**
     * Constructs a new ComplexPojoMapFunction instance with the specified {@link ComplexPojo} value.
     *
     * @param value The {@link ComplexPojo} value to be used for initializing the instance variables.
     */
    public ComplexPojoMapFunction(ComplexPojo value) {
        pojo.stringVariable = value.stringVariable;
        pojo.floatVariable = value.floatVariable;
        pojo.intVariable = value.intVariable;
        pojo.booleanVariable = value.booleanVariable;
        pojo.longVariable = value.longVariable;
        pojo.unsignedLongVariable = value.unsignedLongVariable;
        pojo.shortVariable = value.shortVariable;
        pojo.byteVariable = value.byteVariable;
        pojo.doubleVariable = value.doubleVariable;
    }

    /**
     * Appends fixed values to various instance variables of the input {@link ComplexPojo} value.
     *
     * @param value The {@link ComplexPojo} value to which the fixed values are appended.
     * @return The updated {@link ComplexPojo} value.
     */
    @Override
    public ComplexPojo map(ComplexPojo value) {
        value.stringVariable += pojo.stringVariable;
        value.floatVariable += pojo.floatVariable;
        value.intVariable += pojo.intVariable;
        value.shortVariable += pojo.shortVariable;
        value.longVariable += pojo.longVariable;
        value.unsignedLongVariable += pojo.unsignedLongVariable;
        value.doubleVariable += pojo.doubleVariable;
        value.byteVariable += pojo.byteVariable;
        value.booleanVariable = pojo.booleanVariable && value.booleanVariable;
        return value;
    }
}