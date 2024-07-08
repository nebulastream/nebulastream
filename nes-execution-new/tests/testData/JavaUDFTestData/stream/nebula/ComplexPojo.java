package stream.nebula;
// IMPORTANT: If you make changes to this file, be sure to run buildJar.sh to update the JAR file.
import java.io.Serializable;

/**
 * A Serializable class representing a complex POJO with various primitive-typed instance variables.
 */
public class ComplexPojo implements Serializable {
    String stringVariable;
    int intVariable;
    byte byteVariable;
    short shortVariable;
    long longVariable;
    long unsignedLongVariable;
    float floatVariable;
    double doubleVariable;
    boolean booleanVariable;
}