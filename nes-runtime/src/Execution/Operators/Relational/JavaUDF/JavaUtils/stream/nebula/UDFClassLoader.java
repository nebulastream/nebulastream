package stream.nebula;

/**
 * Private class loader for a Java UDF.
 *
 * In order to isolate UDFs from each other, the dependent classes need to be loaded into their own class loader.
 * However, even though {@link ClassLoader} does not contain any abstract methods, it is an abstract class.
 * This class simply extends {@link ClassLoader}, so we can instantiate it via JNI.
 */
public class UDFClassLoader extends ClassLoader { }
