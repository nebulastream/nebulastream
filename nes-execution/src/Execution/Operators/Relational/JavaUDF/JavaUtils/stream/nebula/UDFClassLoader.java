package stream.nebula;

import java.io.*;
import java.lang.ClassNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Private class loader for a Java UDF.
 * <p>
 * In order to isolate UDFs from each other, the dependent classes need to be loaded into their own class loader.
 * However, even though {@link ClassLoader} does not contain any abstract methods, it is an abstract class.
 * This class simply extends {@link ClassLoader}, so we can instantiate it via JNI.
 * <p>
 * The class instance must also be deserialized from the class loader.
 * Otherwise it would be resolved using the system class loader in which it is unknown.
 * <p>
 * See also: https://docs.oracle.com/javase/8/docs/api/java/io/ObjectInputStream.html#resolveClass-java.io.ObjectStreamClass-
 */
public class UDFClassLoader extends ClassLoader {

    Map<String, byte[]> classes = new HashMap<>();

    static class ClassLoaderObjectInputStream extends ObjectInputStream {

        private final ClassLoader classLoader;

        public ClassLoaderObjectInputStream(InputStream in, ClassLoader classLoader) throws IOException {
            super(in);
            this.classLoader = classLoader;
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
            try {
                return Class.forName(desc.getName(), true, classLoader);
            } catch (ClassNotFoundException e) {
                return super.resolveClass(desc);
            }
        }
    }

    /**
     * Deserialize a byte array into an Object.
     * @param byteArray The byte array to deserialize.
     * @return The deserialized Object.
     */
    public Object deserialize(final byte[] byteArray) {
        try (ObjectInputStream os = new ClassLoaderObjectInputStream(new ByteArrayInputStream(byteArray), this)) {
            return os.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Inject a class into the UDF class loader.
     * @param className The name of the class.
     * @param byteCode The byte code of the class.
     */
    void injectClass(String className, byte[] byteCode) {
        // Store a copy of the byteCode because the parameter will be released by the calling code.
        byte[] copy = Arrays.copyOf(byteCode, byteCode.length);
        classes.put(className, copy);
    }

    @Override
    protected Class<?> findClass(final String name) throws ClassNotFoundException {
        byte[] byteCode = classes.get(name);
        if (byteCode == null) {
            throw new ClassNotFoundException(name);
        }
        return defineClass(name, byteCode, 0, byteCode.length);
    }
}
