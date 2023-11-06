package stream.nebula;

import java.io.*;
import java.lang.ClassNotFoundException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

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

}
