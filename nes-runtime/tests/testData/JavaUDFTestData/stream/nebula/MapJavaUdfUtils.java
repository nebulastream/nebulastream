package stream.nebula;
// IMPORTANT: If you make changes to this file, be sure to run buildJar.sh _and_ reload the cmake project to update the JAR file.

import java.io.ObjectInputStream;
import java.lang.ClassNotFoundException;
import java.io.ByteArrayInputStream;
import java.io.IOException;

public class MapJavaUdfUtils {
    /**
    * Deserialize a byte array into an Object.
    * @param byteArray The byte array to deserialize.
    * @return The deserialized Object.
    */
    public Object deserialize(final byte[] byteArray) {
        try (ObjectInputStream os = new ObjectInputStream(new ByteArrayInputStream(byteArray))) {
            return os.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
