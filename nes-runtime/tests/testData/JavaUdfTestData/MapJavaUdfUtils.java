import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.ClassNotFoundException;
import java.nio.ByteBuffer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class MapJavaUdfUtils {
   /**
    * Deserialize a ByteBuffer into an Object.
    * @param byteBuffer The ByteBuffer to deserialize.
    * @return The deserialized Object.
    */
    public Object deserialize(ByteBuffer byteBuffer) {
        try (ObjectInputStream os = new ObjectInputStream(new ByteArrayInputStream(byteBuffer.array()))) {
            return os.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
