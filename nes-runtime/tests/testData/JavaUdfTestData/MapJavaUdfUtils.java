import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.ClassNotFoundException;
import java.nio.ByteBuffer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class MapJavaUdfUtils {
    public Object deserialize(ByteBuffer byteBuffer) {
        try (ObjectInputStream os = new ObjectInputStream(new ByteArrayInputStream(byteBuffer.array()))) {
            return os.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public ByteBuffer serialize(Object object) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(bos);   
            out.writeObject(object);
            out.flush();
            return ByteBuffer.wrap(bos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                bos.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
