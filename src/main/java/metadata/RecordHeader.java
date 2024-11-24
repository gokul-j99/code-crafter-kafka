package metadata;

import Kafka.PrimitiveTypes;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class RecordHeader {
    private final String key;
    private final byte[] value;

    // Constructor
    public RecordHeader(String key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    // Getters
    public String getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    // Decode method
    public static RecordHeader decode(DataInputStream inputStream) throws IOException {
        int keyLength = PrimitiveTypes.decodeVarint(inputStream);
        byte[] keyBytes = new byte[keyLength];
        inputStream.readFully(keyBytes);
        String key = new String(keyBytes);

        int valueLength = PrimitiveTypes.decodeVarint(inputStream);
        byte[] value = new byte[valueLength];
        inputStream.readFully(value);

        return new RecordHeader(key, value);
    }

    // Encode method
    public void encode(DataOutputStream outputStream) throws IOException {
        PrimitiveTypes.encodeVarint(outputStream, key.length());
        outputStream.write(key.getBytes());
        PrimitiveTypes.encodeVarint(outputStream, value.length);
        outputStream.write(value);
    }
}
