package metadata;

import Kafka.PrimitiveTypes;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class Record {
    private final int length;
    private final byte attributes;
    private final long timestampDelta;
    private final int offsetDelta;
    private final byte[] key;
    private final int valueLength;
    private final byte[] value;
    private final List<RecordHeader> headers;

    // Constructor
    public Record(int length, byte attributes, long timestampDelta, int offsetDelta, byte[] key, int valueLength,
                  byte[] value, List<RecordHeader> headers) {
        this.length = length;
        this.attributes = attributes;
        this.timestampDelta = timestampDelta;
        this.offsetDelta = offsetDelta;
        this.key = key;
        this.valueLength = valueLength;
        this.value = value;
        this.headers = headers;
    }

    // Getters
    public int getLength() {
        return length;
    }

    public byte getAttributes() {
        return attributes;
    }

    public long getTimestampDelta() {
        return timestampDelta;
    }

    public int getOffsetDelta() {
        return offsetDelta;
    }

    public byte[] getKey() {
        return key;
    }

    public int getValueLength() {
        return valueLength;
    }

    public byte[] getValue() {
        return value;
    }

    public List<RecordHeader> getHeaders() {
        return headers;
    }

    // Decode method
    public static Record decode(DataInputStream inputStream) throws IOException {
        int length = PrimitiveTypes.decodeVarint(inputStream);
        byte attributes = PrimitiveTypes.decodeInt8(inputStream);
        long timestampDelta = PrimitiveTypes.decodeVarlong(inputStream);
        int offsetDelta = PrimitiveTypes.decodeVarint(inputStream);
        byte[] key = PrimitiveTypes.decodeCompactBytes(inputStream);
        int valueLength = PrimitiveTypes.decodeVarint(inputStream);

        // Adjust value length (divide by 2)
        int x = valueLength / 2;
        byte[] value = new byte[x];
        inputStream.readFully(value);

        List<RecordHeader> headers = PrimitiveTypes.decodeCompactArray(inputStream, RecordHeader::decode);

        return new Record(length, attributes, timestampDelta, offsetDelta, key, valueLength, value, headers);
    }

    // Encode method
    public void encode(DataOutputStream outputStream) throws IOException {
        PrimitiveTypes.encodeVarint(outputStream, length);
        PrimitiveTypes.encodeInt8(outputStream, attributes);
        PrimitiveTypes.encodeVarlong(outputStream, timestampDelta);
        PrimitiveTypes.encodeVarint(outputStream, offsetDelta);
        PrimitiveTypes.encodeCompactBytes(outputStream, key);
        PrimitiveTypes.encodeVarint(outputStream, valueLength);
        outputStream.write(value); // Write value directly
        PrimitiveTypes.encodeCompactArray(outputStream, headers, (stream, header) -> header.encode(stream));
    }

}
