package messages;

import Kafka.PrimitiveTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class DescribeTopicPartitionsCursor {
    private final String topicName;
    private final int partitionIndex;

    // Constructor
    public DescribeTopicPartitionsCursor(String topicName, int partitionIndex) {
        this.topicName = topicName;
        this.partitionIndex = partitionIndex;
    }

    // Getters
    public String getTopicName() {
        return topicName;
    }

    public int getPartitionIndex() {
        return partitionIndex;
    }

    // Decode method
    public static DescribeTopicPartitionsCursor decode(DataInputStream inputStream) throws IOException {
        if (inputStream.readByte() == (byte) 0xFF) {
            return null; // Indicates no cursor
        }

        inputStream.skipBytes(-1); // Move back one byte

        String topicName = PrimitiveTypes.decodeCompactString(inputStream);
        int partitionIndex = PrimitiveTypes.decodeInt32(inputStream);
        PrimitiveTypes.decodeTaggedFields(inputStream);

        return new DescribeTopicPartitionsCursor(topicName, partitionIndex);
    }

    // Encode method
    public void encode(DataOutputStream outputStream) throws IOException {
        PrimitiveTypes.encodeCompactString(outputStream, topicName);
        PrimitiveTypes.encodeInt32(outputStream, partitionIndex);
        PrimitiveTypes.encodeTaggedFields(outputStream);
    }
}
