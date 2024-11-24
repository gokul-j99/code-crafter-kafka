package messages;

import Kafka.PrimitiveTypes;

import java.io.DataInputStream;
import java.io.IOException;

public class DescribeTopicPartitionsRequestTopic {
    private final String name;

    // Constructor
    public DescribeTopicPartitionsRequestTopic(String name) {
        this.name = name;
    }

    // Getter
    public String getName() {
        return name;
    }

    // Decode method
    public static DescribeTopicPartitionsRequestTopic decode(DataInputStream inputStream) throws IOException {
        String name = PrimitiveTypes.decodeCompactString(inputStream);
        PrimitiveTypes.decodeTaggedFields(inputStream);
        return new DescribeTopicPartitionsRequestTopic(name);
    }
}
