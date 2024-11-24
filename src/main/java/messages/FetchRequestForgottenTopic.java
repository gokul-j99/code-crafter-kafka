package messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import Kafka.PrimitiveTypes;

import static Kafka.PrimitiveTypes.decodeCompactArray;

public class FetchRequestForgottenTopic {
    private final UUID topicId;
    private final List<Integer> partitions;

    public FetchRequestForgottenTopic(UUID topicId, List<Integer> partitions) {
        this.topicId = topicId;
        this.partitions = partitions;
    }

    public static FetchRequestForgottenTopic decode(DataInputStream inputStream) throws IOException {
        UUID topicId = decodeUUID(inputStream);
        List<Integer> partitions = decodeCompactArray(inputStream, input -> input.readInt());
        decodeTaggedFields(inputStream);
        return new FetchRequestForgottenTopic(topicId, partitions);
    }

    private static UUID decodeUUID(DataInputStream inputStream) throws IOException {
        long mostSigBits = inputStream.readLong();
        long leastSigBits = inputStream.readLong();
        return new UUID(mostSigBits, leastSigBits);
    }

    private static void decodeTaggedFields(DataInputStream inputStream) throws IOException {
        inputStream.skipBytes(inputStream.available());
    }
}
