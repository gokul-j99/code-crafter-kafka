package messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static Kafka.PrimitiveTypes.decodeCompactArray;

public class FetchRequestTopic {
    private final UUID topicId;
    private final List<FetchRequestPartition> partitions;

    public UUID getTopicId() {
        return topicId;
    }

    public List<FetchRequestPartition> getPartitions() {
        return partitions;
    }

    public FetchRequestTopic(UUID topicId, List<FetchRequestPartition> partitions) {
        this.topicId = topicId;
        this.partitions = partitions;
    }

    public static FetchRequestTopic decode(DataInputStream inputStream) throws IOException {
        UUID topicId = decodeUUID(inputStream);
        List<FetchRequestPartition> partitions = decodeCompactArray(inputStream, FetchRequestPartition::decode);
        decodeTaggedFields(inputStream);
        return new FetchRequestTopic(topicId, partitions);
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
