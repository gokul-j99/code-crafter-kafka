package messages;

import java.io.DataInputStream;
import java.io.EOFException;
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
        System.out.println(topicId);
        System.out.println(partitions);
        PrimitiveTypes.decodeTaggedFields(inputStream);
        return new FetchRequestForgottenTopic(topicId, partitions);
    }

    private static UUID decodeUUID(DataInputStream inputStream) throws IOException {
        if (inputStream.available() < 16) {
            throw new EOFException("Not enough bytes to decode UUID");
        }
        long mostSigBits = inputStream.readLong();
        long leastSigBits = inputStream.readLong();
        return new UUID(mostSigBits, leastSigBits);
    }




    @Override
    public String toString() {
        return "FetchRequestForgottenTopic{" +
                "topicId=" + topicId +
                ", partitions=" + partitions +
                '}';
    }
}
