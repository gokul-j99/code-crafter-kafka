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
        if (inputStream.available() < 16) {

            System.out.println("Not enough bytes to decode forgotten topic UUID ");
            throw new EOFException("Not enough bytes to decode forgotten topic UUID");
        }

        UUID topicId = decodeUUID(inputStream); // Read the topic ID
        List<Integer> partitions = decodeCompactArray(inputStream, input -> {
            if (input.available() < 4) {
                System.out.println("Not enough bytes to decode partition ");
                throw new EOFException("Not enough bytes to decode partition");
            }
            return input.readInt(); // Read each partition
        });

        PrimitiveTypes.decodeTaggedFields(inputStream); // Skip tagged fields if present
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
