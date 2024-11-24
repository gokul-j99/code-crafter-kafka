package messages;

import Kafka.Constants;
import Kafka.PrimitiveTypes;
import metadata.ClusterMetadata;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class DescribeTopicPartitionsResponseTopic {
    private final Constants.ErrorCode errorCode;
    private final String name;
    private final UUID topicId;
    private final boolean isInternal;
    private final List<DescribeTopicPartitionsResponsePartition> partitions;
    private final int topicAuthorizedOperations;

    // Constructor
    public DescribeTopicPartitionsResponseTopic(Constants.ErrorCode errorCode, String name, UUID topicId, boolean isInternal,
                                                List<DescribeTopicPartitionsResponsePartition> partitions,
                                                int topicAuthorizedOperations) {
        this.errorCode = errorCode;
        this.name = name;
        this.topicId = topicId;
        this.isInternal = isInternal;
        this.partitions = partitions;
        this.topicAuthorizedOperations = topicAuthorizedOperations;
    }

    // Encode method
    public void encode(DataOutputStream outputStream) throws IOException {
        errorCode.encode(outputStream);
        PrimitiveTypes.encodeCompactNullableString(outputStream, name);
        PrimitiveTypes.encodeUUID(outputStream, topicId);
        PrimitiveTypes.encodeBoolean(outputStream, isInternal);
        PrimitiveTypes.encodeCompactArray(outputStream, partitions, (stream, partition) -> partition.encode(stream));
        PrimitiveTypes.encodeInt32(outputStream, topicAuthorizedOperations);
        PrimitiveTypes.encodeTaggedFields(outputStream);
    }


    // Factory method to create a topic response from a topic name
    public static DescribeTopicPartitionsResponseTopic fromTopicName(String topicName) {
        ClusterMetadata clusterMetadata = ClusterMetadata.getInstance();
        UUID topicId = clusterMetadata.getTopicId(topicName);

        if (topicId == null) {
            return new DescribeTopicPartitionsResponseTopic(
                    Constants.ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                    topicName,
                    new UUID(0, 0), // Null UUID
                    false,
                    List.of(), // Empty partitions
                    0
            );
        }

        List<DescribeTopicPartitionsResponsePartition> partitions = clusterMetadata.getPartitionIndices(topicId).stream()
                .map(partitionIndex -> new DescribeTopicPartitionsResponsePartition(
                        Constants.ErrorCode.NONE,
                        partitionIndex,
                        0,
                        0,
                        List.of(),
                        List.of(),
                        List.of(),
                        List.of(),
                        List.of()
                ))
                .toList();

        return new DescribeTopicPartitionsResponseTopic(
                Constants.ErrorCode.NONE,
                topicName,
                topicId,
                false,
                partitions,
                0
        );
    }
}
