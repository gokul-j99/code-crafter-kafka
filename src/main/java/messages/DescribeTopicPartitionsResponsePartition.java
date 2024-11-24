package messages;

import Kafka.Constants;
import Kafka.PrimitiveTypes;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class DescribeTopicPartitionsResponsePartition {
    private final Constants.ErrorCode errorCode;
    private final int partitionIndex;
    private final int leaderId;
    private final int leaderEpoch;
    private final List<Integer> replicaNodes;
    private final List<Integer> isrNodes;
    private final List<Integer> eligibleLeaderReplicas;
    private final List<Integer> lastKnownElr;
    private final List<Integer> offlineReplicas;

    // Constructor
    public DescribeTopicPartitionsResponsePartition(Constants.ErrorCode errorCode, int partitionIndex, int leaderId, int leaderEpoch,
                                                    List<Integer> replicaNodes, List<Integer> isrNodes,
                                                    List<Integer> eligibleLeaderReplicas, List<Integer> lastKnownElr,
                                                    List<Integer> offlineReplicas) {
        this.errorCode = errorCode;
        this.partitionIndex = partitionIndex;
        this.leaderId = leaderId;
        this.leaderEpoch = leaderEpoch;
        this.replicaNodes = replicaNodes;
        this.isrNodes = isrNodes;
        this.eligibleLeaderReplicas = eligibleLeaderReplicas;
        this.lastKnownElr = lastKnownElr;
        this.offlineReplicas = offlineReplicas;
    }

    // Encode method
    public void encode(DataOutputStream outputStream) throws IOException {
        errorCode.encode(outputStream);
        PrimitiveTypes.encodeInt32(outputStream, partitionIndex);
        PrimitiveTypes.encodeInt32(outputStream, leaderId);
        PrimitiveTypes.encodeInt32(outputStream, leaderEpoch);
        PrimitiveTypes.encodeCompactArray(outputStream, replicaNodes, PrimitiveTypes::encodeInt32);
        PrimitiveTypes.encodeCompactArray(outputStream, isrNodes, PrimitiveTypes::encodeInt32);
        PrimitiveTypes.encodeCompactArray(outputStream, eligibleLeaderReplicas, PrimitiveTypes::encodeInt32);
        PrimitiveTypes.encodeCompactArray(outputStream, lastKnownElr, PrimitiveTypes::encodeInt32);
        PrimitiveTypes.encodeCompactArray(outputStream, offlineReplicas, PrimitiveTypes::encodeInt32);
        PrimitiveTypes.encodeTaggedFields(outputStream);
    }
}
