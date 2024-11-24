package messages;

import Kafka.Constants;
import Kafka.PrimitiveTypes;
import metadata.ClusterMetadata;
import metadata.RecordBatch;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class FetchResponseTopic {
    private final UUID topicId;
    private final List<FetchResponsePartition> partitions;

    public FetchResponseTopic(UUID topicId, List<FetchResponsePartition> partitions) {
        this.topicId = topicId;
        this.partitions = partitions;
    }

    public static FetchResponseTopic fromRequestTopic(FetchRequestTopic requestTopic) throws IOException {
        ClusterMetadata clusterMetadata = ClusterMetadata.getInstance();
        String topicName = clusterMetadata.getTopicName(requestTopic.getTopicId());

        if (topicName == null) {
            List<FetchResponsePartition> partitions = List.of(
                    new FetchResponsePartition(0, Constants.ErrorCode.UNKNOWN_TOPIC_ID, 0, 0, 0, List.of(), 0, List.of())
            );
            return new FetchResponseTopic(requestTopic.getTopicId(), partitions);
        }

        List<FetchResponsePartition> partitions = new ArrayList<>();
        for (FetchRequestPartition partition : requestTopic.getPartitions()) {
            List<RecordBatch> records = (List<RecordBatch>) clusterMetadata.readRecordBatches(topicName, partition.getPartition());
            partitions.add(new FetchResponsePartition(partition.getPartition(), Constants.ErrorCode.NONE, 0, 0, 0, List.of(), 0, records));
        }

        return new FetchResponseTopic(requestTopic.getTopicId(), partitions);
    }

    public void encode(DataOutputStream outputStream) throws IOException {
        PrimitiveTypes.encodeUUID(outputStream, topicId);
        PrimitiveTypes.encodeCompactArray(outputStream, partitions, (stream, partition) -> partition.encode(stream));
        PrimitiveTypes.encodeTaggedFields(outputStream);
    }
}
