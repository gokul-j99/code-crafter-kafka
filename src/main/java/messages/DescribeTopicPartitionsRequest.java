package messages;

import Kafka.PrimitiveTypes;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;

public class DescribeTopicPartitionsRequest extends AbstractRequest {
    private final List<DescribeTopicPartitionsRequestTopic> topics;
    private final int responsePartitionLimit;
    private final DescribeTopicPartitionsCursor cursor;

    // Constructor
    public DescribeTopicPartitionsRequest(RequestHeader header, List<DescribeTopicPartitionsRequestTopic> topics,
                                          int responsePartitionLimit, DescribeTopicPartitionsCursor cursor) {
        super(header);
        this.topics = topics;
        this.responsePartitionLimit = responsePartitionLimit;
        this.cursor = cursor;
    }

    // Getters
    public List<DescribeTopicPartitionsRequestTopic> getTopics() {
        return topics;
    }

    public int getResponsePartitionLimit() {
        return responsePartitionLimit;
    }

    public DescribeTopicPartitionsCursor getCursor() {
        return cursor;
    }

    // Decode body
    public static DescribeTopicPartitionsRequest decodeBody(DataInputStream inputStream, RequestHeader header) throws IOException {
        List<DescribeTopicPartitionsRequestTopic> topics = PrimitiveTypes.decodeCompactArray(
                inputStream, DescribeTopicPartitionsRequestTopic::decode);
        int responsePartitionLimit = PrimitiveTypes.decodeInt32(inputStream);
        DescribeTopicPartitionsCursor cursor = DescribeTopicPartitionsCursor.decode(inputStream);
        PrimitiveTypes.decodeTaggedFields(inputStream);

        return new DescribeTopicPartitionsRequest(header, topics, responsePartitionLimit, cursor);
    }

    @Override
    public Object decodeBody(DataInputStream inputStream) throws IOException {
        return null;
    }
}
