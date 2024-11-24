package messages;

import Kafka.PrimitiveTypes;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class DescribeTopicPartitionsResponse extends AbstractResponse {
    private final int throttleTimeMs;
    private final List<DescribeTopicPartitionsResponseTopic> topics;
    private final DescribeTopicPartitionsCursor nextCursor;

    // Constructor
    public DescribeTopicPartitionsResponse(ResponseHeader header, int throttleTimeMs,
                                           List<DescribeTopicPartitionsResponseTopic> topics,
                                           DescribeTopicPartitionsCursor nextCursor) {
        super(header);
        this.throttleTimeMs = throttleTimeMs;
        this.topics = topics;
        this.nextCursor = nextCursor;
    }

    // Create body arguments based on request
    public static DescribeTopicPartitionsResponse makeBody(ResponseHeader header, AbstractRequest request) {
          DescribeTopicPartitionsRequest drequest = (DescribeTopicPartitionsRequest) request;
        List<DescribeTopicPartitionsResponseTopic> topics = drequest.getTopics().stream()
                .map(topic -> DescribeTopicPartitionsResponseTopic.fromTopicName(topic.getName()))
                .toList();

        return new DescribeTopicPartitionsResponse(header, 0, topics, drequest.getCursor());
    }

    @Override
    public Object makeBody(ResponseHeader request) {
        return null;
    }

    // Encode body
    @Override
    protected byte[] encodeBody() throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream dataStream = new DataOutputStream(byteStream);

        PrimitiveTypes.encodeInt32(dataStream, throttleTimeMs);
        PrimitiveTypes.encodeCompactArray(dataStream, topics, (stream, topic) -> topic.encode(stream));
        if (nextCursor == null) {
            dataStream.writeByte(0xFF); // No cursor
        } else {
            nextCursor.encode(dataStream);
        }
        PrimitiveTypes.encodeTaggedFields(dataStream);

        return byteStream.toByteArray();
    }

}
