package messages;


import Kafka.Constants;
import Kafka.PrimitiveTypes;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class FetchResponse extends AbstractResponse {
    private final int throttleTimeMs;
    private final Constants.ErrorCode errorCode;
    private final int sessionId;
    private final List<FetchResponseTopic> responses;

    public FetchResponse(ResponseHeader responseHeader,int throttleTimeMs, Constants.ErrorCode errorCode, int sessionId, List<FetchResponseTopic> responses) {
        super(responseHeader);
        this.throttleTimeMs = throttleTimeMs;
        this.errorCode = errorCode;
        this.sessionId = sessionId;
        this.responses = responses;
    }

    public static FetchResponse makeBody(ResponseHeader responseHeader,AbstractRequest request) {
        FetchRequest fetchRequest = (FetchRequest) request;
        List<FetchResponseTopic> responses = fetchRequest.getTopics().stream()
                .map(topic -> {
                    try {
                        return FetchResponseTopic.fromRequestTopic(topic);
                    } catch (IOException e) {
                        throw new RuntimeException("Error processing topic: " + topic, e);
                    }
                })
                .toList();


        return new FetchResponse(responseHeader,0, Constants.ErrorCode.NONE, 0, responses);
    }

    @Override
    public Object makeBody(ResponseHeader request) {
        return null;
    }

    @Override
    protected byte[] encodeBody() throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream dataStream = new DataOutputStream(byteStream);

        PrimitiveTypes.encodeInt32(dataStream, throttleTimeMs);
        errorCode.encode(dataStream);
        PrimitiveTypes.encodeInt32(dataStream, sessionId);
        PrimitiveTypes.encodeCompactArray(dataStream, responses, (stream, topic) -> topic.encode(stream));
        PrimitiveTypes.encodeTaggedFields(dataStream);

        return byteStream.toByteArray();
    }
}
