package messages;

import Kafka.PrimitiveTypes;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;

import static Kafka.PrimitiveTypes.decodeCompactArray;

public class FetchRequest extends AbstractRequest {
    private final int maxWaitMs;
    private final int minBytes;
    private final int maxBytes;
    private final int isolationLevel;
    private final int sessionId;
    private final int sessionEpoch;
    private final List<FetchRequestTopic> topics;
    private final List<FetchRequestForgottenTopic> forgottenTopicsData;
    private final String rackId;
    private final RequestHeader requestHeader;



    public FetchRequest(int maxWaitMs, int minBytes, int maxBytes, int isolationLevel, int sessionId, int sessionEpoch,
                        List<FetchRequestTopic> topics, List<FetchRequestForgottenTopic> forgottenTopicsData,
                        String rackId, RequestHeader requestHeader) {
        super(requestHeader);
        this.maxWaitMs = maxWaitMs;
        this.minBytes = minBytes;
        this.maxBytes = maxBytes;
        this.isolationLevel = isolationLevel;
        this.sessionId = sessionId;
        this.sessionEpoch = sessionEpoch;
        this.topics = topics;
        this.forgottenTopicsData = forgottenTopicsData;
        this.rackId = rackId;
        this.requestHeader = requestHeader;
    }

    public static FetchRequest decodeBody(DataInputStream inputStream, RequestHeader requestHeader) throws IOException {
        int maxWaitMs = inputStream.readInt();
        System.out.println("maxWaitMs :");
        System.out.println(maxWaitMs);

        int minBytes = inputStream.readInt();
        System.out.println("minBytes :");
        System.out.println(minBytes);

        int maxBytes = inputStream.readInt();
        System.out.println("maxBytes :");
        System.out.println(maxBytes);

        int isolationLevel = inputStream.readByte();
        System.out.println("isolationLevel :");
        System.out.println(isolationLevel);

        int sessionId = inputStream.readInt();
        System.out.println("sessionId :");
        System.out.println(sessionId);

        int sessionEpoch = inputStream.readInt();
        System.out.println("sessionEpoch :");
        System.out.println(sessionEpoch);

        List<FetchRequestTopic> topics = decodeCompactArray(inputStream, FetchRequestTopic::decode);
        System.out.println("FetchRequestTopic :");
        if (topics.isEmpty()) {
            System.out.println("No topics in the Fetch request.");
        } else {
            for (FetchRequestTopic topic : topics) {
                System.out.println(topic);
            }
        }
        System.out.println("forgottenTopicsData :");
        List<FetchRequestForgottenTopic> forgottenTopicsData =
                decodeCompactArray(inputStream, FetchRequestForgottenTopic::decode);
        System.out.println("forgottenTopicsData :");
        if (forgottenTopicsData.isEmpty()) {
            System.out.println("No forgotten topics in the Fetch request.");
        } else {
            for (FetchRequestForgottenTopic forgottenTopic : forgottenTopicsData) {
                System.out.println(forgottenTopic);
            }
        }

        String rackId = PrimitiveTypes.decodeCompactString(inputStream);
        System.out.println("rackId :");
        System.out.println(rackId);

        PrimitiveTypes.decodeTaggedFields(inputStream);

        return new FetchRequest(maxWaitMs, minBytes, maxBytes, isolationLevel, sessionId, sessionEpoch, topics,
                forgottenTopicsData, rackId,requestHeader);
    }






    @Override
    public Object decodeBody(DataInputStream inputStream) throws IOException {
        return null;
    }

    public int getMaxWaitMs() {
        return maxWaitMs;
    }

    public int getMinBytes() {
        return minBytes;
    }

    public int getMaxBytes() {
        return maxBytes;
    }

    public int getIsolationLevel() {
        return isolationLevel;
    }

    public int getSessionId() {
        return sessionId;
    }

    public int getSessionEpoch() {
        return sessionEpoch;
    }

    public List<FetchRequestTopic> getTopics() {
        return topics;
    }

    public List<FetchRequestForgottenTopic> getForgottenTopicsData() {
        return forgottenTopicsData;
    }

    public String getRackId() {
        return rackId;
    }
}
