package messages;

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



    public FetchRequest(int maxWaitMs, int minBytes, int maxBytes, int isolationLevel, int sessionId, int sessionEpoch,
                        List<FetchRequestTopic> topics, List<FetchRequestForgottenTopic> forgottenTopicsData,
                        String rackId) {

        this.maxWaitMs = maxWaitMs;
        this.minBytes = minBytes;
        this.maxBytes = maxBytes;
        this.isolationLevel = isolationLevel;
        this.sessionId = sessionId;
        this.sessionEpoch = sessionEpoch;
        this.topics = topics;
        this.forgottenTopicsData = forgottenTopicsData;
        this.rackId = rackId;
    }

    public static FetchRequest decodeBody(DataInputStream inputStream,  RequestHeader requestHeader ) throws IOException {
        int maxWaitMs = inputStream.readInt();
        int minBytes = inputStream.readInt();
        int maxBytes = inputStream.readInt();
        int isolationLevel = inputStream.readByte();
        int sessionId = inputStream.readInt();
        int sessionEpoch = inputStream.readInt();
        List<FetchRequestTopic> topics = decodeCompactArray(inputStream, FetchRequestTopic::decode);
        List<FetchRequestForgottenTopic> forgottenTopicsData =
                decodeCompactArray(inputStream, FetchRequestForgottenTopic::decode);
        String rackId = decodeCompactString(inputStream);
        decodeTaggedFields(inputStream);

        return new FetchRequest(maxWaitMs, minBytes, maxBytes, isolationLevel, sessionId, sessionEpoch, topics,
                forgottenTopicsData, rackId);
    }

    private static String decodeCompactString(DataInputStream inputStream) throws IOException {
        int length = inputStream.readUnsignedShort();
        if (length == 0) {
            return "";
        }
        byte[] bytes = new byte[length];
        inputStream.readFully(bytes);
        return new String(bytes, "UTF-8");
    }

    private static void decodeTaggedFields(DataInputStream inputStream) throws IOException {
        inputStream.skipBytes(inputStream.available());
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
