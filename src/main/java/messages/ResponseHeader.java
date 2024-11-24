package messages;

import Kafka.Constants;

import java.io.DataOutputStream;
import java.io.IOException;

public class ResponseHeader {
    private final Constants.ApiKey apiKey;
    private final int correlationId;

    // Constructor
    public ResponseHeader(Constants.ApiKey apiKey, int correlationId) {
        this.apiKey = apiKey;
        this.correlationId = correlationId;
    }

    // Getters
    public Constants.ApiKey getApiKey() {
        return apiKey;
    }

    public int getCorrelationId() {
        return correlationId;
    }

    // Factory method to create ResponseHeader from RequestHeader
    public static ResponseHeader fromRequestHeader(RequestHeader requestHeader) {
        return new ResponseHeader(requestHeader.getApiKey(), requestHeader.getCorrelationId());
    }

    // Encode method
    public void encode(DataOutputStream outputStream) throws IOException {
        // Encode correlation ID
        outputStream.writeInt(correlationId);

        // Additional encoding for tagged fields (if necessary)
        if (apiKey != Constants.ApiKey.API_VERSIONS) {
            encodeTaggedFields(outputStream);
        }
    }

    // Helper method to encode tagged fields
    private void encodeTaggedFields(DataOutputStream outputStream) throws IOException {
        // Placeholder: Encode tagged fields if required
        // For now, write zero bytes for tagged fields
        outputStream.writeByte(0);
    }
}
