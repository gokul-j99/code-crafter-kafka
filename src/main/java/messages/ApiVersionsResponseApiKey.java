package messages;

import Kafka.Constants;
import Kafka.PrimitiveTypes;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ApiVersionsResponseApiKey {
    private static  Constants.ApiKey apiKey;
    private static  short minVersion;
    private static  short maxVersion;

    // Constructor
    public ApiVersionsResponseApiKey(Constants.ApiKey apiKey, short minVersion, short maxVersion) {
        this.apiKey = apiKey;
        this.minVersion = minVersion;
        this.maxVersion = maxVersion;
    }

    // Encode the response API key
    public void encode(DataOutputStream outputStream) throws IOException {
        apiKey.encode(outputStream);
        PrimitiveTypes.encodeInt16(outputStream, minVersion);
        PrimitiveTypes.encodeInt16(outputStream, maxVersion);
        PrimitiveTypes.encodeTaggedFields(outputStream);
    }
}