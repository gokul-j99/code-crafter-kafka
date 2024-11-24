package messages;


import Kafka.Constants;
import Kafka.PrimitiveTypes;

import java.io.DataInputStream;
import java.io.IOException;

public class RequestHeader {
    private final Constants.ApiKey apiKey;
    private final int apiVersion;
    private final int correlationId;
    private final String clientId;

    // Constructor
    public RequestHeader(Constants.ApiKey apiKey, int apiVersion, int correlationId, String clientId) {
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.correlationId = correlationId;
        this.clientId = clientId;
    }

    // Getters
    public Constants.ApiKey getApiKey() {
        return apiKey;
    }

    public int getApiVersion() {
        return apiVersion;
    }

    public int getCorrelationId() {
        return correlationId;
    }

    public String getClientId() {
        return clientId;
    }

    // Decode method
    public static RequestHeader decode(DataInputStream inputStream) throws IOException {
        System.out.println("Apikey:");
        Constants.ApiKey apiKey = Constants.ApiKey.decode(inputStream);

        System.out.println(apiKey);
        System.out.println("Apiversion:");
        int apiVersion = inputStream.readShort(); // Decode as a 16-bit integer
        System.out.println(apiVersion);
        int correlationId = inputStream.readInt(); // Decode as a 32-bit integer
        System.out.println("Corelationid:");
        System.out.println(correlationId);
        String clientId = decodeNullableString(inputStream);
        System.out.println("Clientid:");
        System.out.println(clientId);
        PrimitiveTypes.decodeTaggedFields(inputStream); // Handle tagged fields (if necessary)

        return new RequestHeader(apiKey, apiVersion, correlationId, clientId);
    }

    // Decode a nullable string
    private static String decodeNullableString(DataInputStream inputStream) throws IOException {
        int length = inputStream.readShort(); // Read length as 16-bit integer
        if (length == -1) { // Length of -1 indicates null
            return null;
        }
        byte[] bytes = new byte[length];
        inputStream.readFully(bytes);
        return new String(bytes, "UTF-8"); // Decode bytes to UTF-8 string
    }

    // Decode tagged fields (stub for now)
    private static void decodeTaggedFields(DataInputStream inputStream) throws IOException {
        // Logic to decode tagged fields can be added here if necessary
        // Example: Skip or process tagged fields
    }
}
