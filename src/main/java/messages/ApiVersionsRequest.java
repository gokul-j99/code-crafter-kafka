package messages;

import Kafka.PrimitiveTypes;

import java.io.DataInputStream;
import java.io.IOException;

public class ApiVersionsRequest extends AbstractRequest {
    private final String clientSoftwareName;
    private final String clientSoftwareVersion;

    // Constructor
    public ApiVersionsRequest(RequestHeader header, String clientSoftwareName, String clientSoftwareVersion) {
        super(header);
        this.clientSoftwareName = clientSoftwareName;
        this.clientSoftwareVersion = clientSoftwareVersion;
    }

    // Getters
    public String getClientSoftwareName() {
        return clientSoftwareName;
    }

    public String getClientSoftwareVersion() {
        return clientSoftwareVersion;
    }

    // Decode body
    public static ApiVersionsRequest decodeBody(DataInputStream inputStream, RequestHeader header) throws IOException {
        String clientSoftwareName = PrimitiveTypes.decodeCompactString(inputStream);
        String clientSoftwareVersion = PrimitiveTypes.decodeCompactString(inputStream);
        System.out.println("clientSoftwareName"+  clientSoftwareName);
        System.out.println("clientSoftwareVersion " + clientSoftwareVersion);

        PrimitiveTypes.decodeTaggedFields(inputStream);

        return new ApiVersionsRequest(header, clientSoftwareName, clientSoftwareVersion);
    }

    @Override
    public Object decodeBody(DataInputStream inputStream) throws IOException {
        return null;
    }
}
