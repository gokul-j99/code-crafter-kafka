package messages;

import Kafka.Constants;
import Kafka.PrimitiveTypes;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ApiVersionsResponse extends AbstractResponse {
    private final Constants.ErrorCode errorCode;
    private final List<ApiVersionsResponseApiKey> apiKeys;
    private final int throttleTimeMs;

    // Constructor
    public ApiVersionsResponse(ResponseHeader header, Constants.ErrorCode errorCode, List<ApiVersionsResponseApiKey> apiKeys, int throttleTimeMs) {
        super(header);
        this.errorCode = errorCode;
        this.apiKeys = apiKeys;
        this.throttleTimeMs = throttleTimeMs;
    }

    // Create body arguments based on request
    public static ApiVersionsResponse makeBody(ResponseHeader header, AbstractRequest request) {
        Constants.ErrorCode errorCode;
        if (List.of(0, 1, 2, 3, 4).contains(request.getHeader().getApiVersion())) {
            errorCode = Constants.ErrorCode.NONE;
        } else {
            errorCode = Constants.ErrorCode.UNSUPPORTED_VERSION;
        }

        List<ApiVersionsResponseApiKey> apiKeys = new ArrayList<>();
        apiKeys.add(new ApiVersionsResponseApiKey(Constants.ApiKey.FETCH, (short) 0, (short) 16));
        apiKeys.add(new ApiVersionsResponseApiKey(Constants.ApiKey.API_VERSIONS, (short) 0, (short) 4));
        apiKeys.add(new ApiVersionsResponseApiKey(Constants.ApiKey.DESCRIBE_TOPIC_PARTITIONS, (short) 0, (short) 0));

        for (ApiVersionsResponseApiKey apikey: apiKeys
             ) {
            System.out.println(apikey);

        }

        return new ApiVersionsResponse(header, errorCode, apiKeys, 0);
    }

    @Override
    public Object makeBody(ResponseHeader request) {
        return null;
    }

    // Encode the body of the response
    @Override
    protected byte[] encodeBody() throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream dataStream = new DataOutputStream(byteStream);

        errorCode.encode(dataStream);
        PrimitiveTypes.encodeCompactArray(dataStream, apiKeys, (output, apiKey) -> ((ApiVersionsResponseApiKey) apiKey).encode(output));
        PrimitiveTypes.encodeInt32(dataStream, throttleTimeMs);
        PrimitiveTypes.encodeTaggedFields(dataStream);

        return byteStream.toByteArray();
    }
}
