package messages;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class MessageHandler {

    public static AbstractRequest readRequest(DataInputStream inputStream) throws IOException {
        int n = inputStream.readInt();
        byte[] data = new byte[n];
        inputStream.readFully(data);
        System.out.println("Length:");
        System.out.println(n);
        ByteArrayInputStream binaryStream = new ByteArrayInputStream(data);
        DataInputStream dataInputStream = new DataInputStream(binaryStream);

        RequestHeader requestHeader = RequestHeader.decode(dataInputStream);
        AbstractRequest request;

        switch (requestHeader.getApiKey()) {
            case FETCH:
                System.out.println("Fetch req");
                request = FetchRequest.decodeBody(dataInputStream, requestHeader);

                break;
            case API_VERSIONS:
                System.out.println("APi ver req");
                request = ApiVersionsRequest.decodeBody(dataInputStream, requestHeader);
                break;
            case DESCRIBE_TOPIC_PARTITIONS:
                System.out.println("Topic req");
                request = DescribeTopicPartitionsRequest.decodeBody(dataInputStream, requestHeader);
                break;
            default:
                throw new IllegalArgumentException("Unsupported API key: " + requestHeader.getApiKey());
        }

        return request;
    }

    public static AbstractResponse makeResponse(AbstractRequest request) {
        ResponseHeader responseHeader = ResponseHeader.fromRequestHeader(request.getHeader());
        AbstractResponse response;

        switch (responseHeader.getApiKey()) {
            case FETCH:
                response = FetchResponse.makeBody(responseHeader,request);
                break;
            case API_VERSIONS:
                System.out.println("APi ver res");
                response = ApiVersionsResponse.makeBody(responseHeader,  request);
                break;
            case DESCRIBE_TOPIC_PARTITIONS:
                response = DescribeTopicPartitionsResponse.makeBody(responseHeader, request);
                break;
            default:
                throw new IllegalArgumentException("Unsupported API key: " + responseHeader.getApiKey());
        }

        return response;
    }
}
