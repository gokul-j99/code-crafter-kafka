package messages;

import Kafka.PrimitiveTypes;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public abstract class AbstractResponse {
    protected final ResponseHeader header;

    // Constructor
    public AbstractResponse(ResponseHeader header) {
        this.header = header;
    }

    // Getter for header
    public ResponseHeader getHeader() {
        return header;
    }

    // Abstract method to generate body arguments based on the request
    public abstract Object makeBody(ResponseHeader request);

    // Abstract method to encode the body
    protected abstract byte[] encodeBody() throws IOException;

    // Encode the response
    public byte[] encode() throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream dataStream = new DataOutputStream(byteStream);

        // Encode header
        header.encode(dataStream);

        // Encode body
        dataStream.write(encodeBody());

        // Get the encoded response
        byte[] encodedResponse = byteStream.toByteArray();

        // Prepend the length of the response
        ByteArrayOutputStream finalStream = new ByteArrayOutputStream();
        DataOutputStream finalDataStream = new DataOutputStream(finalStream);

        PrimitiveTypes.encodeInt32(finalDataStream, encodedResponse.length);
        finalDataStream.write(encodedResponse);

        return finalStream.toByteArray();
    }
}
