package messages;

import java.io.DataInputStream;
import java.io.IOException;

public abstract class AbstractRequest {
    protected final RequestHeader header;

    // Constructor
    public AbstractRequest(RequestHeader header) {
        this.header = header;
    }

    public AbstractRequest(){

        header = null;
    }

    // Getter for header
    public RequestHeader getHeader() {
        return header;
    }

    // Abstract method for decoding body
    public abstract Object decodeBody(DataInputStream inputStream) throws IOException;
}
