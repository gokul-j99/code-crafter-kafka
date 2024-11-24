package messages;

import Kafka.PrimitiveTypes;

import java.io.DataOutputStream;
import java.io.IOException;

public class FetchResponseAbortedTransaction {
    private final long producerId;
    private final long firstOffset;

    public FetchResponseAbortedTransaction(long producerId, long firstOffset) {
        this.producerId = producerId;
        this.firstOffset = firstOffset;
    }

    public void encode(DataOutputStream outputStream) throws IOException {
        PrimitiveTypes.encodeInt64(outputStream, producerId);
        PrimitiveTypes.encodeInt64(outputStream, firstOffset);
        PrimitiveTypes.encodeTaggedFields(outputStream);
    }
}
