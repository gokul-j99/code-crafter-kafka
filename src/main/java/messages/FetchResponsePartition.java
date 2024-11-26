package messages;

import Kafka.Constants;
import Kafka.PrimitiveTypes;
import metadata.RecordBatch;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class FetchResponsePartition {
    private final int partitionIndex;
    private final Constants.ErrorCode errorCode;
    private final long highWatermark;
    private final long lastStableOffset;
    private final long logStartOffset;
    private final List<FetchResponseAbortedTransaction> abortedTransactions;
    private final int preferredReadReplica;
    private final List<RecordBatch> records;

    public FetchResponsePartition(int partitionIndex, Constants.ErrorCode errorCode, long highWatermark, long lastStableOffset,
                                  long logStartOffset, List<FetchResponseAbortedTransaction> abortedTransactions,
                                  int preferredReadReplica, List<RecordBatch> records) {
        this.partitionIndex = partitionIndex;
        this.errorCode = errorCode;
        this.highWatermark = highWatermark;
        this.lastStableOffset = lastStableOffset;
        this.logStartOffset = logStartOffset;
        this.abortedTransactions = abortedTransactions;
        this.preferredReadReplica = preferredReadReplica;
        this.records = records;
    }

    public void encode(DataOutputStream outputStream) throws IOException {
        PrimitiveTypes.encodeInt32(outputStream, partitionIndex);
        PrimitiveTypes.encodeInt16(outputStream, (short) errorCode.getValue());
        PrimitiveTypes.encodeInt64(outputStream, highWatermark);
        PrimitiveTypes.encodeInt64(outputStream, lastStableOffset);
        PrimitiveTypes.encodeInt64(outputStream, logStartOffset);
        PrimitiveTypes.encodeCompactArray(outputStream, abortedTransactions, (stream, transaction) -> transaction.encode(stream));
        PrimitiveTypes.encodeInt32(outputStream, preferredReadReplica);
        PrimitiveTypes.encodeCompactArray(outputStream, records, (stream, record) -> record.encode(stream));
        PrimitiveTypes.encodeTaggedFields(outputStream);
    }
}
