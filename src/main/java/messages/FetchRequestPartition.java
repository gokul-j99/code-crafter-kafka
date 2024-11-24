package messages;

import java.io.DataInputStream;
import java.io.IOException;

public class FetchRequestPartition {
    private final int partition;
    private final int currentLeaderEpoch;
    private final long fetchOffset;
    private final int lastFetchedEpoch;
    private final long logStartOffset;
    private final int partitionMaxBytes;

    public FetchRequestPartition(int partition, int currentLeaderEpoch, long fetchOffset, int lastFetchedEpoch,
                                 long logStartOffset, int partitionMaxBytes) {
        this.partition = partition;
        this.currentLeaderEpoch = currentLeaderEpoch;
        this.fetchOffset = fetchOffset;
        this.lastFetchedEpoch = lastFetchedEpoch;
        this.logStartOffset = logStartOffset;
        this.partitionMaxBytes = partitionMaxBytes;
    }

    public static FetchRequestPartition decode(DataInputStream inputStream) throws IOException {
        int partition = inputStream.readInt();
        int currentLeaderEpoch = inputStream.readInt();
        long fetchOffset = inputStream.readLong();
        int lastFetchedEpoch = inputStream.readInt();
        long logStartOffset = inputStream.readLong();
        int partitionMaxBytes = inputStream.readInt();
        decodeTaggedFields(inputStream);
        return new FetchRequestPartition(partition, currentLeaderEpoch, fetchOffset, lastFetchedEpoch,
                logStartOffset, partitionMaxBytes);
    }

    private static void decodeTaggedFields(DataInputStream inputStream) throws IOException {
        // Placeholder for decoding tagged fields
        inputStream.skipBytes(inputStream.available());
    }

    public int getPartition() {
        return partition;
    }

    public int getCurrentLeaderEpoch() {
        return currentLeaderEpoch;
    }

    public long getFetchOffset() {
        return fetchOffset;
    }

    public int getLastFetchedEpoch() {
        return lastFetchedEpoch;
    }

    public long getLogStartOffset() {
        return logStartOffset;
    }

    public int getPartitionMaxBytes() {
        return partitionMaxBytes;
    }
}
