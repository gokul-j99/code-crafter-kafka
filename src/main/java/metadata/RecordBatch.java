package metadata;

import Kafka.PrimitiveTypes;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class RecordBatch {
    private final long baseOffset;
    private final int batchLength;
    private final int partitionLeaderEpoch;
    private final byte magic;
    private final long crc;
    private final short attributes;
    private final int lastOffsetDelta;
    private final long baseTimestamp;
    private final long maxTimestamp;
    private final long producerId;
    private final short producerEpoch;
    private final int baseSequence;
    private final List<Record> records;

    // Constructor
    public RecordBatch(long baseOffset, int batchLength, int partitionLeaderEpoch, byte magic, long crc,
                       short attributes, int lastOffsetDelta, long baseTimestamp, long maxTimestamp, long producerId,
                       short producerEpoch, int baseSequence, List<Record> records) {
        this.baseOffset = baseOffset;
        this.batchLength = batchLength;
        this.partitionLeaderEpoch = partitionLeaderEpoch;
        this.magic = magic;
        this.crc = crc;
        this.attributes = attributes;
        this.lastOffsetDelta = lastOffsetDelta;
        this.baseTimestamp = baseTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        this.records = records;
    }

    // Getters
    public long getBaseOffset() {
        return baseOffset;
    }

    public int getBatchLength() {
        return batchLength;
    }

    public int getPartitionLeaderEpoch() {
        return partitionLeaderEpoch;
    }

    public byte getMagic() {
        return magic;
    }

    public long getCrc() {
        return crc;
    }

    public short getAttributes() {
        return attributes;
    }

    public int getLastOffsetDelta() {
        return lastOffsetDelta;
    }

    public long getBaseTimestamp() {
        return baseTimestamp;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public long getProducerId() {
        return producerId;
    }

    public short getProducerEpoch() {
        return producerEpoch;
    }

    public int getBaseSequence() {
        return baseSequence;
    }

    public List<Record> getRecords() {
        return records;
    }

    // Decode method
    public static RecordBatch decode(DataInputStream inputStream) throws IOException {
        long baseOffset = PrimitiveTypes.decodeInt64(inputStream);
        int batchLength = PrimitiveTypes.decodeInt32(inputStream);
        int partitionLeaderEpoch = PrimitiveTypes.decodeInt32(inputStream);
        byte magic = PrimitiveTypes.decodeInt8(inputStream);
        long crc = PrimitiveTypes.decodeUInt32(inputStream);
        short attributes = PrimitiveTypes.decodeInt16(inputStream);
        int lastOffsetDelta = PrimitiveTypes.decodeInt32(inputStream);
        long baseTimestamp = PrimitiveTypes.decodeInt64(inputStream);
        long maxTimestamp = PrimitiveTypes.decodeInt64(inputStream);
        long producerId = PrimitiveTypes.decodeInt64(inputStream);
        short producerEpoch = PrimitiveTypes.decodeInt16(inputStream);
        int baseSequence = PrimitiveTypes.decodeInt32(inputStream);
        List<Record> records = PrimitiveTypes.decodeArray(inputStream, Record::decode);

        return new RecordBatch(baseOffset, batchLength, partitionLeaderEpoch, magic, crc, attributes, lastOffsetDelta,
                baseTimestamp, maxTimestamp, producerId, producerEpoch, baseSequence, records);
    }

    // Encode method
    public void encode(DataOutputStream outputStream) throws IOException {
        PrimitiveTypes.encodeInt64(outputStream, baseOffset);
        PrimitiveTypes.encodeInt32(outputStream, batchLength);
        PrimitiveTypes.encodeInt32(outputStream, partitionLeaderEpoch);
        PrimitiveTypes.encodeInt8(outputStream, magic);
        PrimitiveTypes.encodeUInt32(outputStream, crc);
        PrimitiveTypes.encodeInt16(outputStream, attributes);
        PrimitiveTypes.encodeInt32(outputStream, lastOffsetDelta);
        PrimitiveTypes.encodeInt64(outputStream, baseTimestamp);
        PrimitiveTypes.encodeInt64(outputStream, maxTimestamp);
        PrimitiveTypes.encodeInt64(outputStream, producerId);
        PrimitiveTypes.encodeInt16(outputStream, producerEpoch);
        PrimitiveTypes.encodeInt32(outputStream, baseSequence);
        PrimitiveTypes.encodeArray(outputStream, records, (stream, record) -> record.encode(stream));
    }
}
