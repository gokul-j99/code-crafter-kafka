package metadata;

import Kafka.PrimitiveTypes;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.CRC32;

public class RecordBatch {
    private final long baseOffset;
    private final int batchLength;
    private final int partitionLeaderEpoch;
    private final byte magic;
    private long crc; // Marked as mutable for recalculation
    private final short attributes;
    private final int lastOffsetDelta;
    private final long baseTimestamp;
    private final long maxTimestamp;
    private final long producerId;
    private final short producerEpoch;
    private final int baseSequence;

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

    public void setCrc(long crc) {
        this.crc = crc;
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

    // Getters (unchanged)
    // ... (All getters are fine)

    // Decode method
    public static RecordBatch decode(DataInputStream inputStream) throws IOException {
        RecordBatch rec = new RecordBatch(
                inputStream.readLong(),
                inputStream.readInt(),
                inputStream.readInt(),
                inputStream.readByte(), // magic
                PrimitiveTypes.decodeUInt32(inputStream), // crc
                inputStream.readShort(),
                inputStream.readInt(),
                inputStream.readLong(),
                inputStream.readLong(),
                inputStream.readLong(),
                inputStream.readShort(),
                inputStream.readInt(),
                PrimitiveTypes.decodeArray(inputStream, Record::decode)); // records
        System.out.println("Record");
        System.out.println(rec);
        return rec;
    }

    // Encode method with CRC recalculation
    public void encode(DataOutputStream outputStream) throws IOException {
        // Create a buffer to hold the serialized data
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream tempStream = new DataOutputStream(buffer);

        // Write fields before CRC
        PrimitiveTypes.encodeInt64(tempStream, baseOffset);
        PrimitiveTypes.encodeInt32(tempStream, 0); // Placeholder for batchLength
        PrimitiveTypes.encodeInt32(tempStream, partitionLeaderEpoch);
        PrimitiveTypes.encodeInt8(tempStream, magic);
        int crcStartOffset = tempStream.size();
        PrimitiveTypes.encodeUInt32(tempStream, 0); // Placeholder for CRC
        int crcEndOffset = tempStream.size();

        // Write remaining fields
        PrimitiveTypes.encodeInt16(tempStream, attributes);
        PrimitiveTypes.encodeInt32(tempStream, lastOffsetDelta);
        PrimitiveTypes.encodeInt64(tempStream, baseTimestamp);
        PrimitiveTypes.encodeInt64(tempStream, maxTimestamp);
        PrimitiveTypes.encodeInt64(tempStream, producerId);
        PrimitiveTypes.encodeInt16(tempStream, producerEpoch);
        PrimitiveTypes.encodeInt32(tempStream, baseSequence);
        PrimitiveTypes.encodeArray(tempStream, records, (stream, record) -> record.encode(stream));

        // Update batch length
        int batchLength = tempStream.size() - 12; // 12 bytes: BaseOffset (8) + BatchLength (4)
        PrimitiveTypes.encodeInt32At(buffer.toByteArray(), 8, batchLength);

        // Recalculate CRC for fields after BaseOffset, BatchLength, and PartitionLeaderEpoch
        CRC32 crc32 = new CRC32();
        crc32.update(buffer.toByteArray(), crcEndOffset, tempStream.size() - crcEndOffset);
        this.crc = crc32.getValue(); // Update internal CRC field
        PrimitiveTypes.encodeUInt32At(buffer.toByteArray(), crcStartOffset, (int) this.crc);

        // Write final buffer to the output stream
        outputStream.write(buffer.toByteArray());
    }

    @Override
    public String toString() {
        return "RecordBatch{" +
                "baseOffset=" + baseOffset +
                ", batchLength=" + batchLength +
                ", partitionLeaderEpoch=" + partitionLeaderEpoch +
                ", magic=" + magic +
                ", crc=" + crc +
                ", attributes=" + attributes +
                ", lastOffsetDelta=" + lastOffsetDelta +
                ", baseTimestamp=" + baseTimestamp +
                ", maxTimestamp=" + maxTimestamp +
                ", producerId=" + producerId +
                ", producerEpoch=" + producerEpoch +
                ", baseSequence=" + baseSequence +
                ", records=" + records +
                '}';
    }
}
