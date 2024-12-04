package metadata;

import Kafka.PrimitiveTypes;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.CRC32C;

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
        long baseOffset = inputStream.readLong();
        int batchLength = inputStream.readInt();
        int partitionLeaderEpoch = inputStream.readInt();
        byte magic = inputStream.readByte();

        // Treat CRC as unsigned
        long crc = Integer.toUnsignedLong(inputStream.readInt());

        short attributes = inputStream.readShort();
        int lastOffsetDelta = inputStream.readInt();
        long baseTimestamp = inputStream.readLong();
        long maxTimestamp = inputStream.readLong();
        long producerId = inputStream.readLong();
        short producerEpoch = inputStream.readShort();
        int baseSequence = inputStream.readInt();
        List<Record> records = PrimitiveTypes.decodeArray(inputStream, Record::decode);

        return new RecordBatch(
                baseOffset, batchLength, partitionLeaderEpoch, magic, crc,
                attributes, lastOffsetDelta, baseTimestamp, maxTimestamp,
                producerId, producerEpoch, baseSequence, records
        );
    }


    // Encode method with CRC recalculation
    public void encode(DataOutputStream outputStream) throws IOException {
        ByteArrayOutputStream bufferStream = new ByteArrayOutputStream();
        DataOutputStream bufferOutput = new DataOutputStream(bufferStream);

        PrimitiveTypes.encodeInt64(bufferOutput, baseOffset);
        PrimitiveTypes.encodeInt32(bufferOutput, 0); // Placeholder for batchLength
        PrimitiveTypes.encodeInt32(bufferOutput, partitionLeaderEpoch);
        PrimitiveTypes.encodeInt8(bufferOutput, magic);

        // Placeholder for CRC
        int crcStartOffset = bufferStream.size();
        PrimitiveTypes.encodeInt32(bufferOutput, 0);

        // Write remaining fields
        PrimitiveTypes.encodeInt16(bufferOutput, attributes);
        PrimitiveTypes.encodeInt32(bufferOutput, lastOffsetDelta);
        PrimitiveTypes.encodeInt64(bufferOutput, baseTimestamp);
        PrimitiveTypes.encodeInt64(bufferOutput, maxTimestamp);
        PrimitiveTypes.encodeInt64(bufferOutput, producerId);
        PrimitiveTypes.encodeInt16(bufferOutput, producerEpoch);
        PrimitiveTypes.encodeInt32(bufferOutput, baseSequence);
        PrimitiveTypes.encodeArray(bufferOutput, records,(stream, record) -> record.encode(stream));

        // Calculate CRC
        byte[] buffer = bufferStream.toByteArray();
        CRC32C crc32c = new CRC32C();
        crc32c.update(buffer, crcStartOffset + 4, buffer.length - crcStartOffset - 4);
        long computedCrc = crc32c.getValue();

        // Write CRC back
        encodeUInt32At(buffer, crcStartOffset, computedCrc);

        // Write batchLength
        int batchLength = buffer.length - 12; // Exclude baseOffset (8 bytes) and batchLength (4 bytes)
        encodeUInt32At(buffer, 8, batchLength);

        // Write final buffer to the outputStream
        outputStream.write(buffer);
    }

    public static void encodeUInt32At(byte[] buffer, int offset, long value) {
        // Validate range for unsigned 32-bit integer
        if (value < 0 || value > 0xFFFFFFFFL) {
            throw new IllegalArgumentException("Value out of range for unsigned 32-bit integer: " + value);
        }

        // Write the value as 4 bytes
        buffer[offset] = (byte) ((value >>> 24) & 0xFF);
        buffer[offset + 1] = (byte) ((value >>> 16) & 0xFF);
        buffer[offset + 2] = (byte) ((value >>> 8) & 0xFF);
        buffer[offset + 3] = (byte) (value & 0xFF);
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
