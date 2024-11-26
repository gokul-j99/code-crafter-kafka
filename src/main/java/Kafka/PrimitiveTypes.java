package Kafka;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class PrimitiveTypes {

    // Decode Methods
    public static boolean decodeBoolean(DataInputStream inputStream) throws IOException {
        return inputStream.readBoolean();
    }


    private static byte[] encodeVarint(int value) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        while ((value & 0xFFFFFF80) != 0L) {
            outputStream.write((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        outputStream.write(value & 0x7F);
        return outputStream.toByteArray();
    }

    public static byte decodeInt8(DataInputStream inputStream) throws IOException {
        return inputStream.readByte();
    }

    public static short decodeInt16(DataInputStream inputStream) throws IOException {
        return inputStream.readShort();
    }

    public static int decodeInt32(DataInputStream inputStream) throws IOException {
        return inputStream.readInt();
    }

    public static long decodeInt64(DataInputStream inputStream) throws IOException {
        return inputStream.readLong();
    }

    public static long decodeUInt32(DataInputStream inputStream) throws IOException {
        int data = inputStream.readInt();
        System.out.println("decode uint");
        System.out.println((data));
        return Integer.toUnsignedLong(data);
    }

    public static int decodeVarint(DataInputStream inputStream) throws IOException {
        int value = 0, shift = 0;
        int base = 128;
        int maxVarintBytes = 5; // Varint for int32 should fit in 5 bytes
        int bytesRead = 0;

        while (true) {
            // Check for sufficient bytes
            if (inputStream.available() <= 0) {
                return 0;
            }

            int b = inputStream.readUnsignedByte();
            bytesRead++;

            value |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) break; // Stop if continuation bit is not set
            shift += 7;

            // Check for varint overflow
            if (bytesRead > maxVarintBytes) {
                throw new IOException("Varint too long: exceeds 5 bytes");
            }
        }
        return value;
    }


    public static long decodeVarlong(DataInputStream inputStream) throws IOException {
        return decodeVarint(inputStream);
    }

    public static UUID decodeUUID(DataInputStream inputStream) throws IOException {
        long mostSigBits = inputStream.readLong();
        long leastSigBits = inputStream.readLong();
        return new UUID(mostSigBits, leastSigBits);
    }

    public static double decodeFloat64(DataInputStream inputStream) throws IOException {
        return Double.longBitsToDouble(inputStream.readLong());
    }

    public static String decodeString(DataInputStream inputStream) throws IOException {
        short length = decodeInt16(inputStream);
        if (length < 0) return null;
        byte[] bytes = new byte[length];
        inputStream.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static String decodeCompactString(DataInputStream inputStream) throws IOException {
        int length = decodeVarint(inputStream) - 1; // Compact encoding adds 1 to length
        if (length < 0) {
            return null; // Handle nullable compact string
        }
        if (length == 0) {
            return ""; // Empty string
        }
        byte[] stringBytes = new byte[length];
        inputStream.readFully(stringBytes);
        return new String(stringBytes, StandardCharsets.UTF_8);
    }



    public static byte[] decodeBytes(DataInputStream inputStream) throws IOException {
        int length = decodeInt32(inputStream);
        if (length < 0) return null;
        byte[] bytes = new byte[length];
        inputStream.readFully(bytes);
        return bytes;
    }

    public static <T> List<T> decodeArray(DataInputStream inputStream, DecoderFunction<T> decoder) throws IOException {
        int length = decodeInt32(inputStream);
        if (length < 0) return null;

        List<T> list = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            list.add(decoder.decode(inputStream));
        }
        return list;
    }


    public static <T> List<T> decodeCompactArray(DataInputStream inputStream, DecoderFunction<T> decoder) throws IOException {
        int length = decodeVarint(inputStream) - 1; // Compact array length
        if (length < 0) {
            return new ArrayList<>(); // Return empty list for null or empty array
        }

        List<T> list = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            if (inputStream.available() <= 0) {
                throw new EOFException("Unexpected EOF while decoding compact array");
            }
            list.add(decoder.decode(inputStream)); // Decode each element
        }
        return list;
    }





    public static void decodeTaggedFields(DataInputStream inputStream) throws IOException {
        int numFields = decodeVarint(inputStream);
        for (int i = 0; i < numFields; i++) {
            int tag = decodeVarint(inputStream);
            int size = decodeVarint(inputStream);
            inputStream.skipBytes(size); // Skip the field content
        }
    }

    // Encode Methods
    public static void encodeBoolean(DataOutputStream outputStream, boolean value) throws IOException {
        outputStream.writeBoolean(value);
    }

    public static void encodeInt8(DataOutputStream outputStream, byte value) throws IOException {
        outputStream.writeByte(value);
    }

    public static void encodeInt16(DataOutputStream outputStream, short value) throws IOException {
        outputStream.writeShort(value);
    }

    public static void encodeInt32(DataOutputStream outputStream, int value) throws IOException {
        outputStream.writeInt(value);
    }

    public static void encodeInt64(DataOutputStream outputStream, long value) throws IOException {
        outputStream.writeLong(value);
    }

    public static void encodeVarint(DataOutputStream outputStream, int value) throws IOException {
        int base = 128;
        while (true) {
            int temp = value & 0x7F;
            value >>>= 7;
            if (value != 0) {
                temp |= 0x80;
            }
            outputStream.writeByte(temp);
            if (value == 0) break;
        }
    }

    public static void encodeVarlong(DataOutputStream outputStream, long value) throws IOException {
        encodeVarint(outputStream, (int) value);
    }

    public static void encodeUUID(DataOutputStream outputStream, UUID uuid) throws IOException {
        outputStream.writeLong(uuid.getMostSignificantBits());
        outputStream.writeLong(uuid.getLeastSignificantBits());
    }

    public static void encodeFloat64(DataOutputStream outputStream, double value) throws IOException {
        outputStream.writeLong(Double.doubleToRawLongBits(value));
    }

    public static void encodeString(DataOutputStream outputStream, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        encodeInt16(outputStream, (short) bytes.length);
        outputStream.write(bytes);
    }

    public static void encodeCompactString(DataOutputStream outputStream, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        encodeVarint(outputStream, bytes.length + 1);
        outputStream.write(bytes);
    }

    public static void encodeBytes(DataOutputStream outputStream, byte[] value) throws IOException {
        if (value == null) {
            encodeInt32(outputStream, -1);
        } else {
            encodeInt32(outputStream, value.length);
            outputStream.write(value);
        }
    }

    public static <T> void encodeArray(DataOutputStream outputStream, List<T> array, EncoderFunction<T> encoder) throws IOException {
        if (array == null) {
            encodeInt32(outputStream, -1);
            return;
        }

        encodeInt32(outputStream, array.size());
        for (T item : array) {
            encoder.encode(outputStream, item);
        }
    }


    public static <T> void encodeCompactArray(DataOutputStream outputStream, List<T> array, EncoderFunction<T> encoder) throws IOException {
        if (array == null) {
            encodeVarint(outputStream, 0);
            return;
        }

        encodeVarint(outputStream, array.size() + 1);
        for (T item : array) {
            encoder.encode(outputStream, item);
        }
    }

    public static void encodeCompactBytes(DataOutputStream outputStream, byte[] data) throws IOException {
        if (data == null) {
            encodeVarint(outputStream, 0); // Null data, write 0
        } else {
            encodeVarint(outputStream, data.length + 1); // Length with compact encoding
            outputStream.write(data); // Write the data
        }
    }


    public static void encodeUInt32(DataOutputStream outputStream, long value) throws IOException {
        if (value < 0 || value > 0xFFFFFFFFL) {
            System.out.println(value);
            System.out.println("Value out of range for unsigned 32-bit ");
            throw new IllegalArgumentException("Value out of range for unsigned 32-bit integer: " + value);
        }

        System.out.println(value);
        System.out.println(" long uint");
        outputStream.writeInt((int) value); // Cast to int, as Java uses signed 32-bit integers
    }




    public static void encodeTaggedFields(DataOutputStream outputStream) throws IOException {
        outputStream.writeByte(0);
    }

    public static byte[] decodeCompactBytes(InputStream inputStream) throws IOException {
        int length = decodeVarint((DataInputStream) inputStream) - 1; // Decode the length and adjust
        byte[] data = new byte[length];
        int bytesRead = inputStream.read(data);
        if (bytesRead != length) {
            throw new EOFException("Unexpected end of stream while reading compact bytes.");
        }
        return data;
    }

    // Encodes bytes into compact bytes format
    public static byte[] encodeCompactBytes(byte[] data) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(encodeVarint(data.length + 1)); // Encode length + 1
        outputStream.write(data); // Write the data
        return outputStream.toByteArray();
    }

    public static void encodeCompactNullableString(DataOutputStream outputStream, String value) throws IOException {
        if (value == null) {
            encodeVarint(outputStream, 0); // Null string, write 0
        } else {
            encodeVarint(outputStream, value.length() + 1); // Length + 1
            outputStream.write(value.getBytes("UTF-8")); // Encode string as UTF-8
        }
    }

    // Functional Interfaces
    @FunctionalInterface
    public interface DecoderFunction<T> {
        T decode(DataInputStream inputStream) throws IOException;
    }


    @FunctionalInterface
    public interface EncoderFunction<T> {
        void encode(DataOutputStream outputStream, T value) throws IOException;
    }

}
