package Kafka;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class Constants {

    public enum ApiKey {
        FETCH((short) 1),
        API_VERSIONS((short) 18),
        DESCRIBE_TOPIC_PARTITIONS((short) 75);

        private final short value;

        ApiKey(short value) {
            this.value = value;
        }

        public short getValue() {
            return value;
        }

        public static ApiKey decode(DataInputStream inputStream) throws IOException {
            short shortValue = inputStream.readShort(); // Decode as 16-bit integer
            for (ApiKey apiKey : ApiKey.values()) {
                if (apiKey.getValue() == shortValue) {
                    return apiKey;
                }
            }
            throw new IllegalArgumentException("Unknown ApiKey: " + shortValue);
        }

        public void encode(DataOutputStream outputStream) throws IOException {
            outputStream.writeShort(value); // Encode as 16-bit integer
        }
    }


    public enum ErrorCode {
        NONE(0),
        UNKNOWN_TOPIC_OR_PARTITION(3),
        UNSUPPORTED_VERSION(35),
        UNKNOWN_TOPIC_ID(100);

        private final int value;

        ErrorCode(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static ErrorCode decode(DataInputStream inputStream) throws IOException {
            int intValue = inputStream.readShort(); // Decode as a 16-bit integer
            for (ErrorCode errorCode : ErrorCode.values()) {
                if (errorCode.getValue() == intValue) {
                    return errorCode;
                }
            }
            throw new IllegalArgumentException("Unknown ErrorCode: " + intValue);
        }

        public void encode(DataOutputStream outputStream) throws IOException {
            outputStream.writeShort(value); // Encode as a 16-bit integer
        }
    }
}
