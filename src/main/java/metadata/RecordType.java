package metadata;

public enum RecordType {
    TOPIC(2),
    PARTITION(3),
    FEATURE_LEVEL(12);

    private final int value;

    // Constructor
    RecordType(int value) {
        this.value = value;
    }

    // Getter for value
    public int getValue() {
        return value;
    }

    // Decode method
    public static RecordType fromValue(int value) {
        for (RecordType type : RecordType.values()) {
            if (type.value == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown RecordType value: " + value);
    }
}
