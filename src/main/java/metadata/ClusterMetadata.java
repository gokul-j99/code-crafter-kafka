package metadata;

import Kafka.PrimitiveTypes;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterMetadata {

    // Singleton instance
    private static ClusterMetadata instance;

    // Topic metadata mappings
    private final Map<String, UUID> topicIdLookup = new ConcurrentHashMap<>();
    private final Map<UUID, String> topicNameLookup = new ConcurrentHashMap<>();
    private final Map<UUID, List<Integer>> partitionIndicesLookup = new ConcurrentHashMap<>();

    // Private constructor for singleton
    private ClusterMetadata() {
        try {
            for (RecordBatch recordBatch : readRecordBatches("__cluster_metadata", 0)) {
                for (Record record : recordBatch.getRecords()) {
                    addRecord(record);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize ClusterMetadata", e);
        }
    }

    // Get singleton instance
    public static synchronized ClusterMetadata getInstance() {
        if (instance == null) {
            instance = new ClusterMetadata();
        }
        return instance;
    }

    // Get topic ID by name
    public UUID getTopicId(String topicName) {
        return topicIdLookup.get(topicName);
    }

    // Get topic name by ID
    public String getTopicName(UUID topicId) {
        return topicNameLookup.get(topicId);
    }

    // Get partition indices by topic ID
    public List<Integer> getPartitionIndices(UUID topicId) {
        return partitionIndicesLookup.getOrDefault(topicId, Collections.emptyList());
    }

    // Add record to metadata
    private void addRecord(Record record) throws IOException {
        ByteArrayInputStream binaryStream = new ByteArrayInputStream(record.getValue());
        DataInputStream inputStream = new DataInputStream(binaryStream);

        PrimitiveTypes.decodeInt8(inputStream); // Skip initial byte
        int recordTypeValue = PrimitiveTypes.decodeInt8(inputStream);
        RecordType recordType = RecordType.fromValue(recordTypeValue);

        switch (recordType) {
            case TOPIC -> {
                PrimitiveTypes.decodeInt8(inputStream); // Skip extra byte
                String topicName = PrimitiveTypes.decodeCompactString(inputStream);
                UUID topicId = PrimitiveTypes.decodeUUID(inputStream);

                topicIdLookup.put(topicName, topicId);
                topicNameLookup.put(topicId, topicName);
            }
            case PARTITION -> {
                PrimitiveTypes.decodeInt8(inputStream); // Skip extra byte
                int partitionIndex = PrimitiveTypes.decodeInt32(inputStream);
                UUID topicId = PrimitiveTypes.decodeUUID(inputStream);

                partitionIndicesLookup.computeIfAbsent(topicId, k -> new ArrayList<>()).add(partitionIndex);
            }
        }
    }

    // Read record batches from log file
    public static Iterable<RecordBatch> readRecordBatches(String topicName, int partitionIndex) throws IOException {
        String filepath = String.format("/tmp/kraft-combined-logs/%s-%d/00000000000000000000.log", topicName, partitionIndex);
        List<RecordBatch> recordBatches = new ArrayList<>();

        try (DataInputStream inputStream = new DataInputStream(Files.newInputStream(Paths.get(filepath)))) {
            while (inputStream.available() > 0) {
                recordBatches.add(RecordBatch.decode(inputStream));
            }
        }

        return recordBatches;
    }

}
