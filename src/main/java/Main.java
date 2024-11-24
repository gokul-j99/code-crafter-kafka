import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    private static final int PORT = 9092;
    private static final int THREAD_POOL_SIZE = 4;

    public static void main(String[] args) {
        System.out.println("Kafka server started");

        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            serverSocket.setReuseAddress(true); // Allow port reuse after restart

            while (true) {
                // Accept client connection
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connected!");

                // Handle the client in a separate thread
                executorService.submit(() -> handleClient(clientSocket));
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            executorService.shutdown();
        }
    }

    private static void handleClient(Socket clientSocket) {
        try (clientSocket) {
            while (true) {
                var request = request(clientSocket.getInputStream());
                if (request == null) {

                    var buffer = ByteBuffer.allocate(2);
                    buffer.putShort(0, (short)0);
                    buffer.putShort(1, (short)42);
                    respond(buffer, clientSocket.getOutputStream());
                    break;
                }
                var response = process(request);
                respond(response, clientSocket.getOutputStream());
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private static ByteBuffer request(InputStream inputStream) throws IOException {
        var length = ByteBuffer.wrap(inputStream.readNBytes(4)).getInt();

        var payload = inputStream.readNBytes(length);
        return ByteBuffer.allocate(length).put(payload).rewind();
    }

    private static ByteBuffer process(ByteBuffer request) {
        var apiKey = request.getShort();     // request_api_key
        var apiVersion = request.getShort(); // request_api_version
        var correlationId = request.getInt();

        int errorCode;
        switch (apiKey) {
            case 18: // APIVersions
                if (apiVersion >= 0 && apiVersion <= 4) {
                    errorCode = 0; // No Error
                } else {
                    errorCode = 35; // Unsupported version
                }
                break;
            default:
                errorCode = -1; // Unknown API key
                break;
        }

        // Construct the response
        return constructResponse(correlationId, errorCode);
    }

    private static ByteBuffer constructResponse(int correlationId, int errorCode) {
        // Prepare API key entries for the response
        var apiversionsEntry = createApiKeyEntry((short) 18, (short) 0, (short) 4); // APIVersions
        var describeTopicPartitionsEntry = createApiKeyEntry((short) 75, (short) 0, (short) 0); // DescribeTopicPartitions

        // Calculate total response size
        int responseSize = 4 + // Correlation ID
                2 + // Error Code
                1 + // Number of API key entries
                apiversionsEntry.capacity() +
                describeTopicPartitionsEntry.capacity();

        var buffer = ByteBuffer.allocate(4 + responseSize); // 4 bytes for the size
        buffer.putInt(responseSize); // Message size
        buffer.putInt(correlationId); // Correlation ID
        buffer.putShort((short) errorCode); // Error code
        buffer.put((byte) 3); // Number of API key entries
        buffer.put(apiversionsEntry); // APIVersions entry
        buffer.put(describeTopicPartitionsEntry); // DescribeTopicPartitions entry

        return buffer.rewind();
    }

    private static ByteBuffer createApiKeyEntry(short apiKey, short minVersion, short maxVersion) {
        return ByteBuffer.allocate(6)
                .putShort(apiKey) // API key
                .putShort(minVersion) // Min version
                .putShort(maxVersion)
                .putShort((short)0); // Max version.

    }

    private static void respond(ByteBuffer response, OutputStream outputStream) throws IOException {
        outputStream.write(response.array());
    }
}
