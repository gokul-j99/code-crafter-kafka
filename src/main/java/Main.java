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

//                    var buffer = ByteBuffer.allocate(2);
//                    buffer.putShort(0, (short)0);
//                    buffer.putShort(1, (short)42);
//                    respond(buffer, clientSocket.getOutputStream());
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

    private static ByteBuffer process(ByteBuffer request) throws IOException {
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

    private static ByteBuffer constructResponse(int correlationId, int errorCode) throws IOException {
        // Use ByteArrayOutputStream to build the response
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        // Response Header
        baos.write(ByteBuffer.allocate(4).putInt(correlationId).array()); // Correlation ID
        baos.write(new byte[]{0, 0}); // Error Code (No Error)

        // Number of API key entries (2: APIVersions + DescribeTopicPartitions)
        baos.write(2);

        // APIVersions API Key Entry
        baos.write(new byte[]{0, 18}); // API key: APIVersions
        baos.write(new byte[]{0, 0}); // Min version: 0
        baos.write(new byte[]{0, 4}); // Max version: 4
        baos.write(0); // TAG_BUFFER: empty

        // DescribeTopicPartitions API Key Entry
        baos.write(new byte[]{0, 75}); // API key: DescribeTopicPartitions
        baos.write(new byte[]{0, 0}); // Min version: 0
        baos.write(new byte[]{0, 0}); // Max version: 0
        baos.write(0); // TAG_BUFFER: empty

        // Throttle Time
        baos.write(new byte[]{0, 0, 0, 0}); // Throttle time: 0 (no throttling)

        // Final TAG_BUFFER
        baos.write(0); // TAG_BUFFER: empty

        // Create a ByteBuffer for the complete response with size prefix
        byte[] responseBytes = baos.toByteArray();
        ByteBuffer responseBuffer = ByteBuffer.allocate(4 + responseBytes.length);
        responseBuffer.putInt(responseBytes.length); // Message size
        responseBuffer.put(responseBytes); // Response content
        responseBuffer.rewind(); // Reset buffer position for writing

        return responseBuffer;
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
