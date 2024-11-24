import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class Main {
    private static final int PORT = 9092;

    public static void main(String[] args) {
        System.out.println("Kafka server started");

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            serverSocket.setReuseAddress(true); // Allow reusing the port after program restarts

            while (true) {
                try (Socket clientSocket = serverSocket.accept()) {
                    System.out.println("Client connected!");
                    DataInputStream inputStream = new DataInputStream(clientSocket.getInputStream());
                    OutputStream outputStream = clientSocket.getOutputStream();

                    // Process multiple requests sequentially from the same client
                    while (true) {
                        try {
                            handleRequest(inputStream, outputStream);
                        } catch (IOException e) {
                            System.err.println("Error processing request: " + e.getMessage());
                            break; // Exit the loop if an error occurs
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Error handling client: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Error starting server: " + e.getMessage());
        }
    }

    private static void handleRequest(DataInputStream inputStream, OutputStream outputStream) throws IOException {
        // Read request message size (4 bytes)
        int incomingMessageSize = inputStream.readInt();

        // Read API key (2 bytes)
        byte[] requestApiKeyBytes = inputStream.readNBytes(2);

        // Read API version (2 bytes)
        short requestApiVersion = inputStream.readShort();

        // Read correlation ID (4 bytes)
        byte[] correlationIdBytes = inputStream.readNBytes(4);

        // Read remaining bytes (remaining payload size)
        byte[] remainingBytes = new byte[incomingMessageSize - 8];
        inputStream.readFully(remainingBytes);

        // Build the response
        ByteArrayOutputStream responseStream = new ByteArrayOutputStream();

        // Write Response Header: Correlation ID
        responseStream.write(correlationIdBytes);

        // Write Response Body
        if (requestApiVersion < 0 || requestApiVersion > 4) {
            // Write error code for unsupported version
            responseStream.write(ByteBuffer.allocate(2).putShort((short) 35).array()); // Error code 35
        } else {
            // Write success response
            responseStream.write(new byte[]{0, 0}); // Error code 0 (No Error)
            responseStream.write(ByteBuffer.allocate(4).putInt(1).array()); // Number of API keys (1 key)

            // API Key Entry
            responseStream.write(new byte[]{0, 18}); // API key (18 for ApiVersions)
            responseStream.write(new byte[]{0, 0}); // Min version (0)
            responseStream.write(new byte[]{0, 4}); // Max version (4)
            responseStream.write(ByteBuffer.allocate(4).putInt(0).array()); // Throttle time (0)

            // TAG_BUFFER: Encode as an empty compact array
            responseStream.write(0x00);
        }

        // Write the response
        byte[] responseBytes = responseStream.toByteArray();

        // Write message size (4 bytes)
        outputStream.write(ByteBuffer.allocate(4).putInt(responseBytes.length).array());

        // Write the response itself
        outputStream.write(responseBytes);

        System.out.println("Response sent. Size: " + responseBytes.length);
        System.out.println("Response Content: " + Arrays.toString(responseBytes));
    }
}
