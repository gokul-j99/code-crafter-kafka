import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class Main {
    private static final int PORT = 9092;

    public static void main(String[] args) {
        System.err.println("Starting server...");
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            serverSocket.setReuseAddress(true); // Prevent "Address already in use" errors

            while (true) {
                try (Socket clientSocket = serverSocket.accept()) {
                    System.err.println("Client connected!");
                    handleClient(clientSocket);
                } catch (IOException e) {
                    System.err.println("Error handling client: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Error starting server: " + e.getMessage());
        }
    }

    private static void handleClient(Socket clientSocket) throws IOException {
        InputStream in = clientSocket.getInputStream();
        OutputStream out = clientSocket.getOutputStream();

        while (true) { // Loop to handle multiple requests
            try {
                if (in.available() < 4) {
                    Thread.sleep(10); // Avoid busy waiting if no data is available
                    continue;
                }

                // Parse the request
                RequestData requestData = parseRequest(in);

                // Prepare the response
                ByteArrayOutputStream responseStream = prepareResponse(requestData);

                // Send the response
                sendResponse(out, responseStream);
            } catch (IOException | InterruptedException e) {
                System.err.println("Client disconnected or error occurred: " + e.getMessage());
                break;
            }
        }
    }

    private static RequestData parseRequest(InputStream in) throws IOException {
        // Read size (4 bytes, not used)
        in.readNBytes(4);

        // Read API key (2 bytes, not used)
        in.readNBytes(2);

        // Read API version (2 bytes, used later)
        byte[] apiVersionBytes = in.readNBytes(2);
        short apiVersion = ByteBuffer.wrap(apiVersionBytes).getShort();
        System.err.println("Parsed API Version: " + apiVersion);

        // Read correlation ID (4 bytes)
        byte[] correlationId = in.readNBytes(4);
        System.err.println("Parsed Correlation ID: " + Arrays.toString(correlationId));

        return new RequestData(apiVersion, correlationId);
    }

    private static ByteArrayOutputStream prepareResponse(RequestData requestData) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        // Add correlation ID
        bos.write(requestData.correlationId);

        if (requestData.apiVersion < 0 || requestData.apiVersion > 4) {
            // Unsupported API version
            bos.write(new byte[]{0, 35}); // Error code 35 (UNSUPPORTED_VERSION)
        } else {
            // Supported API version
            bos.write(new byte[]{0, 0});        // Error code 0 (No Error)
            bos.write(new byte[]{0, 0, 0, 1}); // Number of API keys (1 key)

            // Write API key entry
            bos.write(new byte[]{0, 18});       // API key (18 for ApiVersions)
            bos.write(new byte[]{0, 0});       // Min version
            bos.write(new byte[]{0, 4});       // Max version
            bos.write(new byte[]{0, 0, 0, 0}); // Throttle time

            // Correctly encode TAG_BUFFER
            bos.write(0x00); // TAG_BUFFER: empty compact array
        }

        return bos;
    }

    private static void sendResponse(OutputStream out, ByteArrayOutputStream bos) throws IOException {
        // Calculate and write the size
        byte[] sizeBytes = ByteBuffer.allocate(4).putInt(bos.size()).array();
        out.write(sizeBytes);

        // Write the response body
        byte[] response = bos.toByteArray();
        out.write(response);

        // Flush the output stream
        out.flush();

        System.err.println("Response sent. Size: " + bos.size());
        System.err.println("Response Content: " + Arrays.toString(response));
    }

    // Helper class to hold parsed request data
    private static class RequestData {
        short apiVersion;
        byte[] correlationId;

        public RequestData(short apiVersion, byte[] correlationId) {
            this.apiVersion = apiVersion;
            this.correlationId = correlationId;
        }
    }
}
