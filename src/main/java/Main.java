import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    public static void main(String[] args) {
        System.err.println("Logs from your program will appear here!");

        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 9092;

        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);

            // Wait for a connection from the client.
            clientSocket = serverSocket.accept();
            System.err.println("Client connected!");

            // Read the request
            byte[] buffer = new byte[256];
            clientSocket.getInputStream().read(buffer);

            // Parse the request_api_version (bytes 6 and 7)
            int requestApiVersion = ((buffer[6] & 0xFF) << 8) | (buffer[7] & 0xFF);

            // Parse the correlation_id (bytes 8 to 11)
            int correlationId = ((buffer[8] & 0xFF) << 24) | ((buffer[9] & 0xFF) << 16)
                    | ((buffer[10] & 0xFF) << 8) | (buffer[11] & 0xFF);

            System.err.println("Parsed request_api_version: " + requestApiVersion);
            System.err.println("Parsed correlation_id: " + correlationId);

            byte[] responseBody;

            // Handle API version validation
            if (requestApiVersion > 4) {
                // Unsupported version: return error code 35
                responseBody = createErrorResponseBody(35);
            } else {
                // Valid version: construct the normal response
                responseBody = createApiVersionsResponseBody();
            }

            int messageSize = 4 + responseBody.length; // 4 bytes for correlation_id + body length
            byte[] response = new byte[4 + 4 + responseBody.length]; // message_size + correlation_id + body
            int offset = 0;

            // message_size (4 bytes)
            response[offset++] = (byte) ((messageSize >> 24) & 0xFF);
            response[offset++] = (byte) ((messageSize >> 16) & 0xFF);
            response[offset++] = (byte) ((messageSize >> 8) & 0xFF);
            response[offset++] = (byte) (messageSize & 0xFF);

            // correlation_id (4 bytes)
            response[offset++] = (byte) ((correlationId >> 24) & 0xFF);
            response[offset++] = (byte) ((correlationId >> 16) & 0xFF);
            response[offset++] = (byte) ((correlationId >> 8) & 0xFF);
            response[offset++] = (byte) (correlationId & 0xFF);

            // Add the response body
            System.arraycopy(responseBody, 0, response, offset, responseBody.length);

            // Send the response
            OutputStream outputStream = clientSocket.getOutputStream();
            outputStream.write(response);
            outputStream.flush();
            System.err.println("Response sent to the client!");

        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
                if (serverSocket != null) {
                    serverSocket.close();
                }
            } catch (IOException e) {
                System.err.println("IOException during cleanup: " + e.getMessage());
            }
        }
    }

    private static byte[] createErrorResponseBody(int errorCode) {
        byte[] responseBody = new byte[2]; // error_code only
        responseBody[0] = (byte) ((errorCode >> 8) & 0xFF);
        responseBody[1] = (byte) (errorCode & 0xFF);
        return responseBody;
    }

    private static byte[] createApiVersionsResponseBody() {
        int apiKey = 18; // API_VERSIONS
        int minVersion = 0;
        int maxVersion = 4;
        int errorCode = 0;

        byte[] responseBody = new byte[2 + 1 + 6]; // error_code + api_key_count + api_key entry
        int offset = 0;

        // error_code (2 bytes)
        responseBody[offset++] = (byte) ((errorCode >> 8) & 0xFF);
        responseBody[offset++] = (byte) (errorCode & 0xFF);

        // api_key_count (1 byte, 1 key)
        responseBody[offset++] = 0x01;

        // API key entry
        // api_key (2 bytes)
        responseBody[offset++] = (byte) ((apiKey >> 8) & 0xFF);
        responseBody[offset++] = (byte) (apiKey & 0xFF);

        // min_version (2 bytes)
        responseBody[offset++] = (byte) ((minVersion >> 8) & 0xFF);
        responseBody[offset++] = (byte) (minVersion & 0xFF);

        // max_version (2 bytes)
        responseBody[offset++] = (byte) ((maxVersion >> 8) & 0xFF);
        responseBody[offset++] = (byte) (maxVersion & 0xFF);

        return responseBody;
    }
}
