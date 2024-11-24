import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {
    public static void main(String[] args) {
        System.out.println("Kafka server started");
        ServerSocket serverSocket;
        Socket clientSocket = null;
        int port = 9092;

        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);

            // Wait for connection from the client
            clientSocket = serverSocket.accept();

            // Set up input and output streams
            DataInputStream in = new DataInputStream(clientSocket.getInputStream());
            OutputStream out = clientSocket.getOutputStream();

            // Handle requests in a loop
            while (true) {
                handleRequest(in, out);
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }

    private static void handleRequest(DataInputStream inputStream, OutputStream outputStream) throws IOException {
        // Read request size (4 bytes)
        int incomingMessageSize = inputStream.readInt();

        // Read API key (2 bytes)
        byte[] requestApiKeyBytes = inputStream.readNBytes(2);

        // Read API version (2 bytes)
        short requestApiVersion = inputStream.readShort();

        // Read correlation ID (4 bytes)
        byte[] correlationIdBytes = inputStream.readNBytes(4);

        // Read remaining payload
        byte[] remainingBytes = new byte[incomingMessageSize - 8];
        inputStream.readFully(remainingBytes);

        // Build the response
        ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();

        // Response Header: Correlation ID
        byteArrayStream.write(correlationIdBytes);

        // Response Body
        if (requestApiVersion < 0 || requestApiVersion > 4) {
            // Unsupported version error code (35)
            byteArrayStream.write(ByteBuffer.allocate(2).putShort((short) 35).array());
        } else {
            // Success response
            byteArrayStream.write(new byte[]{0, 0}); // Error code 0 (No Error)
            byteArrayStream.write(new byte[]{0, 2}); // Number of API keys (2 bytes)

            // API Key Entry
            byteArrayStream.write(requestApiKeyBytes); // API key
            byteArrayStream.write(new byte[]{0, 0}); // Min version (0)
            byteArrayStream.write(new byte[]{0, 4}); // Max version (4)

            // TAG_BUFFER and Throttle Time
            byteArrayStream.write(new byte[]{0}); // Empty TAG_BUFFER
            byteArrayStream.write(new byte[]{0, 0, 0, 0}); // Throttle time
            byteArrayStream.write(new byte[]{0}); // Empty TAG_BUFFER
        }

        // Write the response
        byte[] responseBytes = byteArrayStream.toByteArray();

        // Write response size (4 bytes)
        outputStream.write(ByteBuffer.allocate(4).putInt(responseBytes.length).array());

        // Write response body
        outputStream.write(responseBytes);

        System.out.println("Response sent: " + responseBytes.length + " bytes");
    }
}
