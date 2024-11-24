import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        System.out.println("Kafka server started");
        int port = 9092;

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true); // Allow port reuse after restart

            while (true) {
                // Accept client connection
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connected!");

                // Handle the client in a separate thread
                new Thread(() -> handleClient(clientSocket)).start();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private static void handleClient(Socket clientSocket) {
        try (
                DataInputStream in = new DataInputStream(clientSocket.getInputStream());
                OutputStream out = clientSocket.getOutputStream()
        ) {
            // Handle multiple requests from this client
            while (true) {
                handleRequest(in, out);
            }
        } catch (IOException e) {
            System.out.println("Client disconnected or error occurred: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.out.println("Error closing client socket: " + e.getMessage());
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
        byteArrayStream.write(getErrorCode(requestApiVersion));
        byteArrayStream.write(new byte[]{0, 1}); // Number of API keys (1 entry)

        // API Key Entry
        byteArrayStream.write(requestApiKeyBytes); // API key
        byteArrayStream.write(new byte[]{0, 0}); // Min version
        byteArrayStream.write(new byte[]{0, 4}); // Max version
        byteArrayStream.write(new byte[]{0, 0, 0, 0}); // Throttle time
        byteArrayStream.write(new byte[]{0}); // TAG_BUFFER

        byte[] bytes = byteArrayStream.toByteArray();

        // Write response size (4 bytes)
        outputStream.write(ByteBuffer.allocate(4).putInt(bytes.length).array());

        // Write response body
        outputStream.write(bytes);

        System.out.println("Response sent: " + bytes.length + " bytes");
        System.out.println("Response Content: " + Arrays.toString(bytes));
    }

    private static byte[] getErrorCode(short requestApiVersion) {
        if (requestApiVersion < 0 || requestApiVersion > 4) {
            return ByteBuffer.allocate(2).putShort((short) 35).array();
        } else {
            return new byte[]{0, 0};
        }
    }
}
