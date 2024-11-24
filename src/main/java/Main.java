import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;

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
                System.out.println("Inside while");
                handleRequest(in, out);
                System.out.println("exit while");
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
        byteArrayStream.write(getErrorCode(requestApiVersion));
        //   .num_api_keys
        byteArrayStream.write(new byte[] {0, 2});

        byteArrayStream.write(requestApiKeyBytes);

        // Write the response
        byteArrayStream.write(new byte[] {0, 0}); // min v0 INT16
        // .max_version
        byteArrayStream.write(new byte[] {0, 4}); // max v4 INT16
        // .TAG_BUFFER
        byteArrayStream.write(new byte[] {0});
        // .throttle_time_ms

        byteArrayStream.write(new byte[] {0, 0, 0, 0});
        // .TAG_BUFFER

        byteArrayStream.write(new byte[] {0});

        byte[] bytes = byteArrayStream.toByteArray();

        // Write response size (4 bytes)
        outputStream.write(ByteBuffer.allocate(4).putInt(bytes.length).array());

        // Write response body
        outputStream.write(bytes);

        System.out.println("Response sent: " + bytes.length + " bytes");
    }

    private static byte[] getErrorCode(short requestApiVersion) {
        if (requestApiVersion < 0 || requestApiVersion > 4) {
            return ByteBuffer.allocate(2).putShort((short)35).array();
        } else {
            return new byte[] {0};
        }
    }
}
