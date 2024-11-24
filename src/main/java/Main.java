import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    private static final int port = 9092;
    private static final int THREAD_POOL_SIZE = 4;
    public static void main(String[] args) {
        System.out.println("Kafka server started");

        ExecutorService executorService =
                Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        try (ServerSocket serverSocket = new ServerSocket(port)) {
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
        }
        finally {
            executorService.shutdown();
        }
    }

    private static void handleClient(Socket clientSocket) {
        try (clientSocket) {
            while (true) {
                var request = request(clientSocket.getInputStream());
                if (request == null) {
                    break;
                }
                var response = process(request);
                respond(response, clientSocket.getOutputStream());
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
    private static ByteBuffer request(InputStream inputStream)
            throws IOException {
        var length = ByteBuffer.wrap(inputStream.readNBytes(4)).getInt();
        var payload = inputStream.readNBytes(length);
        return ByteBuffer.allocate(length).put(payload).rewind();
    }
    private static ByteBuffer process(ByteBuffer request) {
        var apiKey = request.getShort();     // request_api_key
        var apiVersion = request.getShort(); // request_api_version
        var correlationId = request.getInt();
        var errorCode = switch (apiKey) {
            case 18 -> switch (apiVersion) {
                case 0, 1, 2, 3, 4 -> 0;
                default -> 35;
            };
            default -> -1;
        };
        return ByteBuffer.allocate(23)
                .putInt(19)
                .putInt(correlationId)
                .putShort((short) errorCode)
                .put((byte) 2) // response version
                .putShort((short) 18) // api key
                .putShort((short) 4) // api min version
                .putShort((short) 4) // api max version
                .putInt(0) // throttle time
                .putShort((short) 0); // tagged fields
    }
    private static void respond(ByteBuffer response, OutputStream outputStream) throws IOException {
        outputStream.write(response.array());
    }
}