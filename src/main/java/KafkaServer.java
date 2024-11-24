
import messages.AbstractRequest;
import messages.MessageHandler;
import java.io.*;
import java.net.*;
import java.util.concurrent.*;

public class KafkaServer {

    private final String host;
    private final int port;
    private final ExecutorService executorService;

    public KafkaServer(String host, int port) {
        this.host = host;
        this.port = port;
        this.executorService = Executors.newCachedThreadPool(); // Thread pool for handling client connections
    }

    public KafkaServer() {
        this("localhost", 9092); // Default host and port
    }

    // Start the server
    public void start() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket()) {
            serverSocket.setReuseAddress(true); // Enable address reuse
            serverSocket.bind(new InetSocketAddress(host, port));

            System.out.println("KafkaServer started on " + host + ":" + port);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                executorService.submit(() -> handleClient(clientSocket)); // Handle each client in a separate thread
            }
        } catch (IOException e) {
            System.err.println("Error starting KafkaServer: " + e.getMessage());
            throw e;
        } finally {
            executorService.shutdown();
        }
    }

    // Handle client connection
    private void handleClient(Socket clientSocket) {
        try (InputStream inputStream = clientSocket.getInputStream();
             OutputStream outputStream = clientSocket.getOutputStream()) {

            while (true) {
                try {
                    // Read and process the request
                    AbstractRequest requestBytes = MessageHandler.readRequest(new DataInputStream(inputStream));
                    byte[] responseBytes = MessageHandler.makeResponse(requestBytes).encode();

                    // Write the response back to the client
                    outputStream.write(responseBytes);
                    outputStream.flush();
                } catch (EOFException e) {
                    // Client disconnected
                    break;
                } catch (Exception e) {
                    System.err.println("Error handling client: " + e.getMessage());
                    break;
                }
            }
        } catch (IOException e) {
            System.err.println("I/O error with client: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.err.println("Error closing client socket: " + e.getMessage());
            }
        }
    }
}
