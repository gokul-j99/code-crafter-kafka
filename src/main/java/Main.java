import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.err.println("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage

     ServerSocket serverSocket = null;
     Socket clientSocket = null;
     int port = 9092;
     try {
       serverSocket = new ServerSocket(port);
       // Since the tester restarts your program quite often, setting SO_REUSEADDR
       // ensures that we don't run into 'Address already in use' errors
       serverSocket.setReuseAddress(true);
       // Wait for connection from client.
       clientSocket = serverSocket.accept();
       System.out.println("Client connected!");

         InputStream inputStream = clientSocket.getInputStream();
         byte[] buffer = new byte[256]; // Adjust size as necessary for your use case
         int bytesRead = inputStream.read(buffer);

         if (bytesRead > 0) {
             // Parse the correlation_id from the request
             int correlationId = ((buffer[8] & 0xFF) << 24) | ((buffer[9] & 0xFF) << 16)
                     | ((buffer[10] & 0xFF) << 8) | (buffer[11] & 0xFF);

             int requestApiVersion = ((buffer[6] & 0xFF) << 8) | (buffer[7] & 0xFF);


             System.err.println("Parsed correlation_id: " + correlationId);

             System.err.println("Parsed request_api_version: " + requestApiVersion);


             byte[] response = new byte[10];

             // message_size (4 bytes)
             response[0] = 0x00;
             response[1] = 0x00;
             response[2] = 0x00;
             response[3] = 0x00;

             // correlation_id (4 bytes)
             response[4] = (byte) ((correlationId >> 24) & 0xFF);
             response[5] = (byte) ((correlationId >> 16) & 0xFF);
             response[6] = (byte) ((correlationId >> 8) & 0xFF);
             response[7] = (byte) (correlationId & 0xFF);

             if (requestApiVersion > 4) {
                 response[8] = 0x00;
                 response[9] = 0x23; // UNSUPPORTED_VERSION (35)
             } else {
                 response[8] = 0x00;
                 response[9] = 0x00; // No error
             }

             OutputStream out = clientSocket.getOutputStream();
             out.write(response);
             out.flush();
         }

       //  writing output stream

     } catch (IOException e) {
       System.out.println("IOException: " + e.getMessage());
     } finally {
       try {
         if (clientSocket != null) {
           clientSocket.close();
         }
       } catch (IOException e) {
         System.out.println("IOException: " + e.getMessage());
       }
     }
  }
}
