import java.io.*;

public class Main {

    public static void main(String[] args) {
        KafkaServer server = new KafkaServer(); // Default host and port
        try {
            server.start(); // Start the server
        } catch (IOException e) {
            System.err.println("Failed to start the server: " + e.getMessage());
            e.printStackTrace();
        }
    }


}
