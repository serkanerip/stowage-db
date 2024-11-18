package com.serkanerip.stowageclient;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;

import com.serkanerip.stowageserver.ServerOptions;
import com.serkanerip.stowageserver.StowageServer;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ClientTest {

    private Client client;

    private StowageServer server;

    private static final Path testDataPath = Path.of("./test-data");

    @BeforeEach
    public void setUp() {
        var options = new ServerOptions("localhost", 53800, testDataPath);
        server = new StowageServer(options);
        server.start();

        client = new Client("localhost", 53800);
    }

    @AfterEach
    public void tearDown() throws IOException {
        client.shutdown();
        server.shutdown();
        Files.walkFileTree(testDataPath, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }
        });
        Files.delete(testDataPath);
    }

    @Test
    void testPutAndGet() {
        byte[] key = "key1".getBytes();
        byte[] value = "value1".getBytes();

        // Put a value into the cache
        boolean success = client.put(key, value);
        assertTrue(success, "Put operation should succeed");

        // Get the value from the cache
        byte[] retrievedValue = client.get(key);
        assertArrayEquals(value, retrievedValue, "The value retrieved should match the value put in");
    }

    @Test
    void testDelete() {
        byte[] key = "key1".getBytes();
        byte[] value = "value1".getBytes();

        assertTrue(client.put(key, value), "Put operation should succeed");
        assertTrue(client.delete(key), "Delete operation should succeed");

        // Get the value from the cache
        byte[] retrievedValue = client.get(key);
        assertNull(retrievedValue, "The value retrieved should be null");
    }

    @Test
    void testGetNonExistentKey() {
        byte[] key = "nonexistentkey".getBytes();

        // Attempt to get a value for a key that does not exist
        byte[] retrievedValue = client.get(key);
        assertNull(retrievedValue, "Getting a non-existent key should return null");
    }

    @Test
    void testPutOverwrite() {
        byte[] key = "key2".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();

        // Put the first value
        client.put(key, value1);

        // Overwrite with a new value
        client.put(key, value2);

        // Get the value from the cache
        byte[] retrievedValue = client.get(key);
        assertArrayEquals(value2, retrievedValue, "The value retrieved should match the last value put in");
    }

    @Test
    void testMultipleClients() throws Exception {
        Client client1 = new Client("localhost", 53800);
        Client client2 = new Client("localhost", 53800);

        byte[] key1 = "client1Key".getBytes();
        byte[] key2 = "client2Key".getBytes();
        byte[] value1 = "client1Value".getBytes();
        byte[] value2 = "client2Value".getBytes();

        // Client 1 puts a value
        client1.put(key1, value1);
        // Client 2 puts a different value
        client2.put(key2, value2);

        // Client 1 retrieves its value
        assertArrayEquals(value1, client1.get(key1), "Client 1 should retrieve its own value");
        // Client 2 retrieves its value
        assertArrayEquals(value2, client2.get(key2), "Client 2 should retrieve its own value");

        client1.shutdown();
        client2.shutdown();
    }

    @Test
    void testConcurrentPutAndGet() throws InterruptedException {
        byte[] key = "concurrentKey".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();

        // Create a thread to put value1
        Thread putThread1 = new Thread(() -> {
            client.put(key, value1);
        });

        // Create a thread to put value2
        Thread putThread2 = new Thread(() -> {
            client.put(key, value2);
        });

        // Start both threads
        putThread1.start();
        putThread2.start();

        // Wait for both threads to finish
        putThread1.join();
        putThread2.join();

        // The value retrieved should be either value1 or value2, depending on which put operation completed last
        byte[] retrievedValue = client.get(key);
        assertTrue(
            new String(retrievedValue).equals("value1") || new String(retrievedValue).equals("value2"),
            "The value retrieved should match one of the values put in"
        );
    }

    @Test
    void testShutdownAndReconnect() throws InterruptedException {
        byte[] key = "keyShutdown".getBytes();
        byte[] value = "valueShutdown".getBytes();

        // Put a value into the cache
        client.put(key, value);

        // Shutdown the client
        client.shutdown();

        // Start a new client to reconnect
        client = new Client("localhost", 53800);

        // Get the value from the cache after reconnect
        byte[] retrievedValue = client.get(key);
        assertArrayEquals(value, retrievedValue, "The value should persist after client reconnects");
    }
}
