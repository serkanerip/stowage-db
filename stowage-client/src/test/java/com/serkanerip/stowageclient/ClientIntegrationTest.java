package com.serkanerip.stowageclient;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.serkanerip.stowagecommon.DeleteRequest;
import com.serkanerip.stowagecommon.GetRequest;
import com.serkanerip.stowagecommon.GetResponse;
import com.serkanerip.stowagecommon.PutRequest;
import com.serkanerip.stowagecommon.SimpleResponse;
import com.serkanerip.stowagecommon.StowageDbCodec;
import com.serkanerip.stowagecommon.TransportMessage;
import com.serkanerip.stowagecommon.TransportMessageType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 4000, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ClientIntegrationTest {

    // Should be lower than JUnit @Timeout value
    private static final int REQUEST_TIMEOUT = 1000;

    private ServerSocket serverSocket;
    private Client client;

    @BeforeEach
    void setUp() throws IOException {
        serverSocket = new ServerSocket(0); // automatically allocate port number
        client = new Client(ClientConfig.builder()
            .port(serverSocket.getLocalPort())
            .requestTimeout(REQUEST_TIMEOUT)
            .build()
        );
    }

    @AfterEach
    void tearDown() throws IOException {
        client.shutdown();
        serverSocket.close();
    }

    @Test
    void testSuccessfulGet() throws IOException, InterruptedException {
        var socket = serverSocket.accept();
        var key = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        var expectedValue = "EXPECTED_VAL".getBytes(StandardCharsets.UTF_8);
        var serverThread = handleClientMessage(socket, message -> {
                assertEquals(TransportMessageType.GET, message.getType());
                var request = GetRequest.decode(message.getPayload());
                assertArrayEquals(key, request.getKey());
            }, TransportMessageType.GET_RESPONSE, new GetResponse(expectedValue).encode()
        );
        var value = client.get(key);
        serverThread.join();
        assertArrayEquals(expectedValue, value);
    }

    @Test
    void testSuccessfulPut() throws IOException, InterruptedException {
        var socket = serverSocket.accept();
        var key = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        var expectedValue = "EXPECTED_VAL".getBytes(StandardCharsets.UTF_8);
        var serverThread = handleClientMessage(socket, message -> {
                assertEquals(TransportMessageType.PUT, message.getType());
                var request = PutRequest.decode(message.getPayload());
                assertArrayEquals(key, request.getKey());
                assertArrayEquals(expectedValue, request.getValue());
            }, TransportMessageType.SIMPLE_RESPONSE, SimpleResponse.SR_SUCCESS.encode()
        );
        var response = client.put(key, expectedValue);
        serverThread.join();
        assertTrue(response);
    }

    @Test
    void testSuccessfulDelete() throws IOException, InterruptedException {
        var socket = serverSocket.accept();
        var key = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        var serverThread = handleClientMessage(socket, message -> {
                assertEquals(TransportMessageType.DELETE, message.getType());
                var request = DeleteRequest.decode(message.getPayload());
                assertArrayEquals(key, request.getKey());
            }, TransportMessageType.SIMPLE_RESPONSE, SimpleResponse.SR_SUCCESS.encode()
        );
        var response = client.delete(key);
        serverThread.join();
        assertTrue(response);
    }

    @Test
    void testGetWithNullKey() {
        var exception = assertThrows(
            NullPointerException.class,
            () -> client.get(null)
        );
        assertEquals("Key cannot be null", exception.getMessage());
    }

    @Test
    void testPutWithNullKey() {
        var exception = assertThrows(
            NullPointerException.class,
            () -> client.put(null, new byte[0])
        );
        assertEquals("Key cannot be null", exception.getMessage());
    }

    @Test
    void testPutWithNullValue() {
        var exception = assertThrows(
            NullPointerException.class,
            () -> client.put(new byte[0], null)
        );
        assertEquals("Value cannot be null", exception.getMessage());
    }

    @Test
    void shouldCloseSocketOnShutdown() throws IOException {
        var socket = serverSocket.accept();
        socket.getOutputStream().write(10);
        socket.getOutputStream().flush();

        client.shutdown();

        try {
            assertEquals(-1, socket.getInputStream().read()); // Expect end-of-stream
        } catch (SocketException e) {
            // Accept connection reset as a valid outcome
            assertEquals("Connection reset", e.getMessage());
        }
        assertThrows(SocketException.class, () -> {
            // Doing write in a loop,  write may not fail immediately till RST package is received!
            var os = socket.getOutputStream();
            while (!Thread.currentThread().isInterrupted()) {
                os.write(1);
                os.flush();
                Thread.sleep(10);
            }
        });
    }

    @Test
    void testDeleteWithNullKey() {
        var exception = assertThrows(
            NullPointerException.class,
            () -> client.delete(null)
        );
        assertEquals("Key cannot be null", exception.getMessage());
    }

    @Test
    void testServerNotRunning() throws IOException {
        serverSocket.close();
        var port = serverSocket.getLocalPort();

        assertThrows(RuntimeException.class, () -> new Client("localhost", port));
    }

    @Test
    void shouldConnectToServer() throws IOException {
        var clientSocket = serverSocket.accept();

        assertNotNull(clientSocket);
        clientSocket.close();
        serverSocket.close();
    }

    @Test
    void putOnClosedServerShouldThrowError() throws IOException, InterruptedException {
        serverSocket.close();
        Thread.sleep(50);
        var key = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        var value = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);

        assertThrows(Client.ClientConnectionException.class, () -> client.put(key, value));
    }

    @Test
    void getOnClosedServerShouldThrowError() throws IOException, InterruptedException {
        serverSocket.close();
        Thread.sleep(50);
        var key = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);

        assertThrows(Client.ClientConnectionException.class, () -> client.get(key));
    }

    @Test
    @Timeout(5000)
    void deleteOnClosedServerShouldThrowError() throws IOException {
        serverSocket.close();
        await().atLeast(Duration.ofMillis(100)).until(
            () -> !client.isActive()
        );
        var key = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);

        assertThrows(Client.ClientConnectionException.class, () -> client.delete(key));
    }

    @Test
    void putShouldTimeout() {
        var key = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        var value = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);

        assertThrows(Client.RequestTimeoutException.class, () -> client.put(key, value));
    }

    @Test
    void getShouldTimeout() {
        var key = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);

        assertThrows(Client.RequestTimeoutException.class, () -> client.get(key));
    }

    @Test
    void deleteShouldTimeout() {
        var key = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);

        assertThrows(Client.RequestTimeoutException.class, () -> client.delete(key));
    }

    @Test
    void testClientCreationWithNullHost() {
        assertThrows(IllegalArgumentException.class, () -> new Client(null, 8080));
    }

    @Test
    void testClientCreationWithInvalidPort() {
        assertThrows(IllegalArgumentException.class, () -> new Client("localhost", -1));
    }

    @Test
    void testClientCreationWithNegativeTimeout() {
        assertThrows(IllegalArgumentException.class, () -> ClientConfig.builder().requestTimeout(-1).build());
    }

    private static Thread handleClientMessage(
        Socket socket,
        Consumer<TransportMessage> messageVerifier,
        TransportMessageType type,
        ByteBuf payload
    ) {
        return Thread.ofVirtual().start(() -> {
            var b = new byte[65000];
            try {
                // Read incoming data
                socket.getInputStream().read(b);
                var buf = Unpooled.wrappedBuffer(b);

                // Decode and verify message
                var message = StowageDbCodec.decode(buf);
                messageVerifier.accept(message);

                // Prepare and send response

                // Clear buffer and reset indices
                buf.clear();
                buf.writerIndex(0);
                buf.readerIndex(0);
                StowageDbCodec.encode(new TransportMessage(
                    type, message.getCorrelationId(), payload
                ), buf);
                socket.getOutputStream().write(buf.array());
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });
    }

}