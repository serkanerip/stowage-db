package com.serkanerip.stowageclient;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.serkanerip.stowagecommon.DeleteRequest;
import com.serkanerip.stowagecommon.ErrorCode;
import com.serkanerip.stowagecommon.GetRequest;
import com.serkanerip.stowagecommon.GetResponse;
import com.serkanerip.stowagecommon.MessagePayload;
import com.serkanerip.stowagecommon.PutRequest;
import com.serkanerip.stowagecommon.SimpleResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ClientTest {
    @Mock
    private NettyClient mockNettyClient;

    private Client client;

    @BeforeEach
    void setUp() {
        client = new Client("localhost", 8080, mockNettyClient);
    }

    @Test
    void testSuccessfulGet() {
        // Arrange
        byte[] testKey = "testKey".getBytes();
        byte[] expectedValue = "testValue".getBytes();

        OngoingRequest mockRequest = mock(OngoingRequest.class);
        CompletableFuture<MessagePayload> future = CompletableFuture.completedFuture(
            new GetResponse(expectedValue)
        );

        when(mockRequest.getFuture()).thenReturn(future);
        when(mockNettyClient.sendMessage(any(GetRequest.class))).thenReturn(mockRequest);

        // Act
        byte[] result = client.get(testKey);

        // Assert
        assertArrayEquals(expectedValue, result);
        verify(mockNettyClient).sendMessage(any(GetRequest.class));
    }

    @Test
    void testGetWithNullKey() {
        var exception = assertThrows(
            NullPointerException.class,
            () -> client.get(null)
        );
        assertEquals("Key cannot be null", exception.getMessage());
        verify(mockNettyClient, never()).sendMessage(any(GetRequest.class));
    }

    @Test
    void testGetWithUnexpectedResponseType() {
        // Arrange
        byte[] testKey = "testKey".getBytes();

        OngoingRequest mockRequest = mock(OngoingRequest.class);
        CompletableFuture<MessagePayload> future = CompletableFuture.completedFuture(
            new SimpleResponse(true, ErrorCode.NO_ERR)  // Wrong response type
        );

        when(mockRequest.getFuture()).thenReturn(future);
        when(mockNettyClient.sendMessage(any(GetRequest.class))).thenReturn(mockRequest);

        // Act & Assert
        assertThrows(IllegalStateException.class, () -> client.get(testKey));
    }

    @Test
    void testSuccessfulPut() {
        // Arrange
        byte[] testKey = "testKey".getBytes();
        byte[] testValue = "testValue".getBytes();

        OngoingRequest mockRequest = mock(OngoingRequest.class);
        CompletableFuture<MessagePayload> future = CompletableFuture.completedFuture(
            new SimpleResponse(true, ErrorCode.NO_ERR)
        );

        when(mockRequest.getFuture()).thenReturn(future);
        when(mockNettyClient.sendMessage(any(PutRequest.class))).thenReturn(mockRequest);

        // Act
        boolean result = client.put(testKey, testValue);

        // Assert
        assertTrue(result);
        verify(mockNettyClient).sendMessage(any(PutRequest.class));
    }

    @Test
    void testPutWithFailedResponse() {
        // Arrange
        byte[] testKey = "testKey".getBytes();
        byte[] testValue = "testValue".getBytes();

        OngoingRequest mockRequest = mock(OngoingRequest.class);
        CompletableFuture<MessagePayload> future = CompletableFuture.completedFuture(
            new SimpleResponse(false, ErrorCode.INVALID_FRAME)
        );

        when(mockRequest.getFuture()).thenReturn(future);
        when(mockNettyClient.sendMessage(any(PutRequest.class))).thenReturn(mockRequest);

        // Act
        boolean result = client.put(testKey, testValue);

        // Assert
        assertFalse(result);
    }

    @Test
    void testPutWithNullKey() {
        var exception = assertThrows(
            NullPointerException.class,
            () -> client.put(null, new byte[0])
        );
        assertEquals("Key cannot be null", exception.getMessage());
        verify(mockNettyClient, never()).sendMessage(any(GetRequest.class));
    }

    @Test
    void testPutWithNullValue() {
        var exception = assertThrows(
            NullPointerException.class,
            () -> client.put(new byte[0], null)
        );
        assertEquals("Value cannot be null", exception.getMessage());
        verify(mockNettyClient, never()).sendMessage(any(GetRequest.class));
    }

    @Test
    void testDeleteSuccessful() {
        // Arrange
        byte[] testKey = "testKey".getBytes();

        OngoingRequest mockRequest = mock(OngoingRequest.class);
        CompletableFuture<MessagePayload> future = CompletableFuture.completedFuture(
            new SimpleResponse(true, ErrorCode.NO_ERR)
        );

        when(mockRequest.getFuture()).thenReturn(future);
        when(mockNettyClient.sendMessage(any(DeleteRequest.class))).thenReturn(mockRequest);

        // Act
        boolean result = client.delete(testKey);

        // Assert
        assertTrue(result);
        verify(mockNettyClient).sendMessage(any(DeleteRequest.class));
    }

    @Test
    void testDeleteWithNullKey() {
        var exception = assertThrows(
            NullPointerException.class,
            () -> client.delete(null)
        );
        assertEquals("Key cannot be null", exception.getMessage());
        verify(mockNettyClient, never()).sendMessage(any(GetRequest.class));
    }

    @Test
    void testGetWithInterruptedException() {
        // Arrange
        byte[] testKey = "testKey".getBytes();

        OngoingRequest mockRequest = mock(OngoingRequest.class);
        CompletableFuture<MessagePayload> future = new CompletableFuture<>();
        future.completeExceptionally(new InterruptedException("Test interrupt"));

        when(mockRequest.getFuture()).thenReturn(future);
        when(mockNettyClient.sendMessage(any(GetRequest.class))).thenReturn(mockRequest);

        // Act & Assert
        assertThrows(RuntimeException.class, () -> client.get(testKey));
        assertTrue(Thread.currentThread().isInterrupted());
    }

    @Test
    void testGetWithExecutionException() {
        // Arrange
        byte[] testKey = "testKey".getBytes();

        OngoingRequest mockRequest = mock(OngoingRequest.class);
        CompletableFuture<MessagePayload> future = new CompletableFuture<>();
        future.completeExceptionally(new ExecutionException(new RuntimeException("Test execution error")));

        when(mockRequest.getFuture()).thenReturn(future);
        when(mockNettyClient.sendMessage(any(GetRequest.class))).thenReturn(mockRequest);

        // Act & Assert
        assertThrows(RuntimeException.class, () -> client.get(testKey));
    }

    @Test
    void testShutdown() {
        // Act
        client.shutdown();

        // Assert
        verify(mockNettyClient).shutdown();
    }

    // Additional Edge Cases and Error Scenarios
    @Test
    void testClientCreationWithNullHost() {
        assertThrows(NullPointerException.class, () -> new Client(null, 8080));
    }

    @Test
    void testClientCreationWithInvalidPort() {
        assertThrows(IllegalArgumentException.class, () -> new Client("localhost", -1));
    }
}
