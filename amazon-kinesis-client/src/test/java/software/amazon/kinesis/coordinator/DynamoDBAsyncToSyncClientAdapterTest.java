package software.amazon.kinesis.coordinator;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DynamoDBAsyncToSyncClientAdapterTest {

    private static final String TEST_TABLE_NAME = "TestTable";

    @Mock
    private DynamoDbAsyncClient mockAsyncClient;

    private DynamoDbAsyncToSyncClientAdapter adapter;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        adapter = new DynamoDbAsyncToSyncClientAdapter(mockAsyncClient);
    }

    @Test
    public void testGetItem() {
        final Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.builder().s("1").build());
        final Map<String, AttributeValue> item = new HashMap<>(key);
        item.put("data", AttributeValue.builder().s("test data").build());
        final GetItemRequest request =
                GetItemRequest.builder().key(key).tableName(TEST_TABLE_NAME).build();
        final GetItemResponse expectedResponse =
                GetItemResponse.builder().item(item).build();
        when(mockAsyncClient.getItem(any(GetItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(expectedResponse));

        final GetItemResponse actualResponse = adapter.getItem(request);

        assertEquals(expectedResponse, actualResponse);
        verify(mockAsyncClient).getItem(any(GetItemRequest.class));
    }

    @Test
    public void testPutItem() {
        final Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("1").build());
        item.put("data", AttributeValue.builder().s("test data").build());
        final PutItemRequest request =
                PutItemRequest.builder().tableName(TEST_TABLE_NAME).item(item).build();
        final PutItemResponse expectedResponse = PutItemResponse.builder().build();
        when(mockAsyncClient.putItem(any(PutItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(expectedResponse));

        final PutItemResponse actualResponse = adapter.putItem(request);

        assertEquals(expectedResponse, actualResponse);
        verify(mockAsyncClient).putItem(any(PutItemRequest.class));
    }

    @Test
    public void testUpdateItem() {
        final Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.builder().s("1").build());

        final Map<String, AttributeValueUpdate> updates = new HashMap<>();
        updates.put(
                "data",
                AttributeValueUpdate.builder()
                        .value(AttributeValue.builder().s("updated data").build())
                        .action(AttributeAction.PUT)
                        .build());

        final UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(TEST_TABLE_NAME)
                .key(key)
                .attributeUpdates(updates)
                .build();

        final UpdateItemResponse expectedResponse = UpdateItemResponse.builder().build();

        when(mockAsyncClient.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(expectedResponse));

        final UpdateItemResponse actualResponse = adapter.updateItem(request);

        assertEquals(expectedResponse, actualResponse);
        verify(mockAsyncClient).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    public void testDeleteItem() {
        final Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.builder().s("1").build());
        final DeleteItemResponse expectedResponse = DeleteItemResponse.builder().build();
        final DeleteItemRequest request =
                DeleteItemRequest.builder().tableName(TEST_TABLE_NAME).key(key).build();
        when(mockAsyncClient.deleteItem(any(DeleteItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(expectedResponse));

        final DeleteItemResponse actualResponse = adapter.deleteItem(request);

        assertEquals(expectedResponse, actualResponse);
        verify(mockAsyncClient).deleteItem(any(DeleteItemRequest.class));
    }

    @Test
    public void testCreateTable() {
        final CreateTableRequest request =
                CreateTableRequest.builder().tableName(TEST_TABLE_NAME).build();
        final CreateTableResponse expectedResponse = CreateTableResponse.builder()
                .tableDescription(
                        TableDescription.builder().tableName(TEST_TABLE_NAME).build())
                .build();
        when(mockAsyncClient.createTable(any(CreateTableRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(expectedResponse));

        final CreateTableResponse actualResponse = adapter.createTable(request);

        assertEquals(expectedResponse, actualResponse);
        verify(mockAsyncClient).createTable(any(CreateTableRequest.class));
    }

    @Test
    public void testDescribeTable() {
        final DescribeTableRequest request =
                DescribeTableRequest.builder().tableName(TEST_TABLE_NAME).build();
        final DescribeTableResponse expectedResponse = DescribeTableResponse.builder()
                .table(TableDescription.builder().tableName(TEST_TABLE_NAME).build())
                .build();
        when(mockAsyncClient.describeTable(any(DescribeTableRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(expectedResponse));

        final DescribeTableResponse actualResponse = adapter.describeTable(request);

        assertEquals(expectedResponse, actualResponse);
        verify(mockAsyncClient).describeTable(any(DescribeTableRequest.class));
    }

    @Test
    public void testDeleteTable() {
        final DeleteTableRequest request =
                DeleteTableRequest.builder().tableName(TEST_TABLE_NAME).build();
        final DeleteTableResponse expectedResponse = DeleteTableResponse.builder()
                .tableDescription(
                        TableDescription.builder().tableName(TEST_TABLE_NAME).build())
                .build();
        when(mockAsyncClient.deleteTable(any(DeleteTableRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(expectedResponse));

        final DeleteTableResponse actualResponse = adapter.deleteTable(request);

        assertEquals(expectedResponse, actualResponse);
        verify(mockAsyncClient).deleteTable(any(DeleteTableRequest.class));
    }

    @Test
    public void testException() {
        final GetItemRequest request = GetItemRequest.builder()
                .tableName(TEST_TABLE_NAME)
                .key(new HashMap<String, AttributeValue>() {
                    {
                        put("key", AttributeValue.fromS("anyKey"));
                    }
                })
                .build();
        final ProvisionedThroughputExceededException exception = ProvisionedThroughputExceededException.builder()
                .message("Test exception")
                .build();
        when(mockAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> {
            throw exception;
        }));

        try {
            adapter.getItem(request);
            fail("Expected RuntimeException");
        } catch (final ProvisionedThroughputExceededException e) {
            assertEquals(exception, e);
        }
        verify(mockAsyncClient).getItem(any(GetItemRequest.class));
    }

    /**
     * A call whose future never completes must not block the caller forever. The backstop wait trips,
     * the hung future is cancelled, and the call surfaces as an {@link SdkClientException}.
     */
    @Test
    public void testHungCallTimesOutAfterBackstopAndIsCancelled() {
        final DynamoDbAsyncToSyncClientAdapter timedAdapter = new DynamoDbAsyncToSyncClientAdapter(
                mockAsyncClient, Duration.ofMillis(100), Duration.ofSeconds(10), Duration.ofSeconds(5));
        final GetItemRequest request =
                GetItemRequest.builder().tableName(TEST_TABLE_NAME).build();
        final CompletableFuture<GetItemResponse> neverCompletes = new CompletableFuture<>();
        when(mockAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(neverCompletes);

        try {
            timedAdapter.getItem(request);
            fail("Expected SdkClientException");
        } catch (final SdkClientException e) {
            assertTrue(e.getMessage().contains("did not complete within"));
            assertTrue(e.getCause() instanceof TimeoutException);
        }
        assertTrue("hung future should be cancelled", neverCompletes.isCancelled());
    }

    /**
     * Every outgoing request must carry the KCL-owned per-request SDK timeouts so the SDK abandons a slow
     * call on its own, independent of the customer's async-client configuration.
     */
    @Test
    public void testOutgoingRequestCarriesConfiguredTimeouts() {
        final Duration apiCallTimeout = Duration.ofSeconds(10);
        final Duration apiCallAttemptTimeout = Duration.ofSeconds(5);
        final DynamoDbAsyncToSyncClientAdapter timedAdapter = new DynamoDbAsyncToSyncClientAdapter(
                mockAsyncClient, Duration.ofSeconds(15), apiCallTimeout, apiCallAttemptTimeout);
        final GetItemRequest request =
                GetItemRequest.builder().tableName(TEST_TABLE_NAME).build();
        when(mockAsyncClient.getItem(any(GetItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(GetItemResponse.builder().build()));

        timedAdapter.getItem(request);

        final ArgumentCaptor<GetItemRequest> captor = ArgumentCaptor.forClass(GetItemRequest.class);
        verify(mockAsyncClient).getItem(captor.capture());
        final GetItemRequest sent = captor.getValue();
        assertEquals(TEST_TABLE_NAME, sent.tableName());
        assertTrue("override configuration must be present", sent.overrideConfiguration().isPresent());
        final AwsRequestOverrideConfiguration override = sent.overrideConfiguration().get();
        assertEquals(apiCallTimeout, override.apiCallTimeout().orElse(null));
        assertEquals(apiCallAttemptTimeout, override.apiCallAttemptTimeout().orElse(null));
    }

    /**
     * Applying the KCL-owned timeouts must merge onto, not clobber, any override configuration a request
     * already carries (for example a custom header).
     */
    @Test
    public void testExistingRequestOverrideIsPreserved() {
        final Duration apiCallTimeout = Duration.ofSeconds(10);
        final Duration apiCallAttemptTimeout = Duration.ofSeconds(5);
        final DynamoDbAsyncToSyncClientAdapter timedAdapter = new DynamoDbAsyncToSyncClientAdapter(
                mockAsyncClient, Duration.ofSeconds(15), apiCallTimeout, apiCallAttemptTimeout);
        final GetItemRequest request = GetItemRequest.builder()
                .tableName(TEST_TABLE_NAME)
                .overrideConfiguration(AwsRequestOverrideConfiguration.builder()
                        .putHeader("X-Custom-Header", "custom-value")
                        .build())
                .build();
        when(mockAsyncClient.getItem(any(GetItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(GetItemResponse.builder().build()));

        timedAdapter.getItem(request);

        final ArgumentCaptor<GetItemRequest> captor = ArgumentCaptor.forClass(GetItemRequest.class);
        verify(mockAsyncClient).getItem(captor.capture());
        final AwsRequestOverrideConfiguration override =
                captor.getValue().overrideConfiguration().orElse(null);
        assertFalse("override configuration must be present", override == null);
        final List<String> header = override.headers().get("X-Custom-Header");
        assertTrue("existing custom header must be preserved",
                header != null && header.contains("custom-value"));
        assertEquals(apiCallTimeout, override.apiCallTimeout().orElse(null));
        assertEquals(apiCallAttemptTimeout, override.apiCallAttemptTimeout().orElse(null));
    }
}
