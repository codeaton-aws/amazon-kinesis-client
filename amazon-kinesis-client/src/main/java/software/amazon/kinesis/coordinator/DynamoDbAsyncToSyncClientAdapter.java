/*
 * Copyright 2024 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.kinesis.coordinator;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
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
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;
import software.amazon.awssdk.services.dynamodb.paginators.BatchGetItemIterable;
import software.amazon.awssdk.services.dynamodb.paginators.QueryIterable;
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.LeaseManagementConfig;

/**
 * DDB Lock client depends on DynamoDbClient and KCL only has DynamoDbAsyncClient configured.
 * This wrapper delegates APIs from sync client to async client internally so that it can
 * be used with the DDB Lock client.
 * <p>
 * Every call waits at most {@code timeout} for the async client. A call that does not complete in time is
 * cancelled and fails with an {@link SdkClientException}, so a hung request cannot block a caller (for example
 * the synchronized leader decider shared by the Scheduler and lease assignment threads) indefinitely.
 */
@KinesisClientInternalApi
public class DynamoDbAsyncToSyncClientAdapter implements DynamoDbClient {
    private final DynamoDbAsyncClient asyncClient;
    private final Duration timeout;

    public DynamoDbAsyncToSyncClientAdapter(final DynamoDbAsyncClient asyncClient) {
        this(asyncClient, LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT);
    }

    public DynamoDbAsyncToSyncClientAdapter(final DynamoDbAsyncClient asyncClient, final Duration timeout) {
        this.asyncClient = asyncClient;
        this.timeout = timeout;
    }

    @Override
    public String serviceName() {
        return asyncClient.serviceName();
    }

    @Override
    public void close() {
        asyncClient.close();
    }

    private <T> T handleException(final Supplier<CompletableFuture<T>> task) {
        final CompletableFuture<T> future = task.get();
        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final ExecutionException e) {
            rethrow(e.getCause());
            return null;
        } catch (final TimeoutException e) {
            future.cancel(true);
            throw SdkClientException.builder()
                    .message("DynamoDB call did not complete within " + timeout)
                    .cause(e)
                    .build();
        } catch (final InterruptedException e) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw SdkClientException.builder()
                    .message("Interrupted while waiting for DynamoDB call")
                    .cause(e)
                    .build();
        }
    }

    @Override
    public CreateTableResponse createTable(final CreateTableRequest request) {
        return handleException(() -> asyncClient.createTable(request));
    }

    @Override
    public DescribeTableResponse describeTable(final DescribeTableRequest request) {
        return handleException(() -> asyncClient.describeTable(request));
    }

    @Override
    public DeleteTableResponse deleteTable(final DeleteTableRequest request) {
        return handleException(() -> asyncClient.deleteTable(request));
    }

    @Override
    public DeleteItemResponse deleteItem(final DeleteItemRequest request) {
        return handleException(() -> asyncClient.deleteItem(request));
    }

    @Override
    public GetItemResponse getItem(final GetItemRequest request) {
        return handleException(() -> asyncClient.getItem(request));
    }

    @Override
    public PutItemResponse putItem(final PutItemRequest request) {
        return handleException(() -> asyncClient.putItem(request));
    }

    @Override
    public UpdateItemResponse updateItem(final UpdateItemRequest request) {
        return handleException(() -> asyncClient.updateItem(request));
    }

    @Override
    public QueryResponse query(final QueryRequest request) {
        return handleException(() -> asyncClient.query(request));
    }

    @Override
    public ScanResponse scan(final ScanRequest request) {
        return handleException(() -> asyncClient.scan(request));
    }

    @Override
    public QueryIterable queryPaginator(final QueryRequest request) {
        return new QueryIterable(this, request);
    }

    @Override
    public ScanIterable scanPaginator(final ScanRequest request) {
        return new ScanIterable(this, request);
    }

    @Override
    public BatchGetItemResponse batchGetItem(final BatchGetItemRequest request) {
        return handleException(() -> asyncClient.batchGetItem(request));
    }

    @Override
    public BatchWriteItemResponse batchWriteItem(final BatchWriteItemRequest request) {
        return handleException(() -> asyncClient.batchWriteItem(request));
    }

    @Override
    public BatchGetItemIterable batchGetItemPaginator(final BatchGetItemRequest request) {
        return new BatchGetItemIterable(this, request);
    }

    private static void rethrow(final Throwable e) {
        castAndThrow(e);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void castAndThrow(final Throwable e) throws T {
        throw (T) e;
    }
}
