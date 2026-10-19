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

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
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

/**
 * DDB Lock client depends on DynamoDbClient and KCL only has DynamoDbAsyncClient configured.
 * This wrapper delegates APIs from sync client to async client internally so that it can
 * be used with the DDB Lock client.
 * <p>
 * This adapter is the only bridge between the DynamoDB Lock Client (used by the leader decider shared by the
 * Scheduler main loop and the lease-assignment thread) and KCL's async DynamoDB client, so a single hung request
 * here can block coordination indefinitely. Two independent, KCL-owned bounds protect every call, and
 * neither is taken from the customer's {@link DynamoDbAsyncClient} configuration:
 * <ol>
 *   <li>A per-request SDK timeout ({@code apiCallTimeout} / {@code apiCallAttemptTimeout}) is merged onto every
 *       outgoing request so the SDK itself abandons a slow call -- and retries a slow attempt within the overall
 *       budget -- well before the coordinator's 30s heartbeat or 120s leader lease expires.</li>
 *   <li>A {@link CompletableFuture#get(long, TimeUnit)} backstop at a slightly larger value catches hangs that sit
 *       below the SDK (for example in the CRT event loop, where the per-request timeout may never fire). On timeout
 *       the future is cancelled and an {@link SdkClientException} is thrown, so a hung request surfaces as a normal
 *       call failure that callers already handle instead of blocking forever.</li>
 * </ol>
 * The per-request timeouts cover the pages fetched by the {@code queryPaginator}, {@code scanPaginator}, and
 * {@code batchGetItemPaginator} iterables because those iterate by calling back into {@link #query}, {@link #scan},
 * and {@link #batchGetItem} on this adapter.
 */
@KinesisClientInternalApi
public class DynamoDbAsyncToSyncClientAdapter implements DynamoDbClient {

    /**
     * Overall per-request SDK timeout. 10s is orders of magnitude above a healthy single-digit-millisecond
     * DynamoDB call yet well under the coordinator's 30s heartbeat and 120s leader lease, so a genuinely stuck
     * request fails fast without tripping on normal latency or throttling-and-retry.
     */
    static final Duration DEFAULT_API_CALL_TIMEOUT = Duration.ofSeconds(10);

    /**
     * Per-attempt SDK timeout. At 5s a single slow attempt is abandoned with room for the SDK's default retry
     * policy to make a second attempt inside the 10s overall budget, which smooths over a transient latency spike
     * instead of failing the whole call on the first slow attempt.
     */
    static final Duration DEFAULT_API_CALL_ATTEMPT_TIMEOUT = Duration.ofSeconds(5);

    /**
     * Local backstop on the blocking wait. 15s is slightly larger than the overall SDK timeout so the SDK's own
     * timeout (and any retry) normally fires first and this bound only engages for hangs below the SDK that never
     * complete the future. Still far under the 30s heartbeat and 120s lease.
     */
    static final Duration DEFAULT_BACKSTOP_TIMEOUT = Duration.ofSeconds(15);

    private final DynamoDbAsyncClient asyncClient;
    private final Duration backstopTimeout;
    private final Duration apiCallTimeout;
    private final Duration apiCallAttemptTimeout;

    public DynamoDbAsyncToSyncClientAdapter(final DynamoDbAsyncClient asyncClient) {
        this(asyncClient, DEFAULT_BACKSTOP_TIMEOUT, DEFAULT_API_CALL_TIMEOUT, DEFAULT_API_CALL_ATTEMPT_TIMEOUT);
    }

    /**
     * Visible-for-testing constructor that allows the KCL-owned timeouts to be tuned (for example to drive the
     * backstop in a unit test). Production code uses the single-argument constructor and its defaults.
     */
    DynamoDbAsyncToSyncClientAdapter(
            final DynamoDbAsyncClient asyncClient,
            final Duration backstopTimeout,
            final Duration apiCallTimeout,
            final Duration apiCallAttemptTimeout) {
        this.asyncClient = asyncClient;
        this.backstopTimeout = backstopTimeout;
        this.apiCallTimeout = apiCallTimeout;
        this.apiCallAttemptTimeout = apiCallAttemptTimeout;
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
            return future.get(backstopTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final ExecutionException e) {
            rethrow(e.getCause());
            return null;
        } catch (final TimeoutException e) {
            future.cancel(true);
            throw SdkClientException.builder()
                    .message("DynamoDB call did not complete within " + backstopTimeout)
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

    /**
     * Returns a copy of {@code request} carrying the KCL-owned per-request SDK timeouts, merged on top of any
     * {@link AwsRequestOverrideConfiguration} already set on the request so unrelated overrides (headers, metrics
     * publishers, and so on) are preserved rather than clobbered.
     */
    private <R extends AwsRequest> R withTimeout(final R request) {
        final AwsRequestOverrideConfiguration overrideConfiguration = request.overrideConfiguration()
                .map(AwsRequestOverrideConfiguration::toBuilder)
                .orElseGet(AwsRequestOverrideConfiguration::builder)
                .apiCallTimeout(apiCallTimeout)
                .apiCallAttemptTimeout(apiCallAttemptTimeout)
                .build();
        @SuppressWarnings("unchecked")
        final R withOverride = (R) request.toBuilder().overrideConfiguration(overrideConfiguration).build();
        return withOverride;
    }

    @Override
    public CreateTableResponse createTable(final CreateTableRequest request) {
        return handleException(() -> asyncClient.createTable(withTimeout(request)));
    }

    @Override
    public DescribeTableResponse describeTable(final DescribeTableRequest request) {
        return handleException(() -> asyncClient.describeTable(withTimeout(request)));
    }

    @Override
    public DeleteTableResponse deleteTable(final DeleteTableRequest request) {
        return handleException(() -> asyncClient.deleteTable(withTimeout(request)));
    }

    @Override
    public DeleteItemResponse deleteItem(final DeleteItemRequest request) {
        return handleException(() -> asyncClient.deleteItem(withTimeout(request)));
    }

    @Override
    public GetItemResponse getItem(final GetItemRequest request) {
        return handleException(() -> asyncClient.getItem(withTimeout(request)));
    }

    @Override
    public PutItemResponse putItem(final PutItemRequest request) {
        return handleException(() -> asyncClient.putItem(withTimeout(request)));
    }

    @Override
    public UpdateItemResponse updateItem(final UpdateItemRequest request) {
        return handleException(() -> asyncClient.updateItem(withTimeout(request)));
    }

    @Override
    public QueryResponse query(final QueryRequest request) {
        return handleException(() -> asyncClient.query(withTimeout(request)));
    }

    @Override
    public ScanResponse scan(final ScanRequest request) {
        return handleException(() -> asyncClient.scan(withTimeout(request)));
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
        return handleException(() -> asyncClient.batchGetItem(withTimeout(request)));
    }

    @Override
    public BatchWriteItemResponse batchWriteItem(final BatchWriteItemRequest request) {
        return handleException(() -> asyncClient.batchWriteItem(withTimeout(request)));
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
