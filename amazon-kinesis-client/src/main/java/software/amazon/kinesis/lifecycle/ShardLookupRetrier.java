/*
 * Copyright 2026 Amazon.com, Inc. or its affiliates.
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
package software.amazon.kinesis.lifecycle;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.kinesis.leases.ShardDetector;

/**
 * Bounded retry of {@link ShardDetector#shard(String)} for {@link ProcessTask}.
 *
 * <p>{@code ProcessTask} looks the shard up in its constructor, after the batch has already been acknowledged to the
 * records publisher. A {@code ListShards} failure (for example {@code RuntimeException(TimeoutException)} from
 * {@code KinesisShardDetector}) used to propagate out of the constructor, so the batch was never processed and never
 * re-fetched. This helper retries the lookup with exponential backoff and, if every attempt fails, returns
 * {@code null} so the batch is still processed (without the KPL hash-range de-duplication filter, exactly as with
 * {@code skipShardSyncAtWorkerInitializationIfLeasesExist=true}) instead of being dropped.
 */
@Slf4j
final class ShardLookupRetrier {
    static final int DEFAULT_MAX_ATTEMPTS = 3;
    static final long MAX_BACKOFF_MILLIS = 10_000L;

    private ShardLookupRetrier() {}

    static Shard lookup(
            ShardDetector shardDetector, String shardId, String shardInfoId, int maxAttempts, long backoffMillis) {
        final int attempts = Math.max(1, maxAttempts);
        long sleepMillis = Math.max(0L, backoffMillis);
        for (int attempt = 1; ; attempt++) {
            try {
                return shardDetector.shard(shardId);
            } catch (RuntimeException e) {
                if (attempt >= attempts) {
                    log.warn(
                            "{}: shard lookup failed (attempt {}/{}), giving up; processing batch without KPL"
                                    + " de-duplication",
                            shardInfoId,
                            attempt,
                            attempts,
                            e);
                    return null;
                }
                log.warn(
                        "{}: shard lookup failed (attempt {}/{}), retrying in {} ms",
                        shardInfoId,
                        attempt,
                        attempts,
                        sleepMillis,
                        e);
            }
            if (sleepMillis > 0) {
                try {
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.warn("{}: interrupted while retrying shard lookup; processing batch without KPL"
                            + " de-duplication", shardInfoId);
                    return null;
                }
            }
            sleepMillis = Math.min(MAX_BACKOFF_MILLIS, Math.max(1L, sleepMillis) * 2);
        }
    }
}
