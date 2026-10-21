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
package software.amazon.kinesis.leases;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * Lets a worker process at most {@code maxLeasesForWorker} of the leases it holds at a time, starting the leases that
 * are ready to process first.
 *
 * <p>With {@code maxLeasesForWorker} set, child leases that are blocked on a parent and leases at SHARD_END do not
 * take a slot when leases are assigned, so a worker can hold more leases than {@code maxLeasesForWorker}. When a
 * blocked child becomes ready, its worker may already be processing {@code maxLeasesForWorker} leases. This
 * prioritization keeps such a lease assigned but does not return it (so the Scheduler does not start a
 * {@code ShardConsumer} for it) until a slot is free:</p>
 * <ul>
 *     <li>A lease that was returned before (its shard consumer was started) keeps its slot until it reaches SHARD_END
 *     or is no longer held.</li>
 *     <li>A lease at SHARD_END is always returned and does not take a slot.</li>
 *     <li>Any other lease is returned only when a slot is free and every immediate parent lease is at SHARD_END or no
 *     longer in the lease table. A parent held by this worker is checked in memory; other parents are read from the
 *     lease table at most once per {@code parentShardPollIntervalMillis} per lease, and at most
 *     {@value #MAX_LEASE_TABLE_READS_PER_CALL} reads are made per call.</li>
 *     <li>A ready lease that has waited for a slot for {@code releaseAfterMillis} is reported by
 *     {@link #leasesToRelease()} so that the Scheduler can release it to a worker with a free slot.</li>
 * </ul>
 *
 * <p>The returned leases keep the order of the delegate prioritization. Leases that the delegate does not return are
 * not started, as before; leases started earlier still count toward the slots.</p>
 */
@Slf4j
@KinesisClientInternalApi
public final class ReadyFirstShardPrioritization implements ShardPrioritization {
    static final int MAX_LEASE_TABLE_READS_PER_CALL = 10;
    private static final long LOG_INTERVAL_MILLIS = 30_000L;

    private final ShardPrioritization delegate;
    private final int maxLeasesForWorker;
    private final LeaseRefresher leaseRefresher;
    private final long parentShardPollIntervalMillis;
    private final long releaseAfterMillis;
    private final LongSupplier currentTimeMillis;

    /** Lease keys whose shard consumer was started. */
    private final Set<String> started = new HashSet<>();
    /** Lease key to the time it was first seen ready and waiting for a slot. */
    private final Map<String, Long> readySince = new HashMap<>();
    /** Lease key to the time of the last lease-table readiness check. */
    private final Map<String, Long> lastReadyCheck = new HashMap<>();
    /** Lease key to the result of the last lease-table readiness check. */
    private final Map<String, Boolean> readyResult = new HashMap<>();
    /** Ready leases that have waited for a slot for releaseAfterMillis. */
    private final Set<String> toRelease = new HashSet<>();

    private long lastLogMillis;
    private String lastLogState;

    public ReadyFirstShardPrioritization(
            @NonNull final ShardPrioritization delegate,
            final int maxLeasesForWorker,
            @NonNull final LeaseRefresher leaseRefresher,
            final long parentShardPollIntervalMillis,
            final long releaseAfterMillis) {
        this(
                delegate,
                maxLeasesForWorker,
                leaseRefresher,
                parentShardPollIntervalMillis,
                releaseAfterMillis,
                System::currentTimeMillis);
    }

    ReadyFirstShardPrioritization(
            @NonNull final ShardPrioritization delegate,
            final int maxLeasesForWorker,
            @NonNull final LeaseRefresher leaseRefresher,
            final long parentShardPollIntervalMillis,
            final long releaseAfterMillis,
            @NonNull final LongSupplier currentTimeMillis) {
        if (maxLeasesForWorker <= 0) {
            throw new IllegalArgumentException("maxLeasesForWorker must be positive: " + maxLeasesForWorker);
        }
        this.delegate = delegate;
        this.maxLeasesForWorker = maxLeasesForWorker;
        this.leaseRefresher = leaseRefresher;
        this.parentShardPollIntervalMillis = parentShardPollIntervalMillis;
        this.releaseAfterMillis = releaseAfterMillis;
        this.currentTimeMillis = currentTimeMillis;
    }

    @Override
    public synchronized List<ShardInfo> prioritize(final List<ShardInfo> original) {
        final long now = currentTimeMillis.getAsLong();
        final Map<String, ShardInfo> heldByKey = new HashMap<>();
        for (final ShardInfo shardInfo : original) {
            heldByKey.put(ShardInfo.getLeaseKey(shardInfo), shardInfo);
        }
        // forget leases that are no longer held
        started.retainAll(heldByKey.keySet());
        readySince.keySet().retainAll(heldByKey.keySet());
        lastReadyCheck.keySet().retainAll(heldByKey.keySet());
        readyResult.keySet().retainAll(heldByKey.keySet());
        toRelease.retainAll(heldByKey.keySet());

        // every started lease that is not at SHARD_END takes a slot, even if the delegate does not return it (its
        // shard consumer keeps running)
        int slotsInUse = 0;
        for (final ShardInfo shardInfo : original) {
            if (!shardInfo.isCompleted() && started.contains(ShardInfo.getLeaseKey(shardInfo))) {
                slotsInUse++;
            }
        }

        final List<ShardInfo> ordered = delegate.prioritize(original);
        final List<ShardInfo> result = new ArrayList<>(ordered.size());
        final int[] reads = {0};
        int waiting = 0;
        for (final ShardInfo shardInfo : ordered) {
            final String leaseKey = ShardInfo.getLeaseKey(shardInfo);
            if (shardInfo.isCompleted() || started.contains(leaseKey)) {
                result.add(shardInfo);
                continue;
            }
            waiting++;
            if (!isReady(shardInfo, heldByKey, now, reads)) {
                readySince.remove(leaseKey);
                continue;
            }
            if (slotsInUse < maxLeasesForWorker) {
                slotsInUse++;
                waiting--;
                started.add(leaseKey);
                readySince.remove(leaseKey);
                result.add(shardInfo);
                continue;
            }
            final long since = readySince.computeIfAbsent(leaseKey, k -> now);
            if (now - since >= releaseAfterMillis) {
                toRelease.add(leaseKey);
            }
        }
        logState(now, slotsInUse, original.size(), waiting);
        return result;
    }

    /**
     * Returns and forgets the ready leases that have waited for a slot for {@code releaseAfterMillis}. The caller is
     * expected to release them (for example evict them and stop renewing them) so that a worker with a free slot can
     * take them.
     *
     * @return lease keys to release
     */
    public synchronized Set<String> leasesToRelease() {
        if (toRelease.isEmpty()) {
            return new HashSet<>();
        }
        final Set<String> result = new HashSet<>(toRelease);
        toRelease.clear();
        readySince.keySet().removeAll(result);
        return result;
    }

    /**
     * A lease is ready when every immediate parent lease is at SHARD_END or no longer in the lease table. A parent held
     * by this worker is checked in memory; other parents are read from the lease table, at most once per
     * parentShardPollIntervalMillis per lease and at most MAX_LEASE_TABLE_READS_PER_CALL times per call.
     */
    private boolean isReady(
            final ShardInfo shardInfo, final Map<String, ShardInfo> heldByKey, final long now, final int[] reads) {
        final List<String> otherParents = new ArrayList<>();
        for (final String parentShardId : shardInfo.parentShardIds()) {
            final String parentKey = ShardInfo.getLeaseKey(shardInfo, parentShardId);
            final ShardInfo heldParent = heldByKey.get(parentKey);
            if (heldParent == null) {
                otherParents.add(parentKey);
            } else if (!heldParent.isCompleted()) {
                return false;
            }
        }
        if (otherParents.isEmpty()) {
            return true;
        }
        final String leaseKey = ShardInfo.getLeaseKey(shardInfo);
        final Long lastCheck = lastReadyCheck.get(leaseKey);
        if ((lastCheck != null && now - lastCheck < parentShardPollIntervalMillis)
                || reads[0] + otherParents.size() > MAX_LEASE_TABLE_READS_PER_CALL) {
            return readyResult.getOrDefault(leaseKey, false);
        }
        boolean ready = true;
        try {
            for (final String parentKey : otherParents) {
                reads[0]++;
                final Lease parent = leaseRefresher.getLease(parentKey);
                if (parent != null && !ExtendedSequenceNumber.SHARD_END.equals(parent.checkpoint())) {
                    ready = false;
                    break;
                }
            }
        } catch (Exception e) {
            log.warn("Failed to read the parent leases of {}; treating it as not ready", leaseKey, e);
            ready = false;
        }
        lastReadyCheck.put(leaseKey, now);
        readyResult.put(leaseKey, ready);
        return ready;
    }

    private void logState(final long now, final int slotsInUse, final int held, final int waiting) {
        final String state = slotsInUse + "/" + waiting;
        if (waiting > 0 && (!state.equals(lastLogState) || now - lastLogMillis >= LOG_INTERVAL_MILLIS)) {
            lastLogState = state;
            lastLogMillis = now;
            log.info(
                    "Processing {} of {} held leases (maxLeasesForWorker {}); {} leases wait for their parents or for"
                            + " a free slot",
                    slotsInUse,
                    held,
                    maxLeasesForWorker,
                    waiting);
        }
    }
}
