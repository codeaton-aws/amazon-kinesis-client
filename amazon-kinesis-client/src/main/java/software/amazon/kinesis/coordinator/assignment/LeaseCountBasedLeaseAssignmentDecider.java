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

package software.amazon.kinesis.coordinator.assignment;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static java.util.Objects.nonNull;

/**
 * A lease assignment decider that distributes leases based on lease count balancing.
 * This implementation ensures fair distribution of leases across workers by:
 * <ul>
 *   <li>Assigning expired/unassigned leases to workers with the fewest current leases</li>
 *   <li>Balancing lease distribution to minimize variance between workers</li>
 *   <li>Respecting the maximum lease limit per worker</li>
 * </ul>
 */
@Slf4j
@KinesisClientInternalApi
@RequiredArgsConstructor
public final class LeaseCountBasedLeaseAssignmentDecider implements LeaseAssignmentDecider {

    private final LeaseAssignmentManager.InMemoryStorageView inMemoryStorageView;
    private final int maxLeasesForWorker;
    private final Supplier<Long> nanoTimeProvider;

    /**
     * Assigns expired or unassigned leases to workers with the fewest current leases.
     * Leases are sorted by lastCounterIncrementNanos to ensure consistent assignment order.
     *
     * @param expiredOrUnAssignedLeases list of leases that need to be assigned to workers
     */
    @Override
    public void assignExpiredOrUnassignedLeases(final List<Lease> expiredOrUnAssignedLeases) {
        if (expiredOrUnAssignedLeases.isEmpty()
                || inMemoryStorageView.getActiveWorkerIdSet().isEmpty()) {
            return;
        }

        // Leases that are a parent of another lease first (finishing them unblocks children), then oldest first.
        expiredOrUnAssignedLeases.sort(
                Comparator.comparingInt((Lease lease) -> inMemoryStorageView.isParentOfAnotherLease(lease) ? 0 : 1)
                        .thenComparingLong(Lease::lastCounterIncrementNanos));

        final Map<String, Integer> countedPerActiveWorker = getActiveWorkerLeaseCounts();
        final int target = calculateTargetLeaseCount();

        log.info(
                "Lease count balancing: {} total leases, {} workers, target {} leases per worker",
                inMemoryStorageView.getLeaseList().size(),
                inMemoryStorageView.getActiveWorkerIdSet().size(),
                target);

        final Set<Lease> assignedLeases = new HashSet<>();
        for (final Lease lease : expiredOrUnAssignedLeases) {
            final boolean counted = isCounted(lease);
            // An expired lease is still mapped to its current owner: do not count it there while choosing its owner.
            final String currentOwner = lease.actualOwner();
            final boolean heldByActive = counted
                    && nonNull(currentOwner)
                    && countedPerActiveWorker.containsKey(currentOwner)
                    && inMemoryStorageView
                            .getWorkerToLeasesMap()
                            .getOrDefault(currentOwner, Collections.emptySet())
                            .contains(lease);
            if (heldByActive) {
                countedPerActiveWorker.merge(currentOwner, -1, Integer::sum);
            }
            // least-loaded worker below the balance target and below maxLeasesForWorker (counted leases)
            final String workerToAssign = countedPerActiveWorker.entrySet().stream()
                    .filter(e -> !counted || (e.getValue() < target && e.getValue() < maxLeasesForWorker))
                    .min(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey)
                    .orElse(null);
            if (workerToAssign == null) {
                if (heldByActive) {
                    countedPerActiveWorker.merge(currentOwner, 1, Integer::sum);
                }
                continue;
            }
            assignLease(lease, workerToAssign);
            assignedLeases.add(lease);
            if (counted) {
                countedPerActiveWorker.merge(workerToAssign, 1, Integer::sum);
            }
        }

        expiredOrUnAssignedLeases.removeAll(assignedLeases);
    }

    /**
     * Balances lease distribution across workers to minimize variance.
     * Avoids stealing leases that are pending handoff to prevent disruption.
     */
    @Override
    public void balanceWorkerVariance() {
        if (inMemoryStorageView.getActiveWorkerIdSet().isEmpty()) {
            return;
        }
        final Map<String, Integer> leaseCountPerActiveWorker = getActiveWorkerLeaseCounts();
        final Map<String, List<Lease>> availableLeasesByWorker = computeAvailableLeases();
        final int target = calculateTargetLeaseCount();

        final List<Map.Entry<String, Integer>> sortedWorkers = leaseCountPerActiveWorker.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toList());

        // Two pointers: left (underloaded) and right (overloaded)
        int left = 0;
        int right = sortedWorkers.size() - 1;

        while (left < right) {
            final Map.Entry<String, Integer> underloaded = sortedWorkers.get(left);
            final Map.Entry<String, Integer> overloaded = sortedWorkers.get(right);

            final int needed = target - underloaded.getValue();
            final int excess = overloaded.getValue() - target;

            if (needed <= 0) {
                left++;
                continue;
            }
            if (excess <= 0) {
                right--;
                continue;
            }

            // Find available lease from overloaded worker. These are leases not pending handoffs
            final List<Lease> availableLeases = availableLeasesByWorker.get(overloaded.getKey());
            // never move a counted lease to a worker that is already at maxLeasesForWorker
            final boolean underloadedAtMax = underloaded.getValue() >= maxLeasesForWorker;
            Lease leaseToSteal = null;
            if (availableLeases != null) {
                for (int i = 0; i < availableLeases.size(); i++) {
                    if (!underloadedAtMax || !isCounted(availableLeases.get(i))) {
                        leaseToSteal = availableLeases.remove(i);
                        break;
                    }
                }
            }

            if (leaseToSteal != null) {
                assignLease(leaseToSteal, underloaded.getKey());
                if (isCounted(leaseToSteal)) {
                    underloaded.setValue(underloaded.getValue() + 1);
                    overloaded.setValue(overloaded.getValue() - 1);
                }
            } else {
                right--;
            }
        }
    }

    /**
     * Calculates the target number of leases each worker should have.
     *
     * @return target lease count per worker, capped by maxLeasesForWorker
     */
    private int calculateTargetLeaseCount() {
        // Balance on leases that need a slot: not SHARD_END, and not a child that is still blocked on a parent
        // (blocked children are deferred until their parents end). maxLeasesForWorker is not applied to the target;
        // it is a hard cap on counted leases enforced per assignment.
        final long needSlot = inMemoryStorageView.getLeaseList().stream()
                .filter(this::isCounted)
                .filter(lease -> !inMemoryStorageView.isLeaseBlocked(lease) || nonNull(lease.actualOwner()))
                .count();
        final int totalWorkers = inMemoryStorageView.getActiveWorkerIdSet().size();
        final int target = (int) Math.max(1, (needSlot + totalWorkers - 1) / Math.max(1, totalWorkers));
        if (needSlot > (long) maxLeasesForWorker * totalWorkers) {
            log.warn(
                    "{} leases need a slot but fleet capacity is {} ({} workers x maxLeasesForWorker {})."
                            + " Some shards may not be processed until leases finish or workers are added.",
                    needSlot,
                    (long) maxLeasesForWorker * totalWorkers,
                    totalWorkers,
                    maxLeasesForWorker);
        }
        return target;
    }

    /**
     * Computes lease counts using the in-memory worker-to-leases mapping.
     * This reflects real-time assignments made during the current run.
     *
     * @return map of worker ID to estimated lease count after pending transfers
     */
    private Map<String, Integer> getActiveWorkerLeaseCounts() {
        final Map<String, Integer> estimatedCounts = new HashMap<>();
        final Set<String> activeWorkers = inMemoryStorageView.getActiveWorkerIdSet();

        // Count counted (non-SHARD_END) leases by actual owner, only for active workers, plus leases being handed
        // off to the worker.
        for (final Map.Entry<String, Set<Lease>> entry :
                inMemoryStorageView.getWorkerToLeasesMap().entrySet()) {
            final String workerId = entry.getKey();
            if (nonNull(workerId) && activeWorkers.contains(workerId)) {
                estimatedCounts.put(workerId, (int)
                        entry.getValue().stream().filter(this::isCounted).count());
            }
        }

        // Ensure all active workers are represented
        for (final String workerId : activeWorkers) {
            estimatedCounts.merge(workerId, inMemoryStorageView.getPendingHandoffInCount(workerId), Integer::sum);
        }

        return estimatedCounts;
    }

    /**
     * Computes available leases that can be safely transferred (not pending handoff).
     * Uses workerToLeasesMap for real-time view of assignments.
     *
     * @return map of worker ID to list of available leases
     */
    private Map<String, List<Lease>> computeAvailableLeases() {
        final Map<String, List<Lease>> availableLeases = new HashMap<>();
        final long currentTimeMillis = TimeUnit.NANOSECONDS.toMillis(nanoTimeProvider.get());

        for (final Map.Entry<String, Set<Lease>> entry :
                inMemoryStorageView.getWorkerToLeasesMap().entrySet()) {
            final String workerId = entry.getKey();
            final List<Lease> workerAvailableLeases = entry.getValue().stream()
                    .filter(lease -> !lease.blockedOnPendingCheckpoint(currentTimeMillis))
                    .filter(lease -> !inMemoryStorageView.isLeaseBlocked(lease))
                    .collect(Collectors.toList());

            if (!workerAvailableLeases.isEmpty()) {
                availableLeases.put(workerId, workerAvailableLeases);
            }
        }

        return availableLeases;
    }

    /**
     * A lease takes a maxLeasesForWorker slot unless it is at SHARD_END. Without a maxLeasesForWorker limit every lease
     * is counted, so balancing is unchanged.
     */
    private boolean isCounted(final Lease lease) {
        return maxLeasesForWorker == Integer.MAX_VALUE || !ExtendedSequenceNumber.SHARD_END.equals(lease.checkpoint());
    }

    private void assignLease(final Lease lease, final String workerId) {
        log.debug("Assigning lease {} to worker {}", lease.leaseKey(), workerId);
        inMemoryStorageView.performLeaseAssignment(lease, workerId);
    }
}
