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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class ReadyFirstShardPrioritizationTest {
    private static final long POLL_MILLIS = 10_000L;
    private static final long RELEASE_MILLIS = 60_000L;

    private LeaseRefresher leaseRefresher;
    private AtomicLong now;
    private ReadyFirstShardPrioritization prioritization;

    @Before
    public void setup() {
        leaseRefresher = mock(LeaseRefresher.class);
        now = new AtomicLong(1_000_000L);
        prioritization = new ReadyFirstShardPrioritization(
                new NoOpShardPrioritization(), 2, leaseRefresher, POLL_MILLIS, RELEASE_MILLIS, now::get);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNonPositiveMaxLeasesShouldFail() {
        new ReadyFirstShardPrioritization(
                new NoOpShardPrioritization(), 0, leaseRefresher, POLL_MILLIS, RELEASE_MILLIS);
    }

    @Test
    public void testProcessesAtMostMaxLeases() {
        final List<ShardInfo> held = Arrays.asList(shard("a"), shard("b"), shard("c"));

        assertEquals(Arrays.asList("a", "b"), ids(prioritization.prioritize(held)));
        // started leases keep their slot
        assertEquals(Arrays.asList("a", "b"), ids(prioritization.prioritize(held)));
    }

    @Test
    public void testShardEndLeasesDoNotTakeASlot() {
        final List<ShardInfo> held = Arrays.asList(ended("a"), shard("b"), shard("c"), shard("d"));

        assertEquals(Arrays.asList("a", "b", "c"), ids(prioritization.prioritize(held)));
    }

    @Test
    public void testSlotIsReusedWhenAStartedLeaseReachesShardEnd() {
        assertEquals(
                Arrays.asList("a", "b"),
                ids(prioritization.prioritize(Arrays.asList(shard("a"), shard("b"), shard("c")))));

        assertEquals(
                Arrays.asList("a", "b", "c"),
                ids(prioritization.prioritize(Arrays.asList(ended("a"), shard("b"), shard("c")))));
    }

    @Test
    public void testSlotIsFreedWhenALeaseIsNoLongerHeld() {
        prioritization.prioritize(Arrays.asList(shard("a"), shard("b"), shard("c")));

        assertEquals(Arrays.asList("b", "c"), ids(prioritization.prioritize(Arrays.asList(shard("b"), shard("c")))));
    }

    @Test
    public void testChildOfAHeldParentWaitsUntilTheParentEnds() {
        final ShardInfo child = shard("child", "parent");

        assertEquals(
                Collections.singletonList("parent"),
                ids(prioritization.prioritize(Arrays.asList(shard("parent"), child))));
        assertEquals(
                Arrays.asList("parent", "child"),
                ids(prioritization.prioritize(Arrays.asList(ended("parent"), child))));
    }

    @Test
    public void testReadyLeasesStartBeforeBlockedLeasesInTheSameRound() throws Exception {
        final Lease remoteParent = lease("remote", new ExtendedSequenceNumber("123"));
        when(leaseRefresher.getLease("remote")).thenReturn(remoteParent);

        final List<ShardInfo> result =
                prioritization.prioritize(Arrays.asList(shard("child", "remote"), shard("b"), shard("c")));

        assertEquals(Arrays.asList("b", "c"), ids(result));
    }

    @Test
    public void testRemoteParentIsReadAtMostOncePerPollInterval() throws Exception {
        final Lease remoteParent = lease("remote", new ExtendedSequenceNumber("123"));
        when(leaseRefresher.getLease("remote")).thenReturn(remoteParent);
        final List<ShardInfo> held = Collections.singletonList(shard("child", "remote"));

        assertEquals(0, prioritization.prioritize(held).size());
        now.addAndGet(POLL_MILLIS - 1);
        assertEquals(0, prioritization.prioritize(held).size());
        verify(leaseRefresher, times(1)).getLease("remote");

        remoteParent.checkpoint(ExtendedSequenceNumber.SHARD_END);
        now.addAndGet(1);
        assertEquals(Collections.singletonList("child"), ids(prioritization.prioritize(held)));
        verify(leaseRefresher, times(2)).getLease("remote");
    }

    @Test
    public void testRemoteParentThatWasCleanedUpIsTreatedAsEnded() throws Exception {
        when(leaseRefresher.getLease("remote")).thenReturn(null);

        assertEquals(
                Collections.singletonList("child"),
                ids(prioritization.prioritize(Collections.singletonList(shard("child", "remote")))));
    }

    @Test
    public void testReadyLeaseWithoutASlotIsReportedForReleaseAfterTheReleaseTime() {
        final List<ShardInfo> held = Arrays.asList(shard("a"), shard("b"), shard("c"));

        prioritization.prioritize(held);
        now.addAndGet(RELEASE_MILLIS - 1);
        prioritization.prioritize(held);
        assertTrue(prioritization.leasesToRelease().isEmpty());

        now.addAndGet(1);
        prioritization.prioritize(held);
        assertEquals(Collections.singleton("c"), prioritization.leasesToRelease());
        // reported once; the wait starts again if the lease is still held
        assertTrue(prioritization.leasesToRelease().isEmpty());
        verifyZeroInteractions(leaseRefresher);
    }

    @Test
    public void testBlockedLeaseIsNeverReleased() throws Exception {
        final List<ShardInfo> held = Arrays.asList(shard("a"), shard("b"), shard("child", "a"));

        prioritization.prioritize(held);
        now.addAndGet(10 * RELEASE_MILLIS);
        prioritization.prioritize(held);

        assertTrue(prioritization.leasesToRelease().isEmpty());
    }

    @Test
    public void testDelegateOrderIsKept() {
        final ReadyFirstShardPrioritization reversed = new ReadyFirstShardPrioritization(
                original -> {
                    final List<ShardInfo> copy = new java.util.ArrayList<>(original);
                    Collections.reverse(copy);
                    return copy;
                },
                2,
                leaseRefresher,
                POLL_MILLIS,
                RELEASE_MILLIS,
                now::get);

        assertEquals(
                Arrays.asList("c", "b"), ids(reversed.prioritize(Arrays.asList(shard("a"), shard("b"), shard("c")))));
    }

    @Test
    public void testStartedLeaseThatTheDelegateDoesNotReturnStillTakesASlot() {
        final AtomicBoolean skipFirst = new AtomicBoolean(false);
        final ReadyFirstShardPrioritization filtering = new ReadyFirstShardPrioritization(
                original -> skipFirst.get() ? original.subList(1, original.size()) : original,
                2,
                leaseRefresher,
                POLL_MILLIS,
                RELEASE_MILLIS,
                now::get);
        final List<ShardInfo> held = Arrays.asList(shard("a"), shard("b"), shard("c"));
        assertEquals(Arrays.asList("a", "b"), ids(filtering.prioritize(held)));

        // the delegate stops returning "a" (e.g. a depth limit): its shard consumer keeps running, so "c" waits
        skipFirst.set(true);
        assertEquals(Collections.singletonList("b"), ids(filtering.prioritize(held)));
    }

    @Test
    public void testMultiStreamParentLeaseKeys() throws Exception {
        final String stream = "123456789012:stream:1";
        final ShardInfo parent =
                new ShardInfo("parent", "token", Collections.emptyList(), new ExtendedSequenceNumber("1"), stream);
        final ShardInfo child = new ShardInfo(
                "child", "token", Collections.singletonList("parent"), new ExtendedSequenceNumber("1"), stream);

        assertEquals(Collections.singletonList("parent"), ids(prioritization.prioritize(Arrays.asList(parent, child))));
        verifyZeroInteractions(leaseRefresher);

        final ShardInfo otherChild = new ShardInfo(
                "other", "token", Collections.singletonList("remote"), new ExtendedSequenceNumber("1"), stream);
        when(leaseRefresher.getLease(stream + ":remote")).thenReturn(null);
        assertEquals(
                Arrays.asList("parent", "other"), ids(prioritization.prioritize(Arrays.asList(parent, otherChild))));
        verify(leaseRefresher).getLease(stream + ":remote");
    }

    @Test
    public void testLeaseTableReadsArePerCallLimited() throws Exception {
        final ReadyFirstShardPrioritization wide = new ReadyFirstShardPrioritization(
                new NoOpShardPrioritization(), 100, leaseRefresher, POLL_MILLIS, RELEASE_MILLIS, now::get);
        final List<ShardInfo> held = new java.util.ArrayList<>();
        for (int i = 0; i < 3 * ReadyFirstShardPrioritization.MAX_LEASE_TABLE_READS_PER_CALL; i++) {
            held.add(shard("child" + i, "remote" + i));
        }
        when(leaseRefresher.getLease(any())).thenReturn(null);

        assertEquals(
                ReadyFirstShardPrioritization.MAX_LEASE_TABLE_READS_PER_CALL,
                wide.prioritize(held).size());
        verify(leaseRefresher, times(ReadyFirstShardPrioritization.MAX_LEASE_TABLE_READS_PER_CALL))
                .getLease(any());
    }

    private static List<String> ids(final List<ShardInfo> shards) {
        return shards.stream().map(ShardInfo::shardId).collect(Collectors.toList());
    }

    private static ShardInfo shard(final String shardId, final String... parents) {
        return new ShardInfo(shardId, "token", Arrays.asList(parents), new ExtendedSequenceNumber("1"));
    }

    private static ShardInfo ended(final String shardId) {
        return new ShardInfo(shardId, "token", Collections.emptyList(), ExtendedSequenceNumber.SHARD_END);
    }

    private static Lease lease(final String leaseKey, final ExtendedSequenceNumber checkpoint) {
        final Lease lease = new Lease();
        lease.leaseKey(leaseKey);
        lease.checkpoint(checkpoint);
        return lease;
    }
}
