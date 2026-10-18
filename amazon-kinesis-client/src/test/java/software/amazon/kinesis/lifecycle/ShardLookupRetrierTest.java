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

import java.util.concurrent.TimeoutException;

import org.junit.Test;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.kinesis.leases.ShardDetector;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShardLookupRetrierTest {
    private static final String SHARD_ID = "shardId-000000000001";

    @Test
    public void retriesListShardsTimeoutAndReturnsShard() {
        ShardDetector detector = mock(ShardDetector.class);
        Shard shard = Shard.builder().shardId(SHARD_ID).build();
        when(detector.shard(SHARD_ID))
                .thenThrow(new RuntimeException(new TimeoutException()))
                .thenThrow(new RuntimeException(new TimeoutException()))
                .thenReturn(shard);

        assertSame(shard, ShardLookupRetrier.lookup(detector, SHARD_ID, SHARD_ID, 3, 0L));
        verify(detector, times(3)).shard(SHARD_ID);
    }

    @Test
    public void returnsNullInsteadOfThrowingWhenAllAttemptsFail() {
        ShardDetector detector = mock(ShardDetector.class);
        when(detector.shard(SHARD_ID)).thenThrow(new RuntimeException(new TimeoutException()));

        assertNull(ShardLookupRetrier.lookup(detector, SHARD_ID, SHARD_ID, 3, 0L));
        verify(detector, times(3)).shard(SHARD_ID);
    }

    @Test
    public void noRetryWhenLookupSucceeds() {
        ShardDetector detector = mock(ShardDetector.class);
        Shard shard = Shard.builder().shardId(SHARD_ID).build();
        when(detector.shard(SHARD_ID)).thenReturn(shard);

        assertSame(shard, ShardLookupRetrier.lookup(detector, SHARD_ID, SHARD_ID, 3, 0L));
        verify(detector, times(1)).shard(SHARD_ID);
    }
}
