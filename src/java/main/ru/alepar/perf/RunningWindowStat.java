package ru.alepar.perf;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class RunningWindowStat implements StatPerSec {

    private static final long ONE_SECOND = 1_000_000_000L;
    public static final long MILLIS_TO_NANOS = 1_000_000L;

    private final int periodMillis;

    private final AtomicReferenceArray<Bucket> buckets;

    private final AtomicReference<State> state = new AtomicReference<>();

    public RunningWindowStat(int periodMillis, int nBuckets) {
        this.periodMillis = periodMillis;

        buckets = new AtomicReferenceArray<>(nBuckets);
    }

    @Override
    public double avg() {
        final long curtime = System.nanoTime();
        long starttime = Long.MAX_VALUE;
        long sum = 0;

        for (int i=0; i<buckets.length(); i++) {
            final Bucket bucket = buckets.get(i);

            if (bucket != null && curtime-bucket.time<ONE_SECOND) {
                if (bucket.time < starttime) {
                    starttime = bucket.time;
                }

                sum += bucket.counter.get();
            }
        }

        return sum / (double)(curtime-starttime) * ONE_SECOND;
    }

    @Override
    public void add(int i) {
        final long curtime = System.nanoTime();
        State localState = state.get();

        if(localState == null) {
            localState = new State(0);
            if(!state.compareAndSet(null, localState)) {
                localState = state.get();
            }
        }

        final int localIndex = localState.index;

        Bucket localBucket = buckets.get(localIndex);
        if (localBucket == null) {
            localBucket = new Bucket(curtime);
            if(!buckets.compareAndSet(localIndex, null, localBucket)) {
                localBucket = buckets.get(localIndex);
            }
        }

        if (localBucket.time + periodMillis*MILLIS_TO_NANOS < curtime) {
            final int nextIndex = (localIndex+1)%buckets.length();
            buckets.set(nextIndex, new Bucket(curtime));
            state.compareAndSet(localState, new State(nextIndex));
            add(i);
        } else {
            localBucket.counter.getAndAdd(i);
        }
    }

    private class State {
        private final int index;

        private State(int index) {
            this.index = index;
        }
    }

    private class Bucket {
        private final AtomicInteger counter = new AtomicInteger();
        private final long time;

        public Bucket(long time) {
            this.time = time;
        }
    }
}
