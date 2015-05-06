package ru.alepar.perf.generator;

public class UnthrottledTpsGenerator implements TpsGenerator {

    private final JobGenerator jobGenerator;
    private final Listener listener;
    private final Thread[] threads;

    public UnthrottledTpsGenerator(JobGenerator jobGenerator, int nThreads, TpsGenerator.Listener listener) {
        this.jobGenerator = jobGenerator;
        this.listener = listener;

        final Runnable job = new Job();

        threads = new Thread[nThreads];
        for (int i=0; i<nThreads; i++) {
            threads[i] = new Thread(job);
        }
    }

    @Override
    public void start() {
        for (Thread thread : threads) {
            thread.start();
        }
    }

    private class Job implements Runnable {
        @Override
        public void run() {
            while (true) {
                final Runnable runnable = jobGenerator.next();
                try {
                    runnable.run();
                    listener.onSuccess();
                } catch (Exception e) {
                    listener.onError(e);
                }
            }
        }
    }
}
