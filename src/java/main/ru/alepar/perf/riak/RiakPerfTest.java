package ru.alepar.perf.riak;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import org.HdrHistogram.AtomicHistogram;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.alepar.perf.generator.JobGenerator;
import ru.alepar.perf.generator.TpsGenerator;
import ru.alepar.perf.generator.TpsGeneratorStatPrinter;
import ru.alepar.perf.generator.UnthrottledTpsGenerator;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.concurrent.ThreadLocalRandom;

public class RiakPerfTest {

    private static final Logger log = LoggerFactory.getLogger(RiakPerfTest.class);

    public static void main(String[] args) throws Exception {
        final String hostname = args[0];
        final long nKeys = Long.valueOf(args[1]);
        final int nThreads = Integer.valueOf(args[2]);
        final int r = Integer.valueOf(args[3]);

        log.info("hostname {}; nKeys {}; nThreads {}; R {}", hostname, nKeys, nThreads, r);
        final RiakClient client = RiakClient.newClient(args[0].split("\\s*,\\s*"));
        final Namespace bucket = new Namespace("default", "my_bucket");

        final long warmupStart = System.nanoTime();
        final long interval = (nKeys / nThreads) + 1;
        final BinaryValue value = BinaryValue.create("my_value");
        final Thread[] warmupThreads = new Thread[nThreads];
        for(int i=0; i<nThreads; i++) {
            final int ii = i;
            warmupThreads[i] = new Thread(() -> {
                for(long k=ii*interval; k<ii*interval+interval; k++) {
                    final RiakObject storeObj = new RiakObject();

                    storeObj.setValue(value);
                    final Location key = new Location(bucket, "my_key"+k);
                    final StoreValue store = new StoreValue.Builder(storeObj)
                            .withLocation(key)
                            .build();
                    try {
                        client.execute(store);
                    } catch (Exception e) {
                        log.warn("failed to warmup-write key", e);
                    }
                }
            });
            warmupThreads[i].start();
        }
        for(int i=0; i<nThreads; i++) {
            warmupThreads[i].join();
        }
        log.info("warmup finished in {} seconds", (System.nanoTime()-warmupStart)/100_000_000/10.0);

        final Histogram histogram = new AtomicHistogram(1_000L, 10_000_000_000L, 4);
        final JobGenerator readJobGenerator = new RiakReadJobGenerator(client, bucket, nKeys, r, histogram);

        final TpsGenerator.Listener statPrinter = new TpsGeneratorStatPrinter(System.out);
        final TpsGenerator tpsGenerator = new UnthrottledTpsGenerator(readJobGenerator, nThreads, statPrinter);
        tpsGenerator.start();

        while(true) {
            Thread.sleep(1000L);
            try(PrintStream out = new PrintStream(new FileOutputStream("latency.hdr"))) {
                histogram.copy().outputPercentileDistribution(out, 10.);
            }
        }
    }

    private static class RiakReadJobGenerator implements JobGenerator {

        private final Runnable runnable;

        public RiakReadJobGenerator(RiakClient client, Namespace bucket, long nKeys, int r, Histogram histogram) {
            runnable = () -> {
                try {
                    final Location key = new Location(bucket, "my_key"+ThreadLocalRandom.current().nextLong(nKeys));
                    final FetchValue fetch = new FetchValue.Builder(key)
                            .withOption(FetchValue.Option.PR, new Quorum(r))
                            .withOption(FetchValue.Option.R, new Quorum(r))
                            .build();
                    final long start = System.nanoTime();
                    final FetchValue.Response response = client.execute(fetch);
                    final long latency = System.nanoTime() - start;
                    histogram.recordValue(latency);
                    final RiakObject fetchObj = response.getValue(RiakObject.class);
                    if (!"my_value".equals(fetchObj.getValue().toString())) {
                        throw new RuntimeException("assert failed; wrong value read: " + fetchObj.getValue().toString());
                    }
                } catch (Exception e) {
                    throw new RuntimeException("failed to execute read", e);
                }
            };
        }

        @Override
        public Runnable next() {
            return runnable;
        }
    }
}
