package ru.alepar.perf.riak;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import org.HdrHistogram.AtomicHistogram;
import org.HdrHistogram.Histogram;
import ru.alepar.perf.generator.JobGenerator;
import ru.alepar.perf.generator.TpsGenerator;
import ru.alepar.perf.generator.TpsGeneratorStatPrinter;
import ru.alepar.perf.generator.UnthrottledTpsGenerator;

import java.io.FileOutputStream;
import java.io.PrintStream;

public class RiakPerfTest {

    public static void main(String[] args) throws Exception {
        final RiakClient client = RiakClient.newClient("127.0.0.1");
        final Namespace bucket = new Namespace("default", "my_bucket");
        final Location key = new Location(bucket, "my_key");

        final Histogram histogram = new AtomicHistogram(1_000L, 1_000_000_000L, 4);
        final JobGenerator readJobGenerator = new RiakReadJobGenerator(client, key, histogram);

        final TpsGenerator.Listener statPrinter = new TpsGeneratorStatPrinter(System.out);
        final TpsGenerator tpsGenerator = new UnthrottledTpsGenerator(readJobGenerator, 8, statPrinter);
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

        public RiakReadJobGenerator(RiakClient client, Location key, Histogram histogram) {
            runnable = () -> {
                try {
                    final FetchValue fetch = new FetchValue.Builder(key).build();
                    final long start = System.nanoTime();
                    final FetchValue.Response response = client.execute(fetch);
                    histogram.recordValue(System.nanoTime() - start);
                    final RiakObject fetchObj = response.getValue(RiakObject.class);
                    if (!"my_value".equals(fetchObj.getValue().toString())) {
                        throw new RuntimeException("assert failed; wrong value read: " + fetchObj.getValue().toString());
                    }
                } catch (Exception e) {
                    throw new RuntimeException("failed to execute read");
                }
            };
        }

        @Override
        public Runnable next() {
            return runnable;
        }
    }
}
