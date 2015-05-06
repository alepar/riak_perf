package ru.alepar.perf;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;

public class RiakPerfTest {

    public static void main(String[] args) throws Exception {
        final RiakClient client = RiakClient.newClient("52.6.221.69");
        final Namespace bucket = new Namespace("default", "my_bucket");
        final Location key = new Location(bucket, "my_key");
        final JobGenerator readJobGenerator = new RiakReadJobGenerator(client, key);

        final TpsGenerator.Listener statPrinter = new TpsGeneratorStatPrinter(System.out);
        final TpsGenerator tpsGenerator = new UnthrottledTpsGenerator(readJobGenerator, 256, statPrinter);
        tpsGenerator.start();
    }

    private static class RiakReadJobGenerator implements JobGenerator {

        private final Runnable runnable;

        public RiakReadJobGenerator(RiakClient client, Location key) {
            runnable = () -> {
                try {
                    final FetchValue fetch = new FetchValue.Builder(key).build();
                    final FetchValue.Response response = client.execute(fetch);
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
