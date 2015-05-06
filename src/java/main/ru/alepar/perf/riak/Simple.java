package ru.alepar.perf.riak;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;

public class Simple {

    public static void main(String[] args) throws Exception {
        final RiakClient client = RiakClient.newClient("52.6.197.138");

        try {
            final Namespace ns = new Namespace("default", "my_bucket");
            final Location location = new Location(ns, "my_key");

            final RiakObject storeObj = new RiakObject();
            storeObj.setValue(BinaryValue.create("my_value"));
            final StoreValue store = new StoreValue.Builder(storeObj)
                    .withLocation(location)
    //                .withOption(StoreValue.Option.W, new Quorum(3))
                    .build();
            client.execute(store);

            final FetchValue fetch = new FetchValue.Builder(location).build();
            final FetchValue.Response response = client.execute(fetch);
            final RiakObject fetchObj = response.getValue(RiakObject.class);

            System.out.println(fetchObj.getValue().toString());
        } finally {
            client.shutdown();
        }

    }

}
