package com.trans.test.transformautomation.tranforms;

import com.trans.test.transformautomation.model.UserClicks;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class TableCheckNullValue implements TransformerSupplier<String, String, KeyValue<String, UserClicks>> {

    private final String regionStoreName;

    public TableCheckNullValue(final String regionStoreName) {
        this.regionStoreName = regionStoreName;
    }

    @Override
    public Transformer<String, String, KeyValue<String, UserClicks>> get() {
        return new Transformer<String, String, KeyValue<String, UserClicks>>() {

            private KeyValueStore<String, ValueAndTimestamp<String>> regionStore;

            @SuppressWarnings("unchecked")
            @Override
            public void init(final ProcessorContext context) {
                regionStore = (KeyValueStore<String, ValueAndTimestamp<String>>) context.getStateStore(regionStoreName);
            }

            @Override
            public KeyValue<String, UserClicks> transform(final String firstname, final String city) {
                int id = Integer.parseInt(regionStore.get(firstname).value());
                final String lastname = regionStore.get(firstname).value();
                System.out.println("TRANSFORM => " + firstname + ": " + firstname + " - " + lastname);
                return KeyValue.pair(firstname, new UserClicks(id, firstname, lastname, city));
            }

            @Override
            public void close() {
            }
        };
    }
}