package com.minerkasch.accumulo.examples;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.Map;


public class CustomFilteringIterator extends Filter {

    public static final String FILTER_VALUE = "com.minerkasch.accumulo.filter.value";


    private String filterValue = null;


    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);

        if (options.containsKey(FILTER_VALUE)) {
            filterValue = options.get(FILTER_VALUE).toLowerCase();
        }

    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        CustomFilteringIterator copy = (CustomFilteringIterator) super.deepCopy(env);

        copy.filterValue = this.filterValue;

        return copy;
    }

    @Override
    public IteratorOptions describeOptions() {

        // Get the parent iterator's iterator options
        IteratorOptions iterOpts = super.describeOptions();

        // Set the iterators description
        iterOpts.setDescription("The CustomFilteringIterator allows values to be filtered");

        // Add options to allow min and max value to be specified
        iterOpts.addNamedOption(CustomFilteringIterator.FILTER_VALUE,
                "value allowed (default is null)");


        // Return the iterator's options
        return iterOpts;
    }

    @Override
    public boolean accept(Key key, Value value) {
        return this.filterValue != null && value.toString().toLowerCase().contains(this.filterValue);
    }
}
