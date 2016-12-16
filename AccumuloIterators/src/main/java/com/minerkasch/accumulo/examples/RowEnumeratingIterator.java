package com.minerkasch.accumulo.examples;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;


public class RowEnumeratingIterator implements SortedKeyValueIterator<Key, Value> {


    // The previous row's ID
    private Text prevRowId = null;

    private SortedKeyValueIterator<Key,Value> source = null;

    protected void setSource(SortedKeyValueIterator<Key,Value> source) {
        this.source = source;
    }

    @Override
    public void init(SortedKeyValueIterator sortedKeyValueIterator, Map map, IteratorEnvironment iteratorEnvironment) throws IOException {
        this.setSource(sortedKeyValueIterator);
    }

    @Override
    public boolean hasTop() {
        return this.source.hasTop();
    }

    @Override
    public void next() throws IOException {
        this.source.next();

        findTop();
    }

    @Override
    public Key getTopKey() {
        return this.source.getTopKey();
    }

    @Override
    public Value getTopValue() {
        return this.source.getTopValue();
    }

    @Override
    public SortedKeyValueIterator deepCopy(IteratorEnvironment iteratorEnvironment) {
        return null;
    }

    @Override
    public void seek(Range range, Collection collection, boolean b) throws IOException {
        this.source.seek(range, collection, b);

        findTop();
    }

    private void findTop() throws IOException {
        // Continue to call the source iterator's next while the source hasTop,
        // the source top key is not a delete key, and the current key's rowID
        // != prevRowId
        while (this.source.hasTop() && !this.source.getTopKey().isDeleted()
                && this.source.getTopKey().getRow().equals(prevRowId)) {
            this.source.next();
        }

        // Set our previous rowID to the source's top key rowID
        if (this.source.hasTop()) {
            prevRowId = this.source.getTopKey().getRow();
        }
    }
}
