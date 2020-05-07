package ru.mail.polis.stasmilke;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

final class MemTable implements Table {
    private final SortedMap<ByteBuffer, Value> sortedMap = new TreeMap<>();
    private long sizeInBytes = 0L;

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
        return Iterators.transform(sortedMap.tailMap(from).entrySet().iterator(),
                x -> new Cell(x.getKey(), x.getValue()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        final Value oldValue = sortedMap.get(key);
        final Value newValue = new Value(value, System.currentTimeMillis());
        if (oldValue != null) {
            sizeInBytes -= oldValue.sizeInBytes();
        } else {
            sizeInBytes += key.remaining();
        }
        sizeInBytes += newValue.sizeInBytes();
        sortedMap.put(key, newValue);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        final Value oldValue = sortedMap.get(key);
        final Value newValue = new Value(System.currentTimeMillis());
        if (oldValue != null) {
            sizeInBytes -= oldValue.sizeInBytes();
        }
        sizeInBytes += newValue.sizeInBytes();
        sortedMap.put(key, newValue);
    }

    @Override
    public int size() {
        return sortedMap.size();
    }

    @Override
    public long sizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public void close() throws IOException {
        //Do nothing
    }
}
