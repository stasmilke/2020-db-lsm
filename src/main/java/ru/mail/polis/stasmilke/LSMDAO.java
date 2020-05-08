package ru.mail.polis.stasmilke;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.stream.Stream;

public class LSMDAO implements DAO {
    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".temp";

    @NotNull
    private final File storage;
    private final long flushThreshold;

    private Table memTable;
    private final NavigableMap<Integer, Table> ssTables;
    private int generation;

    public LSMDAO(
            @NotNull final File storage,
            final long flushThreshold) throws IOException {
        this.storage = storage;
        assert flushThreshold > 0L;
        this.flushThreshold = flushThreshold;
        this.memTable = new MemTable();
        this.ssTables = new TreeMap<>();
        try(final Stream<Path> files = Files.list(storage.toPath())) {
            files.filter(path -> path.toString().endsWith(SUFFIX)).forEach(file -> {
                try {
                    final String name = file.getFileName().toString();
                    final int generation = Integer.parseInt(name.substring(0, name.indexOf(SUFFIX)));
                    this.generation = Math.max(this.generation, generation);
                    ssTables.put(generation, new SSTable(file.toFile()));
                } catch (Exception e) {
                    //bad file
                }
            });
        }
        generation++;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> iterators = new ArrayList<>(ssTables.size() + 1);
        iterators.add(memTable.iterator(from));
        ssTables.descendingMap().values().forEach(t -> {
            try {
                iterators.add(t.iterator(from));
            } catch (IOException e) {
                throw new RuntimeException("Error", e);
            }
        });
        // Sorted duplicates and tombstones
        final Iterator<Cell> merged = Iterators.mergeSorted(iterators, Cell.COMPARATOR);
        // One cell per key
        final Iterator<Cell> fresh = Iters.collapseEquals(merged, Cell::getKey);
        // Removed tombstones
        final Iterator<Cell> alive = Iterators.filter(fresh, e -> !e.getValue().isTombstone());
        return Iterators.transform(alive, e -> Record.of(e.getKey(), e.getValue().getData()));
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    private void flush() throws IOException {
        final File file = new File(storage, generation + TEMP);
        SSTable.serialize(
                file,
                memTable.iterator(ByteBuffer.allocate(0)),
                memTable.size());
        final File dst = new File(storage, generation + SUFFIX);
        Files.move(file.toPath(), dst.toPath(), StandardCopyOption.ATOMIC_MOVE);
        memTable = new MemTable();
        ssTables.put(generation, new SSTable(dst));
        generation++;
    }

    @Override
    public void close() throws IOException {
        if (memTable.size() > 0) {
            flush();
        }
        for (Map.Entry<Integer, Table> entry : ssTables.entrySet()) {
            entry.getValue().close();
        }
    }
}
