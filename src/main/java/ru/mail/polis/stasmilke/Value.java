package ru.mail.polis.stasmilke;

import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Optional;

final class Value implements Comparable<Value> {
    @Nullable
    private final Optional<ByteBuffer> data;
    private final long timestamp;

    Value(@Nullable ByteBuffer data, long timestamp) {
        assert timestamp > 0L;
        this.data = Optional.of(data);
        this.timestamp = timestamp;
    }

    Value(long timestamp) {
        assert timestamp > 0L;
        this.data = Optional.empty();
        this.timestamp = timestamp;
    }

    boolean isTombstone() {
        return data.isEmpty();
    }

    @NotNull
    ByteBuffer getData() {
        assert !isTombstone();
        return data.orElseThrow().asReadOnlyBuffer();
    }

    @Override
    public int compareTo(@NotNull Value o) {
        return -Long.compare(timestamp, o.timestamp);
    }

    public long sizeInBytes() {
        return Long.BYTES + (data.isPresent() ? data.get().remaining() : 0L);
    }

    public long getTimestamp() {
        return timestamp;
    }
}
