package ru.mail.polis.stasmilke;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Iterator;

final class SSTable implements Table {
    private final static ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES);
    private final static ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);

    @NotNull
    private final FileChannel channel;
    private final int size;

    SSTable(@NotNull final File file) throws IOException {
        channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        channel.read(intBuffer, channel.size() - Integer.BYTES);
        size = intBuffer.getInt();
    }

    private long offsetForRow(final int row) throws IOException {
        if (row == 0) {
            return 0;
        }
        channel.read(longBuffer, channel.size() - Integer.BYTES + Long.BYTES * (-size + row));
        return longBuffer.getLong();
    }

    private ByteBuffer key(final long offset) throws IOException {
        channel.position(offset);
        channel.read(intBuffer);
        ByteBuffer key = ByteBuffer.allocate(intBuffer.getInt());
        channel.read(key);
        return key;
    }

    private Cell cell(final long offset) throws IOException {
        ByteBuffer key = key(offset);
        channel.read(longBuffer);
        final long timestamp = longBuffer.getLong();
        if (timestamp < 0) {
            return new Cell(key, new Value(-timestamp));
        }

        channel.read(intBuffer);
        ByteBuffer value = ByteBuffer.allocate(intBuffer.getInt());
        channel.read(value);
        return new Cell(key, new Value(value, timestamp));
    }

    private int binarySearch(@NotNull final ByteBuffer from) throws IOException {
        int left = 0;
        int right = size - 1;

        while(left <= right) {
            int mid = left + right >>> 1;
            int comp = key(offsetForRow(mid)).compareTo(from);
            if (comp < 0) {
                left = mid + 1;
            } else if (comp > 0) {
                right = mid - 1;
            } else {
                return mid;
            }
        }

        return -1;
    }

    private long[] offsets() throws IOException {
        final long[] offsets = new long[size];
        offsets[0] = 0;
        for (int i = 1; i < size; i++) {
            offsets[i] = offsetForRow(i);
        }
        return offsets;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException {
        return Arrays.stream(offsets()).mapToObj(offset -> {
            try {
                return cell(offset);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).iterator();
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public long sizeInBytes() throws IOException {
        return channel.size();
    }

    @Override
    public void close() throws IOException {
        this.channel.close();
    }

    static void serialize(
            @NotNull File file,
            Iterator<Cell> iterator,
            int size) throws IOException {
        try (FileChannel writeChannel = FileChannel.open(
                file.toPath(),
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            final long[] offsets = new long[size - 1];
            int current = 0;
            long currentSize = 0;
            do {
                if (currentSize != 0) {
                    offsets[current++] = currentSize;
                }

                currentSize += Integer.BYTES * 2;
                final Cell cell = iterator.next();

                final int keySize = cell.getKey().capacity();
                writeChannel.write(intBuffer.putInt(keySize));
                writeChannel.write(cell.getKey());
                currentSize += keySize;

                long timestamp = cell.getValue().getTimestamp();
                timestamp = cell.getValue().isTombstone() ? -1 * timestamp : timestamp;
                writeChannel.write(longBuffer.putLong(timestamp));

                if (!cell.getValue().isTombstone()) {
                    final int valueSize = cell.getValue().getData().capacity();
                    writeChannel.write(intBuffer.putInt(valueSize));
                    writeChannel.write(cell.getValue().getData());
                    currentSize += valueSize + Integer.BYTES;
                }
            } while (iterator.hasNext());
            for (long offset : offsets) {
                writeChannel.write(longBuffer.putLong(offset));
            }
            writeChannel.write(intBuffer.putInt(size));
        }
    }
}
