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

    @NotNull
    private final FileChannel channel;
    private final int size;

    SSTable(@NotNull final File file) throws IOException {
        channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        ByteBuffer sizeBuffer = ByteBuffer.allocate(Integer.BYTES);
        channel.read(sizeBuffer, channel.size() - Integer.BYTES);
        size = sizeBuffer.rewind().getInt();
    }

    private long offsetForRow(final int row) throws IOException {
        if (row == 0) {
            return 0;
        }

        ByteBuffer sizeBuffer = ByteBuffer.allocate(Long.BYTES);
        channel.read(sizeBuffer, channel.size() - Integer.BYTES + Long.BYTES * (-size + row));
        return sizeBuffer.rewind().getLong();
    }

    private ByteBuffer key(final long offset) throws IOException {
        channel.position(offset);
        ByteBuffer sizeBuffer = ByteBuffer.allocate(Integer.BYTES);
        channel.read(sizeBuffer.rewind());
        int size = sizeBuffer.rewind().getInt();
        ByteBuffer key = ByteBuffer.allocate(size);
        channel.read(key);
        return key.rewind();
    }

    private Cell cell(final long offset) throws IOException {
        ByteBuffer key = key(offset);
        ByteBuffer timestampBuffer = ByteBuffer.allocate(Long.BYTES);
        channel.read(timestampBuffer);
        final long timestamp = timestampBuffer.rewind().getLong();
        if (timestamp < 0) {
            return new Cell(key, new Value(-timestamp));
        }

        ByteBuffer sizeBuffer = ByteBuffer.allocate(Integer.BYTES);
        channel.read(sizeBuffer);
        ByteBuffer value = ByteBuffer.allocate(sizeBuffer.rewind().getInt());
        channel.read(value);
        return new Cell(key, new Value(value.rewind(), timestamp));
    }

    private int binarySearch(@NotNull final ByteBuffer from) throws IOException {
        int left = 0;
        int right = size - 1;

        while(left <= right) {
            int mid = (left + right) / 2;
            int comp = from.compareTo(key(offsetForRow(mid)));
            if (comp < 0) {
                right = mid - 1;
            } else if (comp > 0) {
                left = mid + 1;
            } else {
                return mid;
            }
        }

        return left;
    }

    private long[] offsets(int from) throws IOException {
        assert from < size;
        final long[] offsets = new long[size - from];
        if (from == 0) {
            offsets[0] = 0;
        }
        for (int i = 0; i < offsets.length; i++) {
            offsets[i] = offsetForRow(i + from);
        }
        return offsets;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException {
        int fromPos = binarySearch(from);
        return Arrays.stream(offsets(fromPos)).mapToObj(offset -> {
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
            ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES);
            ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);
            while (iterator.hasNext()) {
                if (currentSize != 0) {
                    offsets[current++] = currentSize;
                }

                final Cell cell = iterator.next();

                final int keySize = cell.getKey().remaining();
                writeChannel.write(intBuffer.rewind().putInt(keySize).rewind(), currentSize);
                currentSize += Integer.BYTES;
                writeChannel.write(cell.getKey(), currentSize);
                currentSize += keySize;

                long timestamp =  cell.getValue().getTimestamp() * (cell.getValue().isTombstone() ? -1 : 1);
                writeChannel.write(longBuffer.rewind().putLong(timestamp).rewind(), currentSize);
                currentSize += Long.BYTES;

                if (!cell.getValue().isTombstone()) {
                    final int valueSize = cell.getValue().getData().remaining();
                    writeChannel.write(intBuffer.rewind().putInt(valueSize).rewind(), currentSize);
                    currentSize += Integer.BYTES;
                    writeChannel.write(cell.getValue().getData(), currentSize);
                    currentSize += valueSize;
                }
            }
            for (long offset : offsets) {
                writeChannel.write(longBuffer.rewind().putLong(offset).rewind(), currentSize);
                currentSize += Long.BYTES;
            }
            writeChannel.write(intBuffer.rewind().putInt(size).rewind(), currentSize);
        }
    }
}
