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
import java.util.stream.LongStream;

final class SSTable implements Table {

    @NotNull
    private final FileChannel channel;
    private final int size;

    private final ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES);
    private final ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);

    SSTable(@NotNull final File file) throws IOException {
        channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        channel.read(intBuffer.rewind(), channel.size() - Integer.BYTES);
        size = intBuffer.rewind().getInt();
    }

    private long offsetForRow(final int row) throws IOException {
        if (row == 0) {
            return 0;
        }

        channel.read(longBuffer.rewind(), channel.size() - Integer.BYTES + Long.BYTES * (-size + row));
        return longBuffer.rewind().getLong();
    }

    private ByteBuffer key(long offset) throws IOException {
        channel.read(intBuffer.rewind(), offset);

        ByteBuffer key = ByteBuffer.allocate(intBuffer.rewind().getInt());
        offset += Integer.BYTES;
        channel.read(key, offset);
        return key.rewind();
    }

    private Cell cell(long offset) throws IOException {
        ByteBuffer key = key(offset);
        offset += key.remaining() + Integer.BYTES;
        channel.read(longBuffer.rewind(), offset);
        final long timestamp = longBuffer.rewind().getLong();
        offset += Long.BYTES;
        if (timestamp < 0) {
            return new Cell(key, new Value(-timestamp));
        }

        channel.read(intBuffer.rewind(), offset);
        ByteBuffer value = ByteBuffer.allocate(intBuffer.rewind().getInt());
        offset += Integer.BYTES;
        channel.read(value, offset);
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
        for (int i = 0; i < offsets.length; i++) {
            offsets[i] = offsetForRow(i + from);
        }
        return offsets;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException {
        int fromPos = binarySearch(from);
        LongStream offsets = fromPos >= size ? LongStream.empty() : Arrays.stream(offsets(fromPos));
        return offsets.mapToObj(offset -> {
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
        channel.close();
    }

    static void serialize(
            @NotNull File file,
            Iterator<Cell> iterator,
            int size) throws IOException {
        File tempFile = new File(file.toString() + "header");
        try (FileChannel headerChannel = FileChannel.open(
                tempFile.toPath(),
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.DELETE_ON_CLOSE
        )) {
            try (FileChannel writeChannel = FileChannel.open(
                    file.toPath(),
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE)) {
                int current = 0;
                long currentSize = 0;
                ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES);
                ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);
                while (iterator.hasNext() && current < size) {
                    if (current != 0) {
                        headerChannel.write(longBuffer.rewind().putLong(currentSize).rewind(), Long.BYTES * (current - 1));
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
                    current++;
                }
                headerChannel.write(intBuffer.rewind().putInt(size).rewind(), Long.BYTES * (size - 1));
                writeChannel.transferFrom(headerChannel, currentSize, Long.BYTES * (size - 1) + Integer.BYTES);
            }
        }
    }
}
