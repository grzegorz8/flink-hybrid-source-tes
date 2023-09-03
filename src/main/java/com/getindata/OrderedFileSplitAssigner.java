package com.getindata;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import java.util.TreeSet;

public class OrderedFileSplitAssigner implements FileSplitAssigner {

    private static final class FileSourceSplitComparator implements Comparator<FileSourceSplit> {

        @Override
        public int compare(FileSourceSplit o1, FileSourceSplit o2) {
            return o1.path().compareTo(o2.path());
        }
    }

    private final TreeSet<FileSourceSplit> splits;

    public OrderedFileSplitAssigner(Collection<FileSourceSplit> splits) {
        this.splits = new TreeSet<>(new FileSourceSplitComparator());
        this.splits.addAll(splits);
    }

    @Override
    public Optional<FileSourceSplit> getNext(@Nullable String hostname) {
        final int size = splits.size();
        return size == 0 ? Optional.empty() : Optional.ofNullable(splits.pollFirst());
    }

    @Override
    public void addSplits(Collection<FileSourceSplit> splits) {
        this.splits.addAll(splits);
        // TODO: we have a problem! In case of error we cannot ensure data is read more or less in order.
    }

    @Override
    public Collection<FileSourceSplit> remainingSplits() {
        return splits;
    }

    @Override
    public String toString() {
        return "OrderedFileSplitAssigner " + splits;
    }
}
