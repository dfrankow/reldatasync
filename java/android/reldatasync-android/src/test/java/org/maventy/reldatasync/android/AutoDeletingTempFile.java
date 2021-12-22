package org.maventy.reldatasync.android;

import java.io.File;
import java.io.IOException;

/**
 * Implements AutoCloseable.
 *
 * @see https://stackoverflow.com/a/34050507
 */
public class AutoDeletingTempFile implements AutoCloseable {
    private final File file;

    public AutoDeletingTempFile(String prefix, String suffix) throws IOException {
        file = File.createTempFile(prefix, suffix);
        // This delays deleting for awhile, but why not.
        file.deleteOnExit();
    }

    public File getFile() {
        return file;
    }

    @Override
    public void close() throws IOException {
    }
}
