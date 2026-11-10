package io.github.yq808.taskqueue.it;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** A fresh directory for one test class's H2 files, so test contexts never share a database. */
public final class TestDataDir {

    private TestDataDir() {
    }

    public static String create(String name) {
        try {
            Path dir = Files.createTempDirectory("taskqueue-it-" + name + "-");
            return dir.toAbsolutePath().toString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
