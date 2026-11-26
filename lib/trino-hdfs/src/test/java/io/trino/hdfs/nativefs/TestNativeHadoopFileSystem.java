/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.hdfs.nativefs;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestNativeHadoopFileSystem
{
    private String registryId;
    private TrinoFileSystem nativeFileSystem;
    private Configuration configuration;
    private FileSystem fileSystem;

    @BeforeEach
    void setUp()
            throws IOException
    {
        registryId = "test-" + UUID.randomUUID();
        MemoryFileSystemFactory factory = new MemoryFileSystemFactory();
        nativeFileSystem = factory.create(ConnectorIdentity.ofUser("test"));
        NativeFileSystemRegistry.register(registryId, Map.of("memory", factory));

        configuration = new Configuration(false);
        new NativeFileSystemConfigurationInitializer(registryId).initializeConfiguration(configuration);

        fileSystem = new NativeHadoopFileSystem();
        fileSystem.initialize(URI.create("memory:///"), configuration);
    }

    @AfterEach
    void tearDown()
            throws IOException
    {
        fileSystem.close();
        NativeFileSystemRegistry.unregister(registryId);
    }

    @Test
    void testConfigurationInitializer()
    {
        assertThat(configuration.get("fs.memory.impl")).isEqualTo(NativeHadoopFileSystem.class.getName());
        assertThat(configuration.get(NativeHadoopFileSystem.REGISTRY_ID_KEY)).isEqualTo(registryId);
        // Only schemes with a registered native file system are overridden
        assertThat(configuration.get("fs.s3a.impl")).isNull();
    }

    @Test
    void testInitializeWithUnregisteredScheme()
    {
        assertThatThrownBy(() -> new NativeHadoopFileSystem().initialize(URI.create("s3a://bucket/"), configuration))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("No native file system registered for scheme: s3a");
    }

    @Test
    void testInitializeWithoutRegistryId()
    {
        assertThatThrownBy(() -> new NativeHadoopFileSystem().initialize(URI.create("memory:///"), new Configuration(false)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(NativeHadoopFileSystem.REGISTRY_ID_KEY);
    }

    @Test
    void testOpen()
            throws IOException
    {
        writeNative("memory:///data/file.txt", "hello world");

        try (FSDataInputStream input = fileSystem.open(new Path("memory:///data/file.txt"))) {
            assertThat(new String(input.readAllBytes(), UTF_8)).isEqualTo("hello world");
        }
        try (FSDataInputStream input = fileSystem.open(new Path("memory:///data/file.txt"))) {
            input.seek(6);
            assertThat(input.getPos()).isEqualTo(6);
            assertThat(new String(input.readAllBytes(), UTF_8)).isEqualTo("world");
            // positioned reads do not move the stream position
            byte[] buffer = new byte[5];
            input.readFully(0, buffer);
            assertThat(new String(buffer, UTF_8)).isEqualTo("hello");
        }
    }

    @Test
    void testOpenMissingFile()
    {
        assertThatThrownBy(() -> {
            try (FSDataInputStream input = fileSystem.open(new Path("memory:///data/missing.txt"))) {
                input.read();
            }
        }).isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void testGetFileStatus()
            throws IOException
    {
        writeNative("memory:///data/file.txt", "hello");

        FileStatus file = fileSystem.getFileStatus(new Path("memory:///data/file.txt"));
        assertThat(file.isFile()).isTrue();
        assertThat(file.getLen()).isEqualTo(5);
        assertThat(file.getPath().getName()).isEqualTo("file.txt");

        FileStatus directory = fileSystem.getFileStatus(new Path("memory:///data"));
        assertThat(directory.isDirectory()).isTrue();

        assertThat(fileSystem.getFileStatus(new Path("memory:///")).isDirectory()).isTrue();

        assertThatThrownBy(() -> fileSystem.getFileStatus(new Path("memory:///missing")))
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void testListStatus()
            throws IOException
    {
        writeNative("memory:///data/a.txt", "a");
        writeNative("memory:///data/b.txt", "bb");
        writeNative("memory:///data/sub/c.txt", "ccc");

        // direct children only: files and subdirectories, but not files in subdirectories
        FileStatus[] statuses = fileSystem.listStatus(new Path("memory:///data"));
        assertThat(Arrays.stream(statuses).filter(FileStatus::isFile).map(status -> status.getPath().getName()))
                .containsExactlyInAnyOrder("a.txt", "b.txt");
        assertThat(Arrays.stream(statuses).filter(FileStatus::isDirectory).map(status -> status.getPath().getName()))
                .containsExactly("sub");

        // listing a file returns the file itself
        assertThat(fileSystem.listStatus(new Path("memory:///data/b.txt")))
                .singleElement()
                .satisfies(status -> assertThat(status.getLen()).isEqualTo(2));

        assertThatThrownBy(() -> fileSystem.listStatus(new Path("memory:///missing")))
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void testListFilesRecursive()
            throws IOException
    {
        writeNative("memory:///data/a.txt", "a");
        writeNative("memory:///data/sub/c.txt", "ccc");

        assertThat(listFiles(fileSystem, new Path("memory:///data")))
                .containsExactlyInAnyOrder("a.txt", "c.txt");
    }

    @Test
    void testCreate()
            throws IOException
    {
        Path path = new Path("memory:///data/created.txt");
        try (FSDataOutputStream output = fileSystem.create(path, false)) {
            output.write("first".getBytes(UTF_8));
        }
        assertThat(readNative("memory:///data/created.txt")).isEqualTo("first");

        assertThatThrownBy(() -> fileSystem.create(path, false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("File already exists");

        try (FSDataOutputStream output = fileSystem.create(path, true)) {
            output.write("second".getBytes(UTF_8));
        }
        assertThat(readNative("memory:///data/created.txt")).isEqualTo("second");
    }

    @Test
    void testAppend()
    {
        assertThatThrownBy(() -> fileSystem.append(new Path("memory:///data/file.txt")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Append is not supported");
    }

    @Test
    void testRename()
            throws IOException
    {
        writeNative("memory:///data/source.txt", "content");

        assertThat(fileSystem.rename(new Path("memory:///data/source.txt"), new Path("memory:///data/target.txt"))).isTrue();

        assertThat(nativeFileSystem.newInputFile(Location.of("memory:///data/source.txt")).exists()).isFalse();
        assertThat(readNative("memory:///data/target.txt")).isEqualTo("content");
    }

    @Test
    void testDelete()
            throws IOException
    {
        writeNative("memory:///data/a.txt", "a");
        writeNative("memory:///data/sub/b.txt", "b");

        assertThat(fileSystem.delete(new Path("memory:///data/a.txt"), false)).isTrue();
        assertThat(nativeFileSystem.newInputFile(Location.of("memory:///data/a.txt")).exists()).isFalse();

        assertThatThrownBy(() -> fileSystem.delete(new Path("memory:///data"), false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Directory is not empty");

        assertThat(fileSystem.delete(new Path("memory:///data"), true)).isTrue();
        assertThat(nativeFileSystem.newInputFile(Location.of("memory:///data/sub/b.txt")).exists()).isFalse();
    }

    @Test
    void testViewFsMountLink()
            throws IOException
    {
        // ViewFS resolves the mount link target by scheme, using fs.memory.impl set by the initializer
        writeNative("memory:///mounted/a.txt", "a");
        writeNative("memory:///mounted/sub/b.txt", "bb");

        Configuration viewFsConfiguration = new Configuration(configuration);
        viewFsConfiguration.set("fs.viewfs.impl", "org.apache.hadoop.fs.viewfs.ViewFileSystem");
        viewFsConfiguration.set("fs.viewfs.mounttable.cluster.link./data", "memory:///mounted");

        try (FileSystem viewFs = FileSystem.newInstance(URI.create("viewfs://cluster/"), viewFsConfiguration)) {
            Path data = new Path("viewfs://cluster/data");

            assertThat(Arrays.stream(viewFs.listStatus(data)).map(status -> status.getPath().getName()))
                    .containsExactlyInAnyOrder("a.txt", "sub");
            assertThat(listFiles(viewFs, data)).containsExactlyInAnyOrder("a.txt", "b.txt");

            try (FSDataInputStream input = viewFs.open(new Path("viewfs://cluster/data/sub/b.txt"))) {
                assertThat(new String(input.readAllBytes(), UTF_8)).isEqualTo("bb");
            }
        }
    }

    private static List<String> listFiles(FileSystem fileSystem, Path path)
            throws IOException
    {
        List<String> names = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(path, true);
        while (files.hasNext()) {
            names.add(files.next().getPath().getName());
        }
        return names;
    }

    private void writeNative(String location, String content)
            throws IOException
    {
        nativeFileSystem.newOutputFile(Location.of(location)).createOrOverwrite(content.getBytes(UTF_8));
    }

    private String readNative(String location)
            throws IOException
    {
        try (var input = nativeFileSystem.newInputFile(Location.of(location)).newStream()) {
            return new String(input.readAllBytes(), UTF_8);
        }
    }
}
