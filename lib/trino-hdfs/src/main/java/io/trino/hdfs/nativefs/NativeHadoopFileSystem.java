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

import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Hadoop {@link FileSystem} backed by a native Trino {@link TrinoFileSystem}.
 * <p>
 * Hadoop file systems that resolve child file systems inside the Hadoop client, such as ViewFS
 * mount links to {@code s3a://}, instantiate the child by scheme using {@code fs.<scheme>.impl}.
 * {@link NativeFileSystemConfigurationInitializer} registers this class for the schemes that
 * have a native Trino file system, and the native file system factory is looked up in
 * {@link NativeFileSystemRegistry} using the id stored in the {@link Configuration}.
 */
public class NativeHadoopFileSystem
        extends FileSystem
{
    public static final String REGISTRY_ID_KEY = "trino.native-file-system.registry-id";

    // Native file systems do not expose a block size; use a value suitable for split planning
    private static final long BLOCK_SIZE = 64 * 1024 * 1024;

    private URI uri;
    private Path workingDirectory;
    private TrinoFileSystem fileSystem;

    @Override
    public void initialize(URI name, Configuration conf)
            throws IOException
    {
        super.initialize(name, conf);
        setConf(conf);
        // Locations without an authority, such as memory:///path, are rooted at scheme:///
        uri = name.getAuthority() == null ? URI.create(name.getScheme() + ":///") : URI.create(name.getScheme() + "://" + name.getAuthority());
        workingDirectory = new Path(uri.getAuthority() == null ? uri.toString() : uri + "/");

        String registryId = conf.get(REGISTRY_ID_KEY);
        if (registryId == null) {
            throw new IOException("Native file system is not configured: %s is not set".formatted(REGISTRY_ID_KEY));
        }
        TrinoFileSystemFactory factory = NativeFileSystemRegistry.get(registryId, name.getScheme())
                .orElseThrow(() -> new IOException("No native file system registered for scheme: " + name.getScheme()));
        fileSystem = factory.create(ConnectorIdentity.ofUser(UserGroupInformation.getCurrentUser().getShortUserName()));
    }

    @Override
    public String getScheme()
    {
        return uri.getScheme();
    }

    @Override
    public URI getUri()
    {
        return uri;
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize)
            throws IOException
    {
        TrinoInputFile inputFile = fileSystem.newInputFile(location(path));
        return new FSDataInputStream(new NativeHadoopInputStream(inputFile.newStream()));
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        Location location = location(path);
        if (isFile(location)) {
            if (!overwrite) {
                throw new IOException("File already exists: " + path);
            }
            fileSystem.deleteFile(location);
        }
        TrinoOutputFile outputFile = fileSystem.newOutputFile(location);
        return new FSDataOutputStream(outputFile.create(), statistics);
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progress)
            throws IOException
    {
        throw new IOException("Append is not supported: " + path);
    }

    @Override
    public boolean rename(Path source, Path target)
            throws IOException
    {
        fileSystem.renameFile(location(source), location(target));
        return true;
    }

    @Override
    public boolean delete(Path path, boolean recursive)
            throws IOException
    {
        Location location = location(path);
        if (isFile(location)) {
            fileSystem.deleteFile(location);
            return true;
        }
        if (!recursive && fileSystem.listFiles(location).hasNext()) {
            throw new IOException("Directory is not empty: " + path);
        }
        fileSystem.deleteDirectory(location);
        return true;
    }

    @Override
    public FileStatus[] listStatus(Path path)
            throws IOException
    {
        Location location = location(path);
        if (isFile(location)) {
            TrinoInputFile inputFile = fileSystem.newInputFile(location);
            return new FileStatus[] {fileStatus(location, inputFile.length(), inputFile.lastModified())};
        }

        List<FileStatus> statuses = new ArrayList<>();
        for (Location directory : fileSystem.listDirectories(location)) {
            statuses.add(directoryStatus(directory));
        }
        // Compare paths, as the native file system may format the same location differently than Hadoop
        String parent = withoutTrailingSlash(location.path());
        FileIterator files = fileSystem.listFiles(location);
        while (files.hasNext()) {
            FileEntry file = files.next();
            if (withoutTrailingSlash(file.location().parentDirectory().path()).equals(parent)) {
                statuses.add(fileStatus(file.location(), file.length(), file.lastModified()));
            }
        }
        if (statuses.isEmpty() && !isDirectory(location)) {
            throw new FileNotFoundException("File does not exist: " + path);
        }
        return statuses.toArray(FileStatus[]::new);
    }

    @Override
    public void setWorkingDirectory(Path path)
    {
        workingDirectory = path;
    }

    @Override
    public Path getWorkingDirectory()
    {
        return workingDirectory;
    }

    @Override
    public boolean mkdirs(Path path, FsPermission permission)
            throws IOException
    {
        fileSystem.createDirectory(location(path));
        return true;
    }

    @Override
    public FileStatus getFileStatus(Path path)
            throws IOException
    {
        Location location = location(path);
        if (isFile(location)) {
            TrinoInputFile inputFile = fileSystem.newInputFile(location);
            return fileStatus(location, inputFile.length(), inputFile.lastModified());
        }
        if (isDirectory(location)) {
            return directoryStatus(location);
        }
        throw new FileNotFoundException("File does not exist: " + path);
    }

    private boolean isFile(Location location)
            throws IOException
    {
        return !location.path().isEmpty() && fileSystem.newInputFile(location).exists();
    }

    private boolean isDirectory(Location location)
            throws IOException
    {
        // Object stores report an empty optional for prefixes without objects, treat them as missing
        return location.path().isEmpty() || fileSystem.directoryExists(location).orElse(false);
    }

    private Location location(Path path)
    {
        return Location.of(makeQualified(path).toString());
    }

    private static Path toPath(Location location)
    {
        return new Path(location.toString());
    }

    private static String withoutTrailingSlash(String path)
    {
        return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
    }

    private static FileStatus fileStatus(Location location, long length, Instant lastModified)
    {
        return new FileStatus(length, false, 1, BLOCK_SIZE, lastModified.toEpochMilli(), toPath(location));
    }

    private static FileStatus directoryStatus(Location location)
    {
        return new FileStatus(0, true, 1, 0, 0, toPath(location));
    }
}
