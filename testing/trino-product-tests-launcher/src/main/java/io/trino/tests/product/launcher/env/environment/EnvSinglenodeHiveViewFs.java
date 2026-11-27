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
package io.trino.tests.product.launcher.env.environment;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.docker.DockerFiles.ResourceProvider;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.Minio;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

import static io.trino.tests.product.launcher.env.common.Minio.MINIO_CONTAINER_NAME;
import static org.testcontainers.utility.MountableFile.forHostPath;

/**
 * Hive on HDFS where {@code hdfs://} is overloaded with ViewFS ({@code ViewDistributedFileSystem})
 * and part of the namespace is mounted to {@code s3a://} on MinIO.
 * <p>
 * The {@code hive} catalog uses the legacy S3 file system ({@code hive.s3.*}), which registers
 * {@code TrinoS3FileSystem} as {@code fs.s3a.impl}, so ViewFS can open the {@code s3a://} mount link.
 */
@TestsEnvironment
public final class EnvSinglenodeHiveViewFs
        extends EnvironmentProvider
{
    private static final String S3_BUCKET_NAME = "viewfs-bucket";

    private final ResourceProvider configDir;

    @Inject
    public EnvSinglenodeHiveViewFs(DockerFiles dockerFiles, Standard standard, Hadoop hadoop, Minio minio)
    {
        super(ImmutableList.of(standard, hadoop, minio));
        configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/singlenode-hive-viewfs");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        // Create the bucket in MinIO
        Path bucketDirectory;
        try {
            bucketDirectory = Files.createTempDirectory("viewfs-bucket-contents", PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-xr-x")));
            bucketDirectory.toFile().deleteOnExit();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        builder.configureContainer(MINIO_CONTAINER_NAME, container -> container
                .withCopyFileToContainer(forHostPath(bucketDirectory), "/data/" + S3_BUCKET_NAME)
                // A static KMS key, so that MinIO supports SSE-S3, used by the hive_extended_settings catalog
                .withEnv("MINIO_KMS_SECRET_KEY", "viewfs-test-key:dmlld2ZzLXRlc3Qta21zLWtleS0zMi1ieXRlcy1vayE="));

        builder.addConnector("hive", forHostPath(configDir.getPath("hive.properties")));
        builder.addConnector("hive_without_staging", forHostPath(configDir.getPath("hive_without_staging.properties")));
        builder.addConnector("hive_recursive_directories", forHostPath(configDir.getPath("hive_recursive_directories.properties")));
        builder.addConnector("hive_extended_settings", forHostPath(configDir.getPath("hive_extended_settings.properties")));
    }
}
