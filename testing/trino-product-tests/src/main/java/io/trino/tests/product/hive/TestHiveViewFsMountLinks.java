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
package io.trino.tests.product.hive;

import io.trino.tempto.ProductTest;
import io.trino.testing.minio.MinioClient;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tests.product.TestGroups.HIVE_VIEWFS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests Hive tables whose {@code hdfs://} location is resolved by the Hadoop ViewFS overload
 * scheme ({@code ViewDistributedFileSystem}), either to the HDFS fallback or to an {@code s3a://}
 * mount link.
 * <p>
 * The legacy S3 file system registers {@code TrinoS3FileSystem} as {@code fs.s3a.impl}, so ViewFS
 * can open the {@code s3a://} mount link. Once the legacy S3 file system is removed, ViewFS falls back to
 * {@code org.apache.hadoop.fs.s3a.S3AFileSystem}, which Trino does not ship, and reading tables behind
 * an {@code s3a://} mount link fails with {@code ClassNotFoundException}.
 */
public class TestHiveViewFsMountLinks
        extends ProductTest
{
    private static final String BUCKET_NAME = "viewfs-bucket";
    private static final String BUCKET_PREFIX = "s3_data";
    // ViewFS mounts this path to s3a://viewfs-bucket/s3_data, see singlenode-hive-viewfs/viewfs-core-site.xml
    private static final String S3_MOUNT_LOCATION = "hdfs://hadoop-master:9000/tmp/viewfs/s3_data";

    @Test(groups = {HIVE_VIEWFS, PROFILE_SPECIFIC_TESTS})
    public void testReadTableOnViewFsHdfsFallback()
    {
        String tableName = "test_viewfs_hdfs_fallback";
        onTrino().executeQuery("DROP TABLE IF EXISTS hive.default." + tableName);
        try {
            onTrino().executeQuery("CREATE TABLE hive.default." + tableName + " AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) t(id, name)");
            assertThat(onTrino().executeQuery("SELECT id, name FROM hive.default." + tableName))
                    .containsOnly(row(1, "a"), row(2, "b"), row(3, "c"));
        }
        finally {
            onTrino().executeQuery("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test(groups = {HIVE_VIEWFS, PROFILE_SPECIFIC_TESTS})
    public void testReadTableOnViewFsMountLinkToS3()
            throws Exception
    {
        String tableName = "test_viewfs_s3_mount_link";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        try (MinioClient minio = new MinioClient()) {
            onHive().executeQuery("DROP TABLE IF EXISTS default." + tableName);
            deleteObjects(minio, objectPrefix);
            try {
                // Hive is not ViewFS-aware, so it creates a plain HDFS directory; Trino resolves the same
                // location through the mount table to s3a://viewfs-bucket/s3_data/<table>
                onHive().executeQuery(
                        """
                        CREATE EXTERNAL TABLE default.%s (id INT, name STRING)
                        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                        STORED AS TEXTFILE
                        LOCATION '%s/%s'
                        """.formatted(tableName, S3_MOUNT_LOCATION, tableName));
                minio.putObject(BUCKET_NAME, "1,a\n2,b\n3,c\n".getBytes(UTF_8), objectPrefix + "data.csv");

                assertThat(onTrino().executeQuery("SELECT id, name FROM hive.default." + tableName))
                        .containsOnly(row(1, "a"), row(2, "b"), row(3, "c"));
            }
            finally {
                onHive().executeQuery("DROP TABLE IF EXISTS default." + tableName);
                deleteObjects(minio, objectPrefix);
            }
        }
    }

    @Test(groups = {HIVE_VIEWFS, PROFILE_SPECIFIC_TESTS})
    public void testReadPartitionedTableOnViewFsMountLinkToS3()
            throws Exception
    {
        String tableName = "test_viewfs_s3_mount_link_partitioned";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        try (MinioClient minio = new MinioClient()) {
            onHive().executeQuery("DROP TABLE IF EXISTS default." + tableName);
            deleteObjects(minio, objectPrefix);
            try {
                onHive().executeQuery(
                        """
                        CREATE EXTERNAL TABLE default.%s (id INT, name STRING)
                        PARTITIONED BY (event_date STRING)
                        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                        STORED AS TEXTFILE
                        LOCATION '%s/%s'
                        """.formatted(tableName, S3_MOUNT_LOCATION, tableName));
                minio.putObject(BUCKET_NAME, "1,a\n2,b\n".getBytes(UTF_8), objectPrefix + "event_date=2026-11-01/data.csv");
                minio.putObject(BUCKET_NAME, "3,c\n".getBytes(UTF_8), objectPrefix + "event_date=2026-11-02/data.csv");

                onTrino().executeQuery("CALL hive.system.sync_partition_metadata('default', '" + tableName + "', 'FULL')");

                assertThat(onTrino().executeQuery("SELECT id, name, event_date FROM hive.default." + tableName))
                        .containsOnly(
                                row(1, "a", "2026-11-01"),
                                row(2, "b", "2026-11-01"),
                                row(3, "c", "2026-11-02"));
            }
            finally {
                onHive().executeQuery("DROP TABLE IF EXISTS default." + tableName);
                deleteObjects(minio, objectPrefix);
            }
        }
    }

    @Test(groups = {HIVE_VIEWFS, PROFILE_SPECIFIC_TESTS})
    public void testInsertIntoTableOnViewFsMountLinkToS3()
            throws Exception
    {
        String tableName = "test_viewfs_s3_mount_link_insert";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        try (MinioClient minio = new MinioClient()) {
            onHive().executeQuery("DROP TABLE IF EXISTS default." + tableName);
            deleteObjects(minio, objectPrefix);
            try {
                onHive().executeQuery(
                        """
                        CREATE EXTERNAL TABLE default.%s (id INT, name STRING)
                        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                        STORED AS TEXTFILE
                        LOCATION '%s/%s'
                        """.formatted(tableName, S3_MOUNT_LOCATION, tableName));
                minio.putObject(BUCKET_NAME, "1,a\n".getBytes(UTF_8), objectPrefix + "data.csv");

                onTrino().executeQuery("INSERT INTO hive.default." + tableName + " VALUES (2, 'b')");

                assertThat(onTrino().executeQuery("SELECT id, name FROM hive.default." + tableName))
                        .containsOnly(row(1, "a"), row(2, "b"));
                // The new file is written through the mount link to S3
                assertThat(minio.listObjects(BUCKET_NAME, objectPrefix)).hasSizeGreaterThan(1);
            }
            finally {
                onHive().executeQuery("DROP TABLE IF EXISTS default." + tableName);
                deleteObjects(minio, objectPrefix);
            }
        }
    }

    @Test(groups = {HIVE_VIEWFS, PROFILE_SPECIFIC_TESTS})
    public void testCreateTableOnViewFsMountLinkToS3()
            throws Exception
    {
        String tableName = "test_viewfs_s3_mount_link_create";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        try (MinioClient minio = new MinioClient()) {
            onTrino().executeQuery("DROP TABLE IF EXISTS hive.default." + tableName);
            deleteObjects(minio, objectPrefix);
            try {
                onTrino().executeQuery(
                        """
                        CREATE TABLE hive.default.%s (id INTEGER, name VARCHAR)
                        WITH (format = 'TEXTFILE', external_location = '%s/%s')
                        """.formatted(tableName, S3_MOUNT_LOCATION, tableName));
                onTrino().executeQuery("INSERT INTO hive.default." + tableName + " VALUES (1, 'a'), (2, 'b')");

                assertThat(onTrino().executeQuery("SELECT id, name FROM hive.default." + tableName))
                        .containsOnly(row(1, "a"), row(2, "b"));
                assertThat(minio.listObjects(BUCKET_NAME, objectPrefix)).isNotEmpty();
            }
            finally {
                onTrino().executeQuery("DROP TABLE IF EXISTS hive.default." + tableName);
                deleteObjects(minio, objectPrefix);
            }
        }
    }

    @Test(groups = {HIVE_VIEWFS, PROFILE_SPECIFIC_TESTS})
    public void testInsertWithoutStagingIntoTableOnViewFsMountLinkToS3()
            throws Exception
    {
        String tableName = "test_viewfs_s3_mount_link_insert_without_staging";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        try (MinioClient minio = new MinioClient()) {
            onHive().executeQuery("DROP TABLE IF EXISTS default." + tableName);
            deleteObjects(minio, objectPrefix);
            try {
                onHive().executeQuery(
                        """
                        CREATE EXTERNAL TABLE default.%s (id INT, name STRING)
                        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                        STORED AS TEXTFILE
                        LOCATION '%s/%s'
                        """.formatted(tableName, S3_MOUNT_LOCATION, tableName));
                minio.putObject(BUCKET_NAME, "1,a\n".getBytes(UTF_8), objectPrefix + "data.csv");

                // hive_without_staging writes directly to the table directory, without a temporary staging directory
                onTrino().executeQuery("INSERT INTO hive_without_staging.default." + tableName + " VALUES (2, 'b')");

                assertThat(onTrino().executeQuery("SELECT id, name FROM hive.default." + tableName))
                        .containsOnly(row(1, "a"), row(2, "b"));
                assertThat(minio.listObjects(BUCKET_NAME, objectPrefix)).hasSizeGreaterThan(1);
            }
            finally {
                onHive().executeQuery("DROP TABLE IF EXISTS default." + tableName);
                deleteObjects(minio, objectPrefix);
            }
        }
    }

    @Test(groups = {HIVE_VIEWFS, PROFILE_SPECIFIC_TESTS})
    public void testInsertWithoutStagingIntoEmptyTableOnViewFsMountLinkToS3()
            throws Exception
    {
        String tableName = "test_viewfs_s3_mount_link_insert_without_staging_empty";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        try (MinioClient minio = new MinioClient()) {
            onHive().executeQuery("DROP TABLE IF EXISTS default." + tableName);
            deleteObjects(minio, objectPrefix);
            try {
                // No objects exist under the table location yet
                onHive().executeQuery(
                        """
                        CREATE EXTERNAL TABLE default.%s (id INT, name STRING)
                        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                        STORED AS TEXTFILE
                        LOCATION '%s/%s'
                        """.formatted(tableName, S3_MOUNT_LOCATION, tableName));

                onTrino().executeQuery("INSERT INTO hive_without_staging.default." + tableName + " VALUES (1, 'a')");

                assertThat(onTrino().executeQuery("SELECT id, name FROM hive.default." + tableName))
                        .containsOnly(row(1, "a"));
                assertThat(minio.listObjects(BUCKET_NAME, objectPrefix)).isNotEmpty();
            }
            finally {
                onHive().executeQuery("DROP TABLE IF EXISTS default." + tableName);
                deleteObjects(minio, objectPrefix);
            }
        }
    }

    @Test(groups = {HIVE_VIEWFS, PROFILE_SPECIFIC_TESTS})
    public void testCreateTableAsSelectOnViewFsMountLinkToS3()
            throws Exception
    {
        String tableName = "test_viewfs_s3_mount_link_ctas";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        try (MinioClient minio = new MinioClient()) {
            onTrino().executeQuery("DROP TABLE IF EXISTS hive.default." + tableName);
            deleteObjects(minio, objectPrefix);
            try {
                // CREATE TABLE AS SELECT with an external location writes directly to the target directory
                onTrino().executeQuery(
                        """
                        CREATE TABLE hive.default.%s
                        WITH (format = 'TEXTFILE', external_location = '%s/%s')
                        AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(id, name)
                        """.formatted(tableName, S3_MOUNT_LOCATION, tableName));

                assertThat(onTrino().executeQuery("SELECT id, name FROM hive.default." + tableName))
                        .containsOnly(row(1, "a"), row(2, "b"));
                assertThat(minio.listObjects(BUCKET_NAME, objectPrefix)).isNotEmpty();
            }
            finally {
                onTrino().executeQuery("DROP TABLE IF EXISTS hive.default." + tableName);
                deleteObjects(minio, objectPrefix);
            }
        }
    }

    private static void deleteObjects(MinioClient minio, String prefix)
    {
        minio.listObjects(BUCKET_NAME, prefix).forEach(key -> minio.removeObject(BUCKET_NAME, key));
    }
}
