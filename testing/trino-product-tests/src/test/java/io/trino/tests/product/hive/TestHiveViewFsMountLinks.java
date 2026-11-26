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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.hive.HiveViewFsEnvironment.BUCKET_PREFIX;
import static io.trino.tests.product.hive.HiveViewFsEnvironment.CATALOG_WITHOUT_NATIVE_S3;
import static io.trino.tests.product.hive.HiveViewFsEnvironment.S3_MOUNT_PATH;
import static io.trino.tests.product.hive.HiveViewFsEnvironment.VIEWFS_S3_MOUNT_PATH;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests Hive tables whose {@code hdfs://} location is resolved by the Hadoop ViewFS overload
 * scheme ({@code ViewDistributedFileSystem}), either to the HDFS fallback or to an {@code s3a://}
 * mount link.
 * <p>
 * Tables behind a plain {@code viewfs://} mount table are covered as well.
 * <p>
 * Without the legacy S3 file system, nothing provides a Hadoop {@code FileSystem} for {@code s3a}
 * unless native S3 is enabled in the catalog. Otherwise ViewFS falls back to
 * {@code org.apache.hadoop.fs.s3a.S3AFileSystem}, which Trino does not ship, and reading tables
 * behind an {@code s3a://} mount link fails with {@code ClassNotFoundException}.
 */
@ProductTest
@RequiresEnvironment(HiveViewFsEnvironment.class)
@TestGroup.HiveViewFs
@TestGroup.ProfileSpecificTests
class TestHiveViewFsMountLinks
{
    @Test
    void testReadTableOnViewFsHdfsFallback(HiveViewFsEnvironment env)
    {
        String tableName = "test_viewfs_hdfs_fallback";
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        try {
            env.executeTrinoUpdate("CREATE TABLE hive.default." + tableName + " AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) t(id, name)");
            assertThat(env.executeTrino("SELECT id, name FROM hive.default." + tableName))
                    .containsOnly(row(1, "a"), row(2, "b"), row(3, "c"));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    void testReadTableOnViewFsMountLinkToS3(HiveViewFsEnvironment env)
    {
        String tableName = "test_viewfs_s3_mount_link";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        env.deleteS3Objects(objectPrefix);
        try {
            // hdfs:// alias that ViewFS resolves to s3a://viewfs-bucket/s3_data/<table>
            env.executeHiveUpdate(
                    """
                    CREATE EXTERNAL TABLE default.%s (id INT, name STRING)
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    LOCATION '%s'
                    """.formatted(tableName, env.hdfsLocation(S3_MOUNT_PATH + "/" + tableName)));
            env.putS3Object(objectPrefix + "data.csv", "1,a\n2,b\n3,c\n");

            assertThat(env.executeTrino("SELECT id, name FROM hive.default." + tableName))
                    .containsOnly(row(1, "a"), row(2, "b"), row(3, "c"));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
            env.deleteS3Objects(objectPrefix);
        }
    }

    @Test
    void testReadPartitionedTableOnViewFsMountLinkToS3(HiveViewFsEnvironment env)
    {
        String tableName = "test_viewfs_s3_mount_link_partitioned";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        env.deleteS3Objects(objectPrefix);
        try {
            env.executeHiveUpdate(
                    """
                    CREATE EXTERNAL TABLE default.%s (id INT, name STRING)
                    PARTITIONED BY (event_date STRING)
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    LOCATION '%s'
                    """.formatted(tableName, env.hdfsLocation(S3_MOUNT_PATH + "/" + tableName)));
            env.putS3Object(objectPrefix + "event_date=2026-11-01/data.csv", "1,a\n2,b\n");
            env.putS3Object(objectPrefix + "event_date=2026-11-02/data.csv", "3,c\n");

            env.executeTrinoUpdate("CALL hive.system.sync_partition_metadata('default', '" + tableName + "', 'FULL')");

            assertThat(env.executeTrino("SELECT id, name, event_date FROM hive.default." + tableName))
                    .containsOnly(
                            row(1, "a", "2026-11-01"),
                            row(2, "b", "2026-11-01"),
                            row(3, "c", "2026-11-02"));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
            env.deleteS3Objects(objectPrefix);
        }
    }

    @Test
    void testReadMultiLevelPartitionedTableOnViewFsMountLinkToS3(HiveViewFsEnvironment env)
    {
        String tableName = "test_viewfs_s3_mount_link_multi_level";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        env.deleteS3Objects(objectPrefix);
        try {
            env.executeHiveUpdate(
                    """
                    CREATE EXTERNAL TABLE default.%s (id INT, name STRING)
                    PARTITIONED BY (region STRING, category STRING, event_date STRING)
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    LOCATION '%s'
                    """.formatted(tableName, env.hdfsLocation(S3_MOUNT_PATH + "/" + tableName)));
            env.putS3Object(objectPrefix + "region=eu/category=a/event_date=2026-11-01/data.csv", "1,a\n");
            env.putS3Object(objectPrefix + "region=eu/category=a/event_date=2026-11-02/data.csv", "2,b\n");
            env.putS3Object(objectPrefix + "region=eu/category=b/event_date=2026-11-01/data.csv", "3,c\n");
            env.putS3Object(objectPrefix + "region=us/category=a/event_date=2026-11-01/data.csv", "4,d\n5,e\n");

            // Discovering partitions lists every directory level through ViewFS
            env.executeTrinoUpdate("CALL hive.system.sync_partition_metadata('default', '" + tableName + "', 'FULL')");

            assertThat(env.executeTrino("SELECT region, category, event_date FROM hive.default.\"" + tableName + "$partitions\""))
                    .containsOnly(
                            row("eu", "a", "2026-11-01"),
                            row("eu", "a", "2026-11-02"),
                            row("eu", "b", "2026-11-01"),
                            row("us", "a", "2026-11-01"));
            assertThat(env.executeTrino("SELECT id, name, region, category, event_date FROM hive.default." + tableName))
                    .containsOnly(
                            row(1, "a", "eu", "a", "2026-11-01"),
                            row(2, "b", "eu", "a", "2026-11-02"),
                            row(3, "c", "eu", "b", "2026-11-01"),
                            row(4, "d", "us", "a", "2026-11-01"),
                            row(5, "e", "us", "a", "2026-11-01"));
            assertThat(env.executeTrino("SELECT id FROM hive.default." + tableName + " WHERE region = 'eu' AND category = 'a'"))
                    .containsOnly(row(1), row(2));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
            env.deleteS3Objects(objectPrefix);
        }
    }

    @Test
    void testReadTableOnViewFsSchemeMountLinkToS3(HiveViewFsEnvironment env)
    {
        String tableName = "test_viewfs_scheme_s3_mount_link";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        env.deleteS3Objects(objectPrefix);
        try {
            // viewfs://cluster/s3_data/<table> resolves to s3a://viewfs-bucket/s3_data/<table>
            env.executeHiveUpdate(
                    """
                    CREATE EXTERNAL TABLE default.%s (id INT, name STRING)
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    LOCATION '%s'
                    """.formatted(tableName, env.viewFsLocation(VIEWFS_S3_MOUNT_PATH + "/" + tableName)));
            env.putS3Object(objectPrefix + "data.csv", "1,a\n2,b\n");

            assertThat(env.executeTrino("SELECT id, name FROM hive.default." + tableName))
                    .containsOnly(row(1, "a"), row(2, "b"));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
            env.deleteS3Objects(objectPrefix);
        }
    }

    @Test
    void testReadTableWithNonDataFilesOnViewFsMountLinkToS3(HiveViewFsEnvironment env)
    {
        String tableName = "test_viewfs_s3_mount_link_non_data_files";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        env.deleteS3Objects(objectPrefix);
        try {
            env.executeHiveUpdate(
                    """
                    CREATE EXTERNAL TABLE default.%s (id INT, name STRING)
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    LOCATION '%s'
                    """.formatted(tableName, env.hdfsLocation(S3_MOUNT_PATH + "/" + tableName)));
            env.putS3Object(objectPrefix + "data.csv", "1,a\n2,b\n");
            // Files written by other engines and tools next to the data, which must not be read as data
            env.putS3Object(objectPrefix + "_SUCCESS", "");
            env.putS3Object(objectPrefix + ".hidden", "9,hidden\n");
            env.putS3Object(objectPrefix + "subdirectory_$folder$", "");
            env.putS3Object(objectPrefix + "subdirectory/", "");
            env.putS3Object(objectPrefix + "subdirectory/nested.csv", "8,nested\n");

            assertThat(env.executeTrino("SELECT id, name FROM hive.default." + tableName))
                    .containsOnly(row(1, "a"), row(2, "b"));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
            env.deleteS3Objects(objectPrefix);
        }
    }

    @Test
    void testReadTableOnViewFsMountLinkToS3WithoutNativeS3(HiveViewFsEnvironment env)
    {
        String tableName = "test_viewfs_s3_mount_link_without_native_s3";
        String objectPrefix = BUCKET_PREFIX + "/" + tableName + "/";
        env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        env.deleteS3Objects(objectPrefix);
        try {
            env.executeHiveUpdate(
                    """
                    CREATE EXTERNAL TABLE default.%s (id INT, name STRING)
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    LOCATION '%s'
                    """.formatted(tableName, env.hdfsLocation(S3_MOUNT_PATH + "/" + tableName)));
            env.putS3Object(objectPrefix + "data.csv", "1,a\n");

            // Without native S3 in the catalog, nothing provides a Hadoop file system for s3a://
            assertThatThrownBy(() -> env.executeTrino("SELECT id, name FROM %s.default.%s".formatted(CATALOG_WITHOUT_NATIVE_S3, tableName)))
                    .hasMessageContaining("Class org.apache.hadoop.fs.s3a.S3AFileSystem not found");
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
            env.deleteS3Objects(objectPrefix);
        }
    }
}
