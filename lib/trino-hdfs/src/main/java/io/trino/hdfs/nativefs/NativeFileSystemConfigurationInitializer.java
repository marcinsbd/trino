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

import io.trino.hdfs.ConfigurationInitializer;
import org.apache.hadoop.conf.Configuration;

import static java.util.Objects.requireNonNull;

/**
 * Registers {@link NativeHadoopFileSystem} as the Hadoop file system for every scheme with a
 * native Trino file system, so that Hadoop file systems resolving child file systems by scheme,
 * such as ViewFS mount links, use the native file system.
 */
public final class NativeFileSystemConfigurationInitializer
        implements ConfigurationInitializer
{
    private final String registryId;

    public NativeFileSystemConfigurationInitializer(String registryId)
    {
        this.registryId = requireNonNull(registryId, "registryId is null");
    }

    @Override
    public void initializeConfiguration(Configuration config)
    {
        for (String scheme : NativeFileSystemRegistry.schemes(registryId)) {
            config.set("fs.%s.impl".formatted(scheme), NativeHadoopFileSystem.class.getName());
        }
        config.set(NativeHadoopFileSystem.REGISTRY_ID_KEY, registryId);
    }
}
