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
package io.trino.filesystem;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * The direct children of a directory: the files directly within it, and its direct subdirectories.
 */
public record DirectoryListing(List<FileEntry> files, Set<Location> directories)
{
    public DirectoryListing
    {
        files = ImmutableList.copyOf(requireNonNull(files, "files is null"));
        directories = ImmutableSet.copyOf(requireNonNull(directories, "directories is null"));
    }

    public boolean isEmpty()
    {
        return files.isEmpty() && directories.isEmpty();
    }
}
