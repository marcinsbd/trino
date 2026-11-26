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

import io.trino.filesystem.TrinoInputStream;
import org.apache.hadoop.fs.FSInputStream;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

final class NativeHadoopInputStream
        extends FSInputStream
{
    private final TrinoInputStream delegate;

    NativeHadoopInputStream(TrinoInputStream delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public void seek(long position)
            throws IOException
    {
        delegate.seek(position);
    }

    @Override
    public long getPos()
            throws IOException
    {
        return delegate.getPosition();
    }

    @Override
    public boolean seekToNewSource(long targetPosition)
    {
        return false;
    }

    @Override
    public int read()
            throws IOException
    {
        return delegate.read();
    }

    @Override
    public int read(byte[] buffer, int offset, int length)
            throws IOException
    {
        return delegate.read(buffer, offset, length);
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        return delegate.skip(n);
    }

    @Override
    public int available()
            throws IOException
    {
        return delegate.available();
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }
}
