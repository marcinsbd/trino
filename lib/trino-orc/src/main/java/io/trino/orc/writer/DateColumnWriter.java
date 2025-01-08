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
package io.trino.orc.writer;

import io.trino.orc.OrcWriterOptions;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.OrcColumnId;
import io.trino.orc.metadata.statistics.LongValueStatisticsBuilder;
import io.trino.plugin.base.util.CalendarUtils;
import io.trino.spi.type.Type;

import java.util.function.Supplier;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class DateColumnWriter
        extends LongColumnWriter
{
    private final OrcWriterOptions.WriterIdentification writerIdentification;

    public DateColumnWriter(OrcColumnId columnId, Type type, CompressionKind compression, int bufferSize, Supplier<LongValueStatisticsBuilder> statisticsBuilderSupplier, OrcWriterOptions.WriterIdentification writerIdentification)
    {
        super(columnId, type, compression, bufferSize, statisticsBuilderSupplier);
        this.writerIdentification = requireNonNull(writerIdentification, "writerIdentification is null");
    }

    protected long transformValue(long value)
    {
        switch (writerIdentification) {
            case LEGACY_HIVE_COMPATIBLE -> {
                return value;
            }
            case TRINO -> CalendarUtils.convertDaysToProlepticGregorian(toIntExact(value));
        }
        return value;
    }
}
