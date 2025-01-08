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
package io.trino.orc.reader;

import io.trino.memory.context.LocalMemoryContext;
import io.trino.orc.OrcColumn;
import io.trino.orc.OrcCorruptionException;
import io.trino.orc.metadata.CalendarKind;
import io.trino.plugin.base.util.CalendarUtils;
import io.trino.spi.type.Type;

import static io.trino.orc.metadata.CalendarKind.JULIAN_GREGORIAN;
import static java.lang.Math.toIntExact;

public class DateColumnReader
        extends LongColumnReader
{
    private final CalendarKind calendar;

    public DateColumnReader(Type type, OrcColumn column, LocalMemoryContext memoryContext, CalendarKind calendar)
            throws OrcCorruptionException
    {
        super(type, column, memoryContext);
        this.calendar = calendar;
    }

    protected void maybeTransformValues(long[] values, int nextBatchSize)
    {
        if (calendar == JULIAN_GREGORIAN) {
            for (int i = 0; i < nextBatchSize; i++) {
                values[i] = CalendarUtils.convertDaysToProlepticGregorian(toIntExact(values[i]));
            }
        }
    }
}
