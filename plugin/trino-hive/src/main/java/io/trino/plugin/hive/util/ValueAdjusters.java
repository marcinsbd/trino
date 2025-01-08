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
package io.trino.plugin.hive.util;

import io.trino.plugin.base.util.CalendarUtils;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DateType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;

import java.util.Optional;

import static io.trino.spi.type.DateType.DATE;

public final class ValueAdjusters
{
    private ValueAdjusters() {}

    public static Optional<ValueAdjuster<? extends Type>> createValueAdjuster(Type primitiveType)
    {
        if (DATE.equals(primitiveType)) {
            return Optional.of(new DateValueAdjuster());
        }
        return Optional.empty();
    }

    private static class DateValueAdjuster
            extends ValueAdjuster<DateType>
    {
        public DateValueAdjuster()
        {
            super(DATE);
        }

        @Override
        public Block apply(Block block)
        {
            Block currentBlock = block.getLoadedBlock();

            BlockBuilder blockBuilder = IntegerType.INTEGER.createBlockBuilder(null, currentBlock.getPositionCount());

            for (int i = 0; i < currentBlock.getPositionCount(); i++) {
                if (currentBlock.isNull(i)) {
                    blockBuilder.appendNull();
                    continue;
                }
                int intValue = IntegerType.INTEGER.getInt(currentBlock, i);
                intValue = CalendarUtils.convertDaysToProlepticGregorian(intValue);
                IntegerType.INTEGER.writeInt(blockBuilder, intValue);
            }
            return blockBuilder.build();
        }
    }
}
