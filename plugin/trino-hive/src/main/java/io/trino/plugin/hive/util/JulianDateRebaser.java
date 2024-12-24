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

import java.time.LocalDate;
import java.time.chrono.ChronoLocalDate;
import java.time.chrono.Chronology;

public final class JulianDateRebaser
{
    private static final int[] JULIAN_GREGORIAN_DIFFS = {2, 1, 0, -1, -2, -3, -4, -5, -6, -7, -8, -9, -10, 0};
    private static final int[] JULIAN_GREGORIAN_DIFF_SWITCH_DAY = {
            -719164, -682945, -646420, -609895, -536845,
            -500320, -463795, -390745, -354220, -317695,
            -244645, -208120, -171595, -141427
    };

    // Constant for the last switch Julian day
    public static final int LAST_SWITCH_JULIAN_DAY = JULIAN_GREGORIAN_DIFF_SWITCH_DAY[JULIAN_GREGORIAN_DIFF_SWITCH_DAY.length - 1];

    // Constant for Julian common era start day
    private static final int JULIAN_COMMON_ERA_START_DAY = JULIAN_GREGORIAN_DIFF_SWITCH_DAY[0];

    private JulianDateRebaser() {}

    public static int rebaseJulianToGregorianDays(int days)
    {
        if (days < JULIAN_COMMON_ERA_START_DAY) {
            return localRebaseJulianToGregorianDays(days);
        }
        else if (days < LAST_SWITCH_JULIAN_DAY) {
            return rebaseDays(JULIAN_GREGORIAN_DIFF_SWITCH_DAY, JULIAN_GREGORIAN_DIFFS, days);
        }
        return days;
    }

    private static int localRebaseJulianToGregorianDays(int days)
    {
        Chronology julianChronology = Chronology.of("Julian");
        ChronoLocalDate julianDate = julianChronology.dateEpochDay(days);
        LocalDate gregorianDate = LocalDate.ofEpochDay(julianDate.toEpochDay());
        return (int) gregorianDate.toEpochDay();
    }

    private static int rebaseDays(int[] switches, int[] diffs, int days)
    {
        int i = switches.length - 1;
        while (i > 0 && days < switches[i]) {
            i--;
        }
        return days + diffs[i];
    }
}
