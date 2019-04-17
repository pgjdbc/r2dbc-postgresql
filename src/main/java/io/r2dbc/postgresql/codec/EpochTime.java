package io.r2dbc.postgresql.codec;

import java.time.Instant;

class EpochTime {

    private final long javaSeconds;

    private final int nanos;

    private EpochTime(long pgMicros) {
        long pgSeconds = pgMicros / 1000000;
        this.javaSeconds = toJavaSeconds(pgSeconds);
        this.nanos = (int) (pgMicros - pgSeconds * 1000000) * 1000;
    }

    static EpochTime fromInt(int pgDays) {
        return new EpochTime(pgDays * 86400L * 1000000);
    }

    static EpochTime fromLong(long pgMicros) {
        return new EpochTime(pgMicros);
    }

    long getJavaDays() {
        return javaSeconds / 86400L;
    }

    int getNanos() {
        return nanos;
    }

    long getSeconds() {
        return javaSeconds;
    }

    Instant toInstant() {
        return Instant.ofEpochSecond(getSeconds(), getNanos());
    }

    /**
     * original implementation https://github.com/pgjdbc/pgjdbc/blob/master/pgjdbc/src/main/java/org/postgresql/jdbc/TimestampUtils.java#L1333
     */
    private long toJavaSeconds(long secs) {
        // postgres epoch to java epoch
        secs += 946684800L;

        // Julian/Gregorian calendar cutoff point
        if (secs < -12219292800L) { // October 4, 1582 -> October 15, 1582
            secs += 86400 * 10;
            if (secs < -14825808000L) { // 1500-02-28 -> 1500-03-01
                int extraLeaps = (int) ((secs + 14825808000L) / 3155760000L);
                extraLeaps--;
                extraLeaps -= extraLeaps / 4;
                secs += extraLeaps * 86400L;
            }
        }
        return secs;
    }
}
