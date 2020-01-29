/*
 * Copyright 2019-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql.replication;

import java.util.Objects;

/**
 * LSN (Log Sequence Number). The value represents a pointer to a location in the XLOG.
 */
public final class LogSequenceNumber implements Comparable<LogSequenceNumber> {

    /**
     * Zero is used indicate an invalid pointer. Bootstrap skips the first possible WAL segment,
     * initializing the first WAL page at XLOG_SEG_SIZE, so no XLOG record can begin at zero.
     */
    public static final LogSequenceNumber INVALID_LSN = LogSequenceNumber.valueOf(0);

    private final long value;

    private LogSequenceNumber(long value) {
        this.value = value;
    }

    /**
     * @param value numeric represent position in the write-ahead log stream
     * @return not null LSN instance
     */
    public static LogSequenceNumber valueOf(long value) {
        return new LogSequenceNumber(value);
    }

    /**
     * Create LSN instance by string represent LSN.
     *
     * @param strValue not null string as two hexadecimal numbers of up to 8 digits each, separated by
     *                 a slash. For example {@code 16/3002D50}, {@code 0/15D68C50}
     * @return the LSN or {@link LogSequenceNumber#INVALID_LSN} if specified string does not have not valid form
     */
    public static LogSequenceNumber valueOf(String strValue) {
        int slashIndex = strValue.lastIndexOf('/');

        if (slashIndex <= 0) {
            return INVALID_LSN;
        }

        int logicalXlog = (int) Long.parseLong(strValue.substring(0, slashIndex), 16);
        int segment = (int) Long.parseLong(strValue.substring(slashIndex + 1), 16);
        long value = ((long) logicalXlog) << 32 | segment;

        return LogSequenceNumber.valueOf(value);
    }

    /**
     * @return Long represent position in the write-ahead log stream
     */
    public long asLong() {
        return this.value;
    }

    /**
     * @return String represent position in the write-ahead log stream as two hexadecimal numbers of
     * up to 8 digits each, separated by a slash. For example {@code 16/3002D50}, {@code 0/15D68C50}
     */
    public String asString() {
        int logicalXlog = (int) (this.value >> 32);
        int segment = (int) (this.value);
        return String.format("%X/%X", logicalXlog, segment);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LogSequenceNumber)) {
            return false;
        }
        LogSequenceNumber that = (LogSequenceNumber) o;
        return this.value == that.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.value);
    }

    @Override
    public String toString() {
        return "LSN{" + asString() + '}';
    }

    @Override
    public int compareTo(LogSequenceNumber o) {
        if (this.value == o.value) {
            return 0;
        }
        //Unsigned comparison
        return this.value + Long.MIN_VALUE < o.value + Long.MIN_VALUE ? -1 : 1;
    }

}
