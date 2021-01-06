/*
 * Copyright 2017 the original author or authors.
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

package io.r2dbc.postgresql.message.backend;

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.util.Assert;
import reactor.util.annotation.Nullable;

import java.util.Objects;

import static io.r2dbc.postgresql.message.backend.BackendMessageUtils.readCStringUTF8;

/**
 * The CommandComplete message.
 */
public final class CommandComplete implements BackendMessage {

    private final static String[] NO_ROW_ID_TAGS = {"SELECT", "UPDATE", "DELETE", "COPY", "FETCH", "MOVE",};

    private final String command;

    private final Integer rowId;

    private final Integer rows;

    /**
     * Create a new message.
     *
     * @param command the command that was completed
     * @param rowId   the object ID of the inserted row
     * @param rows    the number of rows affected by the command
     * @throws IllegalArgumentException if {@code command} is {@code null}
     */
    public CommandComplete(String command, @Nullable Integer rowId, @Nullable Integer rows) {
        this.command = Assert.requireNonNull(command, "command must not be null");
        this.rowId = rowId;
        this.rows = rows;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CommandComplete that = (CommandComplete) o;
        return Objects.equals(this.command, that.command) &&
            Objects.equals(this.rowId, that.rowId) &&
            Objects.equals(this.rows, that.rows);
    }

    /**
     * Returns the command that was completed.
     *
     * @return the command that was completed
     */
    public String getCommand() {
        return this.command;
    }

    /**
     * Returns the object ID of the inserted row if a single row is inserted the target table has OIDs. Otherwise is {@code 0}.
     *
     * @return the object ID of the inserted row
     */
    @Nullable
    public Integer getRowId() {
        return this.rowId;
    }

    /**
     * Returns the number of rows affected by the command.
     *
     * @return the number of rows affected by the command
     */
    @Nullable
    public Integer getRows() {
        return this.rows;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.command, this.rowId, this.rows);
    }

    @Override
    public String toString() {
        return "CommandComplete{" +
            "command=" + this.command +
            ", rowId=" + this.rowId +
            ", rows=" + this.rows +
            '}';
    }

    static CommandComplete decode(ByteBuf in) {
        Assert.requireNonNull(in, "in must not be null");

        String tag = readCStringUTF8(in);

        if (tag.startsWith("INSERT")) {

            int index1 = tag.indexOf(' ');
            int index2 = tag.indexOf(' ', index1 + 1);
            int index3 = tag.indexOf(' ', index2 + 1);
            String command = tag.substring(0, index1);
            String rowId = tag.substring(index1 + 1, index2);
            String rows = tag.substring(index2 + 1, index3 != -1 ? index3 : tag.length());

            return new CommandComplete(command, Integer.parseInt(rowId), Integer.parseInt(rows));
        } else if (isNoRowId(tag)) {

            int index1 = tag.indexOf(' ');
            int index2 = tag.indexOf(' ', index1 + 1);
            String command = tag.substring(0, index1 != -1 ? index1 : tag.length());
            String rows = index1 != -1 ? tag.substring(index1 + 1, index2 != -1 ? index2 : tag.length()) : null;

            return new CommandComplete(command, null, rows != null ? Integer.parseInt(rows) : null);
        } else {
            return new CommandComplete(tag, null, null);
        }
    }

    private static boolean isNoRowId(String tag) {
        for (String noRowIdTag : NO_ROW_ID_TAGS) {
            if (tag.startsWith(noRowIdTag)) {
                return true;
            }
        }
        return false;
    }

}
