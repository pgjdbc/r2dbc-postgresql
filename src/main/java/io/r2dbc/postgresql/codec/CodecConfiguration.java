/*
 * Copyright 2022 the original author or authors.
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

package io.r2dbc.postgresql.codec;

import java.time.ZoneId;

/**
 * Configuration object for codecs providing various details required for encoding and decoding data.
 *
 * @since 1.0
 */
public interface CodecConfiguration {

    /**
     * Return the zone identifier used for temporal conversions from local to zoned temporals.
     *
     * @return the zone identifier
     */
    ZoneId getZoneId();

}
