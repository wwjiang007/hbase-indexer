/*
 * Copyright 2013 NGDATA nv
 *
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
package com.ngdata.hbaseindexer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Maps;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

public class ConfigureUtil {
    
    /**
     * Configure an object with the given configuration if the object implements {@link Configurable}.
     * @param obj object to configure
     * @param config configuration parameters
     */
    public static void configure(Object obj, byte[] config) {
        if (obj instanceof Configurable) {
            ((Configurable)obj).configure(config);
        }
    }

    public static <T extends JsonNode> T deepCopy(T orig) {
        try {
            return (T) new ObjectMapper().readTree(orig.traverse());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    public static byte[] mapToJson(Map<String, String> params) {
        try {
            return new ObjectMapper().writeValueAsBytes(params == null ? Collections.EMPTY_MAP : params);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    public static Map<String, String> jsonToMap(byte[] config) {
        try {
            return config == null ? Maps.newHashMap() : new ObjectMapper().readValue(config, HashMap.class);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
