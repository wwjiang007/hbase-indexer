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
package com.ngdata.hbaseindexer.indexer;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Shards ids using the default sharding logic as it existed before Lily 3.0.
 */
public class LilySharder implements Sharder {

    private int numShards;
    private MessageDigest mdAlgorithm;

    public LilySharder(int numShards) throws NoSuchAlgorithmException {
        Preconditions.checkArgument(numShards > 0, "There should be at least one shard");
        this.numShards = numShards;
        this.mdAlgorithm = MessageDigest.getInstance("MD5");

    }

    private long hash(String key) throws SharderException {
        try {
            // Cloning message digest rather than looking it up each time
            MessageDigest md = (MessageDigest)mdAlgorithm.clone();
            byte[] digest = md.digest(key.getBytes("UTF-8"));
            return ((digest[0] & 0xFF) << 8) + ((digest[1] & 0xFF));
        } catch (UnsupportedEncodingException e) {
            throw new SharderException("Error calculating hash.", e);
        } catch (CloneNotSupportedException e) {
            // Sun's MD5 supports cloning, so we don't expect this to happen
            throw new RuntimeException(e);
        }
    }

    public int getShard(String id) throws SharderException {
        String[] parts = LilySharder.escapedSplit(id, '.');

        if (parts.length < 2) {
            throw new RuntimeException("Does not look like a Lily id: " + id);
        }

        // only keep the master record id (part 0 (the type) and part 1)
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(escapeReservedCharacters(parts[0]));
        stringBuilder.append(".");
        stringBuilder.append(escapeReservedCharacters(parts[1]));

        long h = hash(stringBuilder.toString());
        return (int)((h % numShards) + numShards) % numShards;

    }

    /**
     * Borrowed from Lily's IdGenerator class
     */
    private static String[] escapedSplit(String s, char delimiter) {
        List<String> split = Lists.newArrayList();
        StringBuffer sb = new StringBuffer();
        boolean escaped = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (escaped) {
                escaped = false;
                sb.append(c);
            } else if (delimiter == c) {
                split.add(sb.toString());
                sb = new StringBuffer();
            } else if ('\\' == c) {
                escaped = true;
                sb.append(c);
            } else {
                sb.append(c);
            }
        }
        split.add(sb.toString());

        return split.toArray(new String[0]);
    }

    /**
     * Borrowed from Lily's IdGenerator class
     */
    protected static String escapeReservedCharacters(String text) {
        return text.replaceAll("([.,=\\\\])", "\\\\$1");
    }

}
