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

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Random;

import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Multimaps;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HashSharderTest {

    /*
     * this is just here to check that the default sharding strategy doesn't change
     */
    @Test
    public void testBasics() throws IOException, NoSuchAlgorithmException, SharderException {
        HashSharder sharder = new HashSharder(Lists.newArrayList("one", "two", "three"));

        assertEquals("two", sharder.getShard(String.valueOf("alpha")));
        assertEquals("three", sharder.getShard(String.valueOf("beta")));
        assertEquals("two", sharder.getShard(String.valueOf("gamma")));
        assertEquals("three", sharder.getShard(String.valueOf("delta")));
    }

    /**
     * Tests many values to detect problems with modulo calculation on negative values
     * @throws IOException
     */
    @Test
    public void testIndexOutOfBounds() throws IOException, NoSuchAlgorithmException, SharderException {
        ArrayList<String> shards = Lists.newArrayList("one", "two", "three");
        HashSharder sharder = new HashSharder(shards);

        Random rg = new Random();
        for (int i = 0; i < 100; i++) {
            sharder.getShard("foo" + rg.nextInt());
        }
    }

}
