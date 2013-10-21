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

import static com.ngdata.hbaseindexer.metrics.IndexerMetricsUtil.metricName;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.ngdata.sep.EventListener;
import com.ngdata.sep.SepEvent;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * SEP {@code EventListener} that sends all events through an {@link Indexer}
 * to create index documents.
 */
public class IndexingEventListener implements EventListener {
    
    protected Log log = LogFactory.getLog(getClass());
    
    private final Indexer indexer;
    private final Meter incomingEventsMeter;
    private final Meter applicableEventsMeter;
    private Predicate<SepEvent> tableEqualityPredicate;
    
    /**
     * Instantiate with the underlying indexer, and the name of the table for which events are to be intercepted.
     * 
     * @param indexer indexer engine that will create index documents from incoming event data
     * @param targetTableName name of the table for which updates are to be indexed
     */
    public IndexingEventListener(Indexer indexer, final byte[] targetTableName) {
        this.indexer = indexer;
        incomingEventsMeter = Metrics.newMeter(metricName(getClass(), "Incoming events", indexer.getName()),
                "Rate of incoming SEP events", TimeUnit.SECONDS);
        applicableEventsMeter = Metrics.newMeter(metricName(getClass(), "Applicable events", indexer.getName()),
                "Rate of incoming SEP events that are considered applicable", TimeUnit.SECONDS);
        
        tableEqualityPredicate = new Predicate<SepEvent>() {

            @Override
            public boolean apply(@Nullable SepEvent event) {
                return Arrays.equals(event.getTable(), targetTableName);
            }
        };

    }

    @Override
    public void processEvents(List<SepEvent> events) {
        if (log.isDebugEnabled()) {
            log.debug(String.format("Indexer %s received %s events from SEP", indexer.getName(), events.size()));
        }
        try {

            incomingEventsMeter.mark(events.size());
            events = Lists.newArrayList(Iterables.filter(events, tableEqualityPredicate));
            applicableEventsMeter.mark(events.size());
            
            indexer.indexRowData(Lists.transform(events, SepEventToRowDataFunction.INSTANCE));
           
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Converts SEP events to lists of KeyValues.
     */
    private static class SepEventToRowDataFunction implements Function<SepEvent, RowData> {
        
        static final SepEventToRowDataFunction INSTANCE = new SepEventToRowDataFunction();

        @Override
        public RowData apply(@Nullable SepEvent input) {
            return new SepEventRowData(input);
        }
        
    }

}
