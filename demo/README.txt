#sep-demo

This project contains some sample code making use of the SEP.

## Introduction

When starting to explore the demo code, first look at LoggingConsumer.

The SolrIndexingConsumer is already much more advanced, and shows how this might be used
in a real-world scenario.

## Setup

For now, start using launch-test-lily

    launch-test-lily -hadoop -solr -s demo/schema.xml -c 2

TODO: when replacing with real hbase, don't forget forkedreplicationsource & required hbase config

## Demo

Start by creating a schema:

    mvn exec:java -Dexec.mainClass=com.ngdata.sep.demo.DemoSchema

Start the indexing consumer:

    mvn exec:java -Dexec.mainClass=com.ngdata.sep.demo.SolrIndexingConsumer

Upload some content:

    mvn exec:java -Dexec.mainClass=com.ngdata.sep.demo.DemoIngester

(this continues forever, interrupt with ctrl+c)

Check it is indexed by querying Solr:

    http://localhost:8983/solr

To follow what's going on, you can use the hbase shell to insert an individual row:

    put 'sep-user-demo', 'my-user-id', 'info:name', 'Jules'

Unfortunately, the put command in the shell only allows to set one column value at a time.

You can check it exists in Solr with the query

    id:my-user-id

Now we can update this user to also add the email address:

    put 'sep-user-demo', 'my-user-id', 'info:email', 'jules@hotmail.com'

If you now query Solr, you will see the indexed document contains both fields, which is only possible
because the indexer went back to HBase to read the row.

### Multiple indexers

It is possible to start the SolrIndexingConsumer multiple times. Each instance will receive a
subset of the events, so the work will be divided amongst them.

So start another instance:

    mvn exec:java -Dexec.mainClass=com.ngdata.sep.demo.SolrIndexingConsumer

In the HBase shell, add some more users:

    put 'sep-user-demo', 'user-1', 'info:name', 'Julio'
    put 'sep-user-demo', 'user-2', 'info:name', 'Julia'
    put 'sep-user-demo', 'user-3', 'info:name', 'Juli'

and check by the logging of the SolrIndexingConsumer's that only one of the indexing consumers
processed each event.

### Multiple subscribers

It is possible to register multiple event subscribers, each subscriber will receive all events.
The events will be distributed amongst the consumers started for that subscriber.

For example, start the logging consumer:

    mvn exec:java -Dexec.mainClass=com.ngdata.sep.demo.LoggingConsumer

Again, create a record using the shell, and notice how it is processed by both one of the indexing
consumers and the logging consumer, since these have registered two different subscriptions.
