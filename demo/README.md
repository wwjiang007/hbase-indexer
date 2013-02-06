#sep-demo

This project contains some sample code making use of the SEP.

## Introduction

When starting to explore the demo code, first look at LoggingConsumer.

The SolrIndexingConsumer is already much more advanced, and shows how this might be used
in a real-world scenario.

## Setup

For the purpose of this demo, we will run HBase in standalone mode (zookeeper is embbeded,
no hdfs is used). Note that by default, HBase stores its data in /tmp, which will be lost,
see the HBase docs on how to change this.

### Download HBase

Download HBase (0.94) from [http://hbase.apache.org/](http://hbase.apache.org/).

### Configure HBase

Edit conf/hbase-site.xml and add the following configuration:

    <configuration>
      <!-- SEP is basically replication, so enable it -->
      <property>
        <name>hbase.replication</name>
        <value>true</value>
      </property>
      <!-- Source ratio of 100% makes sure that each SEP consumer is actually
           used (otherwise, some can sit idle, especially with small clusters) -->
      <property>
        <name>replication.source.ratio</name>
        <value>1.0</value>
      </property>
      <!-- Maximum number of hlog entries to replicate in one go. If this is
           large, and a consumer takes a while to process the events, the
           HBase rpc call will time out. -->
      <property>
        <name>replication.source.nb.capacity</name>
        <value>1000</value>
      </property>
      <!-- A custom replication source that fixes a few things and adds
           some functionality (doesn't interfere with normal replication
           usage). -->
      <property>
        <name>replication.replicationsource.implementation</name>
        <value>com.ngdata.sep.impl.SepReplicationSource</value>
      </property>
    </configuration>

### Add sep libraries to HBase:

This makes available the SepReplicationSource to HBase.

    cp hbase-sep/impl/target/hbase-sep-impl-1.0-SNAPSHOT.jar hbase/lib/
    cp hbase-sep/api/target/hbase-sep-api-1.0-SNAPSHOT.jar hbase/lib/

### Start HBase

    ./bin/start-hbase

You can check HBase is running fine by browsing to
[http://localhost:60010/](http://localhost:60010/)

### Download Solr

Solr is only required to run the SolrIndexingDemo.

Download Solr (4.1) from [http://lucene.apache.org/](http://lucene.apache.org/)
and extract the .tgz

### Configure Solr

Edit

    example/solr/collection1/conf/solrconfig.xml

and uncomment the &lt;autoSoftCommit> tag

### Start Solr

Start Solr as follows

    cd example
    java -Dsolr.solr.home=`cd solr; pwd` -jar start.jar

The specification of solr home is necessary so that the demo code will be
able to get the absolute filesystem location of your solr schema through
the core admin api, which it needs for setting up Solr Cell.

## Demo

Start by creating a schema:

    mvn exec:java -Dexec.mainClass=com.ngdata.sep.demo.DemoSchema

Important in the code of DemoSchema is that we enable replication by calling setScope(1)!

Start the indexing consumer:

    mvn exec:java -Dexec.mainClass=com.ngdata.sep.demo.SolrIndexingConsumer

Upload some content:

    mvn exec:java -Dexec.mainClass=com.ngdata.sep.demo.DemoIngester

(this continues forever, interrupt with ctrl+c)

Check it is indexed by querying Solr:

    [http://localhost:8983/solr](http://localhost:8983/solr)

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

If you would do the same with the DemoIngester, you'll notice that only one of
the consumers is working at a time, this is because there is only one region server,
which only sends out one batch of events at a time.

### Multiple subscribers

It is possible to register multiple event subscribers, each subscriber will receive all events.
The events will be distributed amongst the consumers started for that subscriber.

For example, start the logging consumer:

    mvn exec:java -Dexec.mainClass=com.ngdata.sep.demo.LoggingConsumer

Again, create a record using the shell, and notice how it is processed by both one of the indexing
consumers and the logging consumer, since these have registered two different subscriptions.
