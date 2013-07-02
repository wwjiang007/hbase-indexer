hbase-sep-monitoring
====================

Monitoring tool for the HBase SEP or HBase replication in general. It is based on reading
information from ZooKeeper, HDFS (to get hlog file sizes) and HBase JMX (optional, to get
some metrics).

Build:

    mvn install

Run:

    ./target/sep-replication-status

Use the -z option to specify the zookeeper host.

Package in a .tar.gz to deploy to your server:

    mvn -Pdist install


There is also a tool to wait until replication (sep processing) is done:

    ./target/sep-replication-wait

or to see what it is doing

    ./target/sep-replication-wait --verbose

Again, use the -z option to specify the zookeeper host.

Note that:

 * this assumes there is no ongoing hbase write activity, otherwise replication
   will never be finished. There can be updates to SEP listeners, but those
   should eventually die out.

 * this tool can't know for sure replication is done, it is based on some
   heuristics which can fail.

