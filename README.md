hbase-sep-monitoring
====================

Monitoring tool for the HBase SEP or HBase replication in general. It is based on reading
information from ZooKeeper, HDFS (to get hlog file sizes) and HBase JMX (optional, to get
some metrics).

Build:

    mvn install

Run:

    ./target/sep-replication-status

Package in a .tar.gz to deploy to your server:

    mvn -Pdist install
