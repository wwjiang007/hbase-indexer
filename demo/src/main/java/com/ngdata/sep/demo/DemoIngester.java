package com.ngdata.sep.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.UUID;

public class DemoIngester {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();

        HBaseAdmin admin = new HBaseAdmin(conf);
        // TODO rename record to demo -- depends on sep changes
        if (!admin.tableExists("record")) {
            HTableDescriptor tableDescriptor = new HTableDescriptor("record");
            HColumnDescriptor columnDescriptor = new HColumnDescriptor("data");
            columnDescriptor.setScope(1);
            tableDescriptor.addFamily(columnDescriptor);
            admin.createTable(tableDescriptor);
        }

        final byte[] dataCf = Bytes.toBytes("data");
        final byte[] field1 = Bytes.toBytes("field1");

        HTable htable = new HTable(conf, "record");

        while (true) {
            byte[] rowkey = Bytes.toBytes(UUID.randomUUID().toString());
            Put put = new Put(rowkey);
            put.add(dataCf, field1, Bytes.toBytes("this is some text"));
            put.add(dataCf, Bytes.toBytes("pl"), Bytes.toBytes("this is the payload"));
            htable.put(put);
            System.out.println("Added row " + Bytes.toString(rowkey));
        }
    }
}
