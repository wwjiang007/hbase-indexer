package com.ngdata.sep.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class DemoSchema {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        createSchema(conf);
    }

    public static void createSchema(Configuration hbaseConf) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(hbaseConf);
        if (!admin.tableExists("sep-user-demo")) {
            HTableDescriptor tableDescriptor = new HTableDescriptor("sep-user-demo");

            HColumnDescriptor infoCf = new HColumnDescriptor("info");
            infoCf.setScope(1);
            tableDescriptor.addFamily(infoCf);

            admin.createTable(tableDescriptor);
        }
        admin.close();
    }
}
