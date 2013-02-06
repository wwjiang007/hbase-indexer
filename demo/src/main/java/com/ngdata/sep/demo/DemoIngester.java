package com.ngdata.sep.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class DemoIngester {
    private List<String> names;
    private List<String> domains;

    public static void main(String[] args) throws Exception {
        new DemoIngester().run();
    }

    public void run() throws Exception {
        Configuration conf = HBaseConfiguration.create();

        DemoSchema.createSchema(conf);

        final byte[] infoCf = Bytes.toBytes("info");

        // column qualifiers
        final byte[] nameCq = Bytes.toBytes("name");
        final byte[] emailCq = Bytes.toBytes("email");
        final byte[] ageCq = Bytes.toBytes("age");
        final byte[] payloadCq = Bytes.toBytes("payload");

        loadData();

        ObjectMapper jsonMapper = new ObjectMapper();

        HTable htable = new HTable(conf, "sep-user-demo");

        while (true) {
            byte[] rowkey = Bytes.toBytes(UUID.randomUUID().toString());
            Put put = new Put(rowkey);

            String name = pickName();
            String email = name.toLowerCase() + "@" + pickDomain();
            String age = String.valueOf((int) Math.ceil(Math.random() * 100));

            put.add(infoCf, nameCq, Bytes.toBytes(name));
            put.add(infoCf, emailCq, Bytes.toBytes(email));
            put.add(infoCf, ageCq, Bytes.toBytes(age));

            MyPayload payload = new MyPayload();
            payload.setPartialUpdate(false);
            put.add(infoCf, payloadCq, jsonMapper.writeValueAsBytes(payload));

            htable.put(put);
            System.out.println("Added row " + Bytes.toString(rowkey));
        }
    }

    private String pickName() {
        return names.get((int)Math.floor(Math.random() * names.size()));
    }

    private String pickDomain() {
        return domains.get((int)Math.floor(Math.random() * domains.size()));
    }

    private void loadData() throws IOException {
        // Names
        BufferedReader reader =
                new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("names/names.txt")));

        names = new ArrayList<String>();

        String line;
        while ((line = reader.readLine()) != null) {
            names.add(line);
        }

        // Domains
        domains = new ArrayList<String>();
        domains.add("gmail.com");
        domains.add("hotmail.com");
        domains.add("yahoo.com");
        domains.add("live.com");
        domains.add("ngdata.com");
    }
}
