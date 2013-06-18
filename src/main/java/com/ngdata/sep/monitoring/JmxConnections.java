package com.ngdata.sep.monitoring;

import com.ngdata.sep.util.io.Closer;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JmxConnections {
    private Map<String, JMXConnector> connections = new HashMap<String, JMXConnector>();

    public JMXConnector getConnector(String serverName, int port) throws IOException {
        String hostport = serverName + ":" + port;
        JMXConnector connector = connections.get(hostport);
        if (connector == null) {
            JMXServiceURL url = new JMXServiceURL("service:jmx:rmi://" + hostport + "/jndi/rmi://" + hostport + "/jmxrmi");
            connector = JMXConnectorFactory.connect(url);
            connector.connect();
            connections.put(hostport, connector);
        }
        return connector;
    }

    public void close() {
        List<JMXConnector> list = new ArrayList<JMXConnector>(connections.values());
        connections.clear();

        for (JMXConnector conn : list) {
            Closer.close(conn);
        }
    }
}
