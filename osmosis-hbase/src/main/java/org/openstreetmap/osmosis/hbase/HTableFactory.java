package org.openstreetmap.osmosis.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.openstreetmap.osmosis.hbase.common.TableFactory;

import java.io.IOException;

/**
 * Loads and wraps an hbase connection
 *
 * Created by willtemperley@gmail.com on 14-Jul-16.
 */
class HTableFactory  implements TableFactory {


    private final Connection connection;

    public HTableFactory() throws IOException {
        connection = ConnectionFactory.createConnection(getConfiguration());
    }

    private Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "hadoop-m2,hadoop-m1,hadoop-01");
        configuration.set("hbase.master", "hadoop-m2");
        return configuration;
    }

    @Override
    public Table getTable(String tableName) throws IOException {
        return connection.getTable(TableName.valueOf(tableName));
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
