package org.openstreetmap.osmosis.hbase.common;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.openstreetmap.osmosis.core.domain.v0_6.CommonEntityData;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;

/**
 * encode/decode a Node
 *
 * Created by willtemperley@gmail.com on 18-Jul-16.
 */
public class NodeSerDe extends EntitySerDe<Node> {

    private static byte[] latitude = "lat".getBytes();
    private static byte[] longitude = "lon".getBytes();

    @Override
    public void encode(Node entity, Put put) {

        setDouble(latitude, entity.getLatitude(), put);
        setDouble(longitude, entity.getLongitude(), put);

    }

    @Override
    public Node constructEntity(Result result, CommonEntityData commonEntityData) {

        Double lat = getDouble(latitude, result);
        Double lon = getDouble(longitude, result);

        return new Node(commonEntityData, lat, lon);
    }
}
