package org.openstreetmap.osmosis.hbase.common;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.WritableUtils;
import org.openstreetmap.osmosis.core.domain.v0_6.CommonEntityData;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Ser/Deserializer for a Way
 *
 * Created by willtemperley@gmail.com on 15-Jul-16.
 */
public class WaySerDe extends EntitySerDe<Way> {

    private static byte[] nodeCol = "waynodes".getBytes();

    @Override
    public void encode(Way entity, Put put) {

        List<WayNode> wayNodes = entity.getWayNodes();
        long[] nodeIds = new long[wayNodes.size()];

        for (int i = 0; i < wayNodes.size(); i++) {
            nodeIds[i] = wayNodes.get(i).getNodeId();
        }

        ArrayPrimitiveWritable writable = new ArrayPrimitiveWritable();
        writable.set(nodeIds);
        byte[] bytes = WritableUtils.toByteArray(writable);

        put.addColumn(data, nodeCol, bytes);

    }

    @Override
    public Way constructEntity(Result result, CommonEntityData commonEntityData) {

        ArrayPrimitiveWritable writable = new ArrayPrimitiveWritable();
        try {
            writable.readFields(new DataInputStream(new ByteArrayInputStream(result.getValue(data, nodeCol))));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        long[] longs = (long[]) writable.get();
        List<WayNode> wayNodes = new ArrayList<WayNode>(longs.length);

        for (long aLong : longs) {
            wayNodes.add(new WayNode(aLong));
        }

        return new Way(commonEntityData, wayNodes);
    }

}
