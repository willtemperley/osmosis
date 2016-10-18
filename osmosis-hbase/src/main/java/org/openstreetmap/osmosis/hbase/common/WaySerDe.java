package org.openstreetmap.osmosis.hbase.common;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.WritableUtils;
import org.openstreetmap.osmosis.core.domain.v0_6.CommonEntityData;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
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

    private static byte[] nodeCol = Bytes.toBytes("waynodes");
    public static byte[] geom = Bytes.toBytes("geom");
    private WKBReader wkbReader = new WKBReader();

    private static Geometry emptyGeom;
    static {
        emptyGeom = new GeometryFactory().createGeometryCollection(null);
    }

    @Override
    public int getEntityType() {
        return EntityType.Way.ordinal();
    }

    @Override
    public void encode(byte[] rowKey, Way entity, List<Cell> keyValues) {

        List<WayNode> wayNodes = entity.getWayNodes();
        long[] nodeIds = new long[wayNodes.size()];

        for (int i = 0; i < wayNodes.size(); i++) {
            nodeIds[i] = wayNodes.get(i).getNodeId();
        }

        ArrayPrimitiveWritable writable = new ArrayPrimitiveWritable();
        writable.set(nodeIds);
        byte[] bytes = WritableUtils.toByteArray(writable);

        keyValues.add(getDataCellGenerator().getKeyValue(rowKey, nodeCol, bytes));
        keyValues.add(getDataCellGenerator().getKeyValue(rowKey, nodeCol, bytes));

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

        byte[] value = result.getValue(EntityDataAccess.data, geom);
        try {

            Geometry geometry;
            if (value != null) {
               geometry = wkbReader.read(value);
            } else {
               geometry = emptyGeom;
            }
            //fixme geometry :)
            return new Way(commonEntityData, wayNodes);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

    }

}
