package org.openstreetmap.osmosis.hbase.common;

import com.google.common.primitives.Longs;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.openstreetmap.osmosis.core.domain.v0_6.CommonEntityData;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.OsmUser;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.util.*;

/**
 * The basic machinery to ser/deserialize an osm entity.
 *
 * Created by willtemperley@gmail.com on 15-Jul-16.
 */
public abstract class EntitySerDe<T extends  Entity> {

    public static byte[] tags = "t".getBytes();
    public static byte[] data = "d".getBytes();

    private static byte[] version = "v".getBytes();
    private static byte[] timestamp = "ts".getBytes();
    private static byte[] changeset = "cs".getBytes();
    private static byte[] uid = "uid".getBytes();
    private static byte[] uname = "uname".getBytes();

    private long getId(byte[] rowKey) {
        ArrayUtils.reverse(rowKey);
        return Longs.fromByteArray(rowKey);
    }

    private String getString(byte[] column, Result result) {
        return Bytes.toString(result.getValue(data, column));
    }
    private void setString(Put put, byte[] column, String value) { put.addColumn(data, column, Bytes.toBytes(value));}

    private long getLong(byte[] column, Result result) {
        return Bytes.toLong(result.getValue(data, column));
    }
    private void setLong(Put put, byte[] column, long value) { put.addColumn(data, column, Bytes.toBytes(value));}

    private int getInt(byte[] column, Result result) {
        return Bytes.toInt(result.getValue(data, column));
    }
    private void setInt(Put put, byte[] column, int value) { put.addColumn(data, column, Bytes.toBytes(value));}

    double getDouble(byte[] column, Result result) { return Bytes.toDouble(result.getValue(data, column)); }
    void setDouble(byte[] column, double value, Put put) { put.addColumn(data, column, Bytes.toBytes(value));}


    void serialize(Put put, T entity) {

        setInt(put, version, entity.getVersion());
        setLong(put, timestamp, entity.getTimestamp().getTime());
        setLong(put, changeset, entity.getChangesetId());
        setInt(put, uid, entity.getUser().getId());
        setString(put, uname, entity.getUser().getName());

        for (Tag tag : entity.getTags()) {
            put.addColumn(tags, tag.getKey().getBytes(), tag.getValue().getBytes());
        }

        encode(entity, put);

    }

    public abstract void encode(T entity, Put put);


    T deSerialize(Result result) {

        return constructEntity(result, getCommonEntityData(result));
    }

    protected abstract T constructEntity(Result result, CommonEntityData commonEntityData);


    private CommonEntityData getCommonEntityData(Result result) {

        long id = getId(result.getRow());
        int v = getInt(version, result);
        long ts = getLong(timestamp, result);
        long cs = getLong(changeset, result);

        Date date = new Date(ts);

        int anInt = getInt(uid, result);
        String string = getString(uname, result);

        OsmUser osmUser = OsmUser.NONE;
        if (anInt != -1) {
            osmUser = new OsmUser(anInt, string);
        }

        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(tags);

        Set<Tag> tags = new HashSet<Tag>(familyMap.size());
        for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
            String key = Bytes.toString(entry.getKey());
            String value = Bytes.toString(entry.getValue());
            tags.add(new Tag(key,value));
        }

        return new CommonEntityData(id, v, date, osmUser, cs, tags);

    }
}
