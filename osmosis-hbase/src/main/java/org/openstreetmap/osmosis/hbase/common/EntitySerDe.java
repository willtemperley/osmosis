package org.openstreetmap.osmosis.hbase.common;

import com.google.common.primitives.Longs;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
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
public abstract class EntitySerDe<T extends Entity> {

    public static byte[] tags = "t".getBytes();
    public static byte[] data = "d".getBytes();

    private static byte[] version = "v".getBytes();
    private static byte[] timestamp = "ts".getBytes();
    private static byte[] changeset = "cs".getBytes();
    private static byte[] uid = "uid".getBytes();
    private static byte[] uname = "uname".getBytes();

    public CellGenerator getDataCellGenerator() {
        return dataKvGen;
    }

    private CellGenerator dataKvGen;

    private long getId(byte[] rowKey) {
        ArrayUtils.reverse(rowKey);
        return Longs.fromByteArray(rowKey);
    }

    public byte[] getRowKey(Entity entity) {
        long entityId = entity.getId();
        return getRowKey(entityId);
    }

    public byte[] getRowKey(long entityId) {
        byte[] bytes = Bytes.toBytes(entityId);
        ArrayUtils.reverse(bytes);
        return bytes;
    }

    private String getString(byte[] column, Result result) {
        return Bytes.toString(result.getValue(data, column));
    }
    private long getLong(byte[] column, Result result) {
        return Bytes.toLong(result.getValue(data, column));
    }
    private int getInt(byte[] column, Result result) {
        return Bytes.toInt(result.getValue(data, column));
    }
    double getDouble(byte[] column, Result result) { return Bytes.toDouble(result.getValue(data, column)); }

    /**
     * Generate cells for the main column family
     *
     * @param rowKey the entity row key
     * @param entity the OSM entity
     * @return all columns in the data col fam
     */
    public List<Cell> dataKeyValues(byte[] rowKey, T entity) {

        dataKvGen = new CellGenerator(data);

        List<Cell> keyValues = new ArrayList<Cell>();

        keyValues.add( dataKvGen.getKeyValue(rowKey, version, entity.getVersion()) );
        keyValues.add( dataKvGen.getKeyValue(rowKey, timestamp, entity.getTimestamp().getTime()) );
        keyValues.add( dataKvGen.getKeyValue(rowKey, changeset, entity.getChangesetId()) );
        keyValues.add( dataKvGen.getKeyValue(rowKey, uid, entity.getUser().getId()) );
        keyValues.add( dataKvGen.getKeyValue(rowKey, uname, entity.getUser().getName()) );

        encode(rowKey, entity, keyValues);

        return keyValues;
    }


    public List<Cell> tagKeyValues(byte[] rowKey, T entity) {

        List<Cell> keyValues = new ArrayList<Cell>();
        CellGenerator tagKvGen = new CellGenerator(tags);
        for (Tag tag : entity.getTags()) {
            keyValues.add( tagKvGen.getKeyValue(rowKey, tag.getKey().getBytes(), tag.getValue()) );
        }
        return keyValues;
    }

    Put serialize(T entity) {

        byte[] rowKey = getRowKey(entity);
        Put put = new Put(rowKey);

        NavigableMap<byte[], List<Cell>> familyCellMap = put.getFamilyCellMap();

        familyCellMap.put(data, dataKeyValues(rowKey, entity));
        familyCellMap.put(tags, tagKeyValues(rowKey, entity));


        return put;
    }

    protected abstract void encode(byte[] rowKey, T entity, List<Cell> keyValues);

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
            tags.add(new Tag(key, value));
        }

        return new CommonEntityData(id, v, date, osmUser, cs, tags);

    }
}
