package org.openstreetmap.osmosis.hbase.common;

import com.google.common.primitives.Longs;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;

/**
 * Constants and basic methods for OSM data access
 *
 * Created by willtemperley@gmail.com on 31-Aug-16.
 */
public class EntityDataAccess {

    public static byte[] tags = "t".getBytes();
    public static byte[] data = "d".getBytes();
    protected static byte[] version = "v".getBytes();
    protected static byte[] timestamp = "ts".getBytes();
    protected static byte[] changeset = "cs".getBytes();
    protected static byte[] uid = "uid".getBytes();
    protected static byte[] uname = "uname".getBytes();

    public static long getId(byte[] rowKey) {
        ArrayUtils.reverse(rowKey);
        return Longs.fromByteArray(rowKey);
    }

    public static byte[] getRowKey(Entity entity) {
        long entityId = entity.getId();
        return getRowKey(entityId);
    }

    public static byte[] getRowKey(long entityId) {
        byte[] bytes = Bytes.toBytes(entityId);
        ArrayUtils.reverse(bytes);
        return bytes;
    }

    protected static String getString(byte[] column, Result result) {
        return Bytes.toString(result.getValue(data, column));
    }

    protected static long getLong(byte[] column, Result result) {
        return Bytes.toLong(result.getValue(data, column));
    }

    protected static int getInt(byte[] column, Result result) {
        return Bytes.toInt(result.getValue(data, column));
    }

    protected static double getDouble(byte[] column, Result result) {
        return Bytes.toDouble(result.getValue(data, column));
    }

}
