package org.openstreetmap.osmosis.hbase.common;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.task.common.ChangeAction;

import java.io.IOException;

/**
 * DAOs and serdes are separated to allow reuse of SerDes in e.g. iterators
 *
 * Created by willtemperley@gmail.com on 13-Jul-16.
 */
public abstract class EntityDao<T extends Entity> {

    private final EntitySerDe<T> serde;

    EntityDao(Table table) {
        this.table = table;
        this.serde = getSerDe();
    }

    private final Table table;


    private byte[] getRowKey(Entity entity) {
        byte[] bytes = Bytes.toBytes(entity.getId());
        ArrayUtils.reverse(bytes);
        return bytes;
    }


    public void process(T entity, ChangeAction action) {

        if (action.equals(ChangeAction.Create)) {
            put(entity);
        } else if (action.equals(ChangeAction.Modify)) {
            put(entity);
        } else if (action.equals(ChangeAction.Delete)) {
            delete(entity);
        } else {
            throw new RuntimeException("Unknown action: " + action);
        }
    }

    private void delete(T entity) {

        Delete delete = new Delete(getRowKey(entity));
        try {
            table.delete(delete);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public boolean exists(T entity) {
        Get get = new Get(getRowKey(entity));
        try {
            return table.exists(get);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public void get(Entity entity) {

        Get get = new Get(getRowKey(entity));
        try {
            table.get(get);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }



    public void put(T entity) {

        //No point in having a whole load of machinery to process this
        if (entity.getType().equals(EntityType.Bound)) {
            return;
        }

        //A put with the timestamp of the entity
        Put put = new Put(getRowKey(entity));

        serde.serialize(put, entity);

        try {
            table.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }

    }

    public abstract EntitySerDe<T> getSerDe();

}
