package org.openstreetmap.osmosis.hbase.common;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.client.*;
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

        Delete delete = new Delete(serde.getRowKey(entity));
        try {
            table.delete(delete);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

//    public boolean exists(T entity) {
//        Get get = new Get(serde.getRowKey(entity));
//        try {
//            return table.exists(get);
//        } catch (IOException e) {
//            throw new RuntimeException(e.getMessage());
//        }
//    }


    //FIXME multiple gets will be required too
    public T get(long entityId) {


        Get get = new Get(serde.getRowKey(entityId));
        try {

            Result result = table.get(get);
            if (result == null) {
                System.out.println("table = " + table.getName());
                System.out.println("result = " + null);
            }
            return serde.deSerialize(result);

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

        Put put = serde.serialize(entity);

        try {
            table.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }

    }

    public abstract EntitySerDe<T> getSerDe();

}
