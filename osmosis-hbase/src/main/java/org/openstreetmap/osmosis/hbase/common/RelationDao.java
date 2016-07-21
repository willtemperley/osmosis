package org.openstreetmap.osmosis.hbase.common;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;

/**
 * DAO for relations
 *
 * Created by willtemperley@gmail.com on 14-Jul-16.
 */
public class RelationDao extends EntityDao<Relation> {

    public RelationDao(Table table) {
        super(table);
    }

    @Override
    public EntitySerDe<Relation> getSerDe() {
        return new RelationSerDe();
    }


}
