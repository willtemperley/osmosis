package org.openstreetmap.osmosis.hbase.common;

import org.apache.hadoop.hbase.client.Table;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;

/**
 *
 * Created by willtemperley@gmail.com on 13-Jul-16.
 */
public class WayDao extends EntityDao<Way> {

    public WayDao(Table table) {
        super(table);
    }

    @Override
    public EntitySerDe<Way> getSerDe() {
        return new WaySerDe();
    }

}
