package org.openstreetmap.osmosis.hbase.mr;

import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.hbase.common.WaySerDe;

/**
 * Created by willtemperley@gmail.com on 28-Jul-16.
 */
public class WayMapper extends OsmEntityMapper<Way> {
    public WayMapper() {
        super(EntityType.Way, new WaySerDe());
    }
}
