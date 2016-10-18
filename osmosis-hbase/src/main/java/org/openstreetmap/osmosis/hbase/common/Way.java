package org.openstreetmap.osmosis.hbase.common;

import org.openstreetmap.osmosis.core.domain.v0_6.CommonEntityData;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;

import java.util.List;

/**
 * Created by willtemperley@gmail.com on 18-Oct-16.
 */
public class Way extends org.openstreetmap.osmosis.core.domain.v0_6.Way implements Entity {

    public Way(org.openstreetmap.osmosis.core.domain.v0_6.Way relation) {
        super(new CommonEntityData(relation.getId(),relation.getVersion(), relation.getTimestamp(), relation.getUser(), relation.getChangesetId(), relation.getTags()), relation.getWayNodes());
    }

    public Way(CommonEntityData entityData, List<WayNode> wayNodes) {
        super(entityData, wayNodes);
    }

}
