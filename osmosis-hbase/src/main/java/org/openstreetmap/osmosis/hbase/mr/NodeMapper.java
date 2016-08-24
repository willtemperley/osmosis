package org.openstreetmap.osmosis.hbase.mr;

import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.hbase.common.NodeSerDe;

/**
 * Maps nodes to hbase key-values
 *
 * Created by willtemperley@gmail.com on 28-Jul-16.
 */
public class NodeMapper extends OsmEntityMapper<Node> {
    public NodeMapper() {
        super(EntityType.Node, new NodeSerDe());
    }
}
