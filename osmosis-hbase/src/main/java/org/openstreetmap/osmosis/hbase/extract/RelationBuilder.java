package org.openstreetmap.osmosis.hbase.extract;

import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.container.v0_6.NodeContainer;
import org.openstreetmap.osmosis.core.container.v0_6.WayContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.*;
import org.openstreetmap.osmosis.hbase.common.NodeDao;
import org.openstreetmap.osmosis.hbase.common.RelationDao;
import org.openstreetmap.osmosis.hbase.common.TableFactory;
import org.openstreetmap.osmosis.hbase.common.WayDao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by willtemperley@gmail.com on 29-Aug-16.
 */
public class RelationBuilder {

    private final RelationDao relationDao;
    private final WayDao wayDao;
    private final NodeDao nodeDao;

    public RelationBuilder(TableFactory tableFactory) throws IOException {

        relationDao = new RelationDao(tableFactory.getTable("relations"));
        wayDao = new WayDao(tableFactory.getTable("ways"));
        nodeDao = new NodeDao(tableFactory.getTable("nodes"));

    }

    public List<EntityContainer> getRelation(long relationId) {
        Relation relation = relationDao.get(relationId);
        List<EntityContainer> entityContainers = new ArrayList<EntityContainer>();
        return getRelationMembers(relation, entityContainers);
    }

    public List<EntityContainer> getRelationMembers(Relation relation, List<EntityContainer> containers) {
        List<RelationMember> members = relation.getMembers();
        for (RelationMember member : members) {
            EntityType memberType = member.getMemberType();
            if (memberType.equals(EntityType.Way)) {
                Way way = wayDao.get(member.getMemberId());
                WayContainer wayContainer = new WayContainer(way);
                containers.add(wayContainer);

                List<WayNode> wayNodes = way.getWayNodes();
                for (WayNode wayNode : wayNodes) {
                    long nodeId = wayNode.getNodeId();
                    Node node = nodeDao.get(nodeId);
                    NodeContainer nodeContainer = new NodeContainer(node);
                    containers.add(nodeContainer);
                }
            } else if (memberType.equals(EntityType.Relation)) {
                getRelationMembers(relation, containers);
            }
        }

        return containers;
    }

}
