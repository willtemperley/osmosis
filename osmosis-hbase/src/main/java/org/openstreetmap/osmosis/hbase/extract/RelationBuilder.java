package org.openstreetmap.osmosis.hbase.extract;

import org.apache.hadoop.hbase.client.Get;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.container.v0_6.NodeContainer;
import org.openstreetmap.osmosis.core.container.v0_6.RelationContainer;
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
        entityContainers.add(new RelationContainer(relation));
        return getRelationMembers(relation, entityContainers);
    }

    public List<EntityContainer> getRelationMembers(Relation relation, List<EntityContainer> containers) {
        List<RelationMember> members = relation.getMembers();
        for (RelationMember member : members) {
            EntityType memberType = member.getMemberType();
            System.out.println("memberType = " + memberType);
            System.out.println("id = " + member.getMemberId());

            if (memberType.equals(EntityType.Node)) {
                Node node = nodeDao.get(member.getMemberId());
                containers.add(new NodeContainer(node));
            } else if (memberType.equals(EntityType.Way)) {
                Way way = wayDao.get(member.getMemberId());
                WayContainer wayContainer = new WayContainer(way);
                containers.add(wayContainer);

                List<WayNode> wayNodes = way.getWayNodes();
                long[] longs = new long[wayNodes.size()];
                for (int i = 0; i < wayNodes.size(); i++) {
                    longs[i] = wayNodes.get(i).getNodeId();
                }

                List<Node> nodes = nodeDao.get(longs);
                System.out.println("nodes = " + nodes.size());
                for (Node node : nodes) {
                    NodeContainer nodeContainer = new NodeContainer(node);
                    containers.add(nodeContainer);
                }
            } else if (memberType.equals(EntityType.Relation)) {
                Relation nestedRelation = relationDao.get(member.getMemberId());
                getRelationMembers(nestedRelation, containers);
            }
        }

        return containers;
    }

}
