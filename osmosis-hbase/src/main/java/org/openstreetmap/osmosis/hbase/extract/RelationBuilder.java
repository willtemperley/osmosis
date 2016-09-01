package org.openstreetmap.osmosis.hbase.extract;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang.NotImplementedException;
import org.geojson.Feature;
import org.geojson.FeatureCollection;
import org.geojson.LngLatAlt;
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
 * Builds a relation
 *
 * TODO: proper test with nested nodes, ways and relations
 *
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

    public Feature getWay(long wayId) throws JsonProcessingException {

        Way way = wayDao.get(wayId);

        List<Node> nodes = getNodes(way);

        LngLatAlt[] lngLatAlts = new LngLatAlt[nodes.size()];
        for (int i = 0; i < nodes.size(); i++) {
            Node node = nodes.get(i);
            LngLatAlt lngLatAlt = new LngLatAlt(node.getLongitude(), node.getLatitude());
            lngLatAlts[i] = lngLatAlt;
        }

        Feature feature = new Feature();
        if (PolygonDetector.isWayPolygon(way)) {
            feature.setGeometry(new org.geojson.Polygon(lngLatAlts));
        } else {
            feature.setGeometry(new org.geojson.LineString(lngLatAlts));
        }

        for (Tag tag : way.getTags()) {
            feature.setProperty(tag.getKey(), tag.getValue());
        }

        return feature;
    }

    /**
     *
     *
     * @param relationId
     * @return
     * @throws JsonProcessingException
     */
    public FeatureCollection getRelationAsJson(long relationId) throws JsonProcessingException {

        /**
         * is there some mini graph lib somewhere please?
         *
         * Should be a DCG
         *
         * http://www.openstreetmap.org/relation/87565
         *
         *
         */

        FeatureCollection featureCollection = new FeatureCollection();
        Relation relation = relationDao.get(relationId);

//        if (relation.g)

        List<Way> inners = new ArrayList<Way>();

        List<RelationMember> members = relation.getMembers();
        for (RelationMember member : members) {
            EntityType memberType = member.getMemberType();


            if (memberType.equals(EntityType.Node)) {
                Node node = nodeDao.get(member.getMemberId());
                throw new NotImplementedException("Node in relation");
            } else if (memberType.equals(EntityType.Way)) {

                if (member.getMemberRole().equals("inner")) {

//                    inners.add();
//                } else if (member.getMemberRole().equals("outer")) {

                }
//                Way way = wayDao.get(member.getMemberId());
                Feature way = getWay(member.getMemberId());
                featureCollection.add(way);

            } else if (memberType.equals(EntityType.Relation)) {
                System.out.println("Todo nested " + memberType);
//                Relation nestedRelation = relationDao.get(member.getMemberId());
//                getRelationMembers(nestedRelation, containers);
            }
        }

        return featureCollection;
    }

    public List<EntityContainer> getRelationMembers(Relation relation, List<EntityContainer> containers) {
        containers.add(new RelationContainer(relation));
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

                List<Node> nodes = getNodes(way);

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

    private List<Node> getNodes(Way way) {
        List<WayNode> wayNodes = way.getWayNodes();
        long[] longs = new long[wayNodes.size()];
        for (int i = 0; i < wayNodes.size(); i++) {
            longs[i] = wayNodes.get(i).getNodeId();
        }

        return nodeDao.get(longs);
    }

}
