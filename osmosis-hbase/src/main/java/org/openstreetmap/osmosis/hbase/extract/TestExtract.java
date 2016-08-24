package org.openstreetmap.osmosis.hbase.extract;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.container.v0_6.NodeContainer;
import org.openstreetmap.osmosis.core.container.v0_6.RelationContainer;
import org.openstreetmap.osmosis.core.container.v0_6.WayContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.*;
import org.openstreetmap.osmosis.hbase.TableModule;
import org.openstreetmap.osmosis.hbase.common.NodeDao;
import org.openstreetmap.osmosis.hbase.common.RelationDao;
import org.openstreetmap.osmosis.hbase.common.TableFactory;
import org.openstreetmap.osmosis.hbase.common.WayDao;
import org.openstreetmap.osmosis.xml.common.CompressionMethod;
import org.openstreetmap.osmosis.xml.v0_6.XmlWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility to extract a relation from db
 *
 * Created by willtemperley@gmail.com on 23-Aug-16.
 */
public class TestExtract  {


//    Injector objectGraph = Guice.createInjector(new MockHTableModule());
//    private long relationId = 1L;


    /**
     * http://www.openstreetmap.org/relation/1443024#map=9/-29.3151/29.5999
     *
     */
    public static void main(String[] args) throws IOException {

        String pathname = "/tmp/x.xml";
        Injector objectGraph = Guice.createInjector(new TableModule());
        long relationId = 1443024L;
//    }
//    @Test
//    public void testRelation() throws IOException {


//        ExtractRelation extractRelation = objectGraph.getInstance(ExtractRelation.class);

//        Relation relation = extractRelation.get(1443024);
//        System.out.println("relation = " + relation);

//        File snapshotFile;

        // Generate input files.
//        snapshotFile = dataUtils.createDataFile("v0_6/db-snapshot.osm");

        //two for one
//        HBaseChangeWriter changeWriter = objectGraph.getInstance(HBaseChangeWriter.class);

        //read
//        XmlReader xmlReader = new XmlReader(snapshotFile, true, CompressionMethod.None);
//        xmlReader.setSink(changeWriter);
//        xmlReader.run();


        TableFactory tableFactory = objectGraph.getInstance(TableFactory.class);

        RelationDao relationDao = new RelationDao(tableFactory.getTable("relations"));
        WayDao wayDao = new WayDao(tableFactory.getTable("ways"));
        NodeDao nodeDao = new NodeDao(tableFactory.getTable("nodes"));


        Relation relation = relationDao.get(relationId);
        System.out.println("relation = " + relation);

        List<EntityContainer> l = new ArrayList<EntityContainer>();
        RelationContainer relationContainer = new RelationContainer(relation);
        l.add(relationContainer);

        List<RelationMember> members = relation.getMembers();
        for (RelationMember member : members) {
            if (member.getMemberType().equals(EntityType.Way)) {
                Way way = wayDao.get(member.getMemberId());
                WayContainer wayContainer = new WayContainer(way);
                l.add(wayContainer);

                List<WayNode> wayNodes = way.getWayNodes();
                for (WayNode wayNode : wayNodes) {
                    long nodeId = wayNode.getNodeId();
                    Node node = nodeDao.get(nodeId);
                    NodeContainer nodeContainer = new NodeContainer(node);
                    l.add(nodeContainer);
                }
            }
        }

        EntityListDumper entityListDumper = new EntityListDumper(l);
        writeHBaseDataToXml(entityListDumper, new File(pathname));

    }

    private static void writeHBaseDataToXml(EntityListDumper dumpDataset, File actualResultFile) {

        XmlWriter writer = new XmlWriter(actualResultFile, CompressionMethod.None);


        //dataset sunk into sorter
        dumpDataset.setSink(writer);

        //sorter sunk into writer
        dumpDataset.process(null);
    }
}
