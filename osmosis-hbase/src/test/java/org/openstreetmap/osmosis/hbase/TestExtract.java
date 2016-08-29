package org.openstreetmap.osmosis.hbase;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Test;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.container.v0_6.NodeContainer;
import org.openstreetmap.osmosis.core.container.v0_6.RelationContainer;
import org.openstreetmap.osmosis.core.container.v0_6.WayContainer;
import org.openstreetmap.osmosis.dataset.v0_6.DumpDataset;
import org.openstreetmap.osmosis.hbase.HBaseChangeWriter;
import org.openstreetmap.osmosis.hbase.MockHTableModule;
import org.openstreetmap.osmosis.hbase.TableModule;
import org.openstreetmap.osmosis.hbase.common.NodeDao;
import org.openstreetmap.osmosis.hbase.common.RelationDao;
import org.openstreetmap.osmosis.hbase.common.TableFactory;
import org.openstreetmap.osmosis.hbase.common.WayDao;
import org.openstreetmap.osmosis.hbase.extract.EntityListDumper;
import org.openstreetmap.osmosis.hbase.extract.RelationBuilder;
import org.openstreetmap.osmosis.hbase.reader.HBaseReader;
import org.openstreetmap.osmosis.testutil.AbstractDataTest;
import org.openstreetmap.osmosis.xml.common.CompressionMethod;
import org.openstreetmap.osmosis.xml.v0_6.XmlReader;
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
public class TestExtract extends AbstractDataTest {


    private String pathname = "E:/tmp/x.xml";



    Injector objectGraph = Guice.createInjector(new MockHTableModule());
    private long relationId = 1L;

//    Injector objectGraph = Guice.createInjector(new TableModule());
//    private long relationId = 1443024L;

    /**
     * http://www.openstreetmap.org/relation/1443024#map=9/-29.3151/29.5999
     *
     */
    @Test
    public void testRelation() throws IOException {


//        ExtractRelation extractRelation = objectGraph.getInstance(ExtractRelation.class);

//        Relation relation = extractRelation.get(1443024);
//        System.out.println("relation = " + relation);

        File snapshotFile;

        // Generate input files.
        snapshotFile = dataUtils.createDataFile("v0_6/db-snapshot.osm");

        //two for one
        HBaseChangeWriter changeWriter = objectGraph.getInstance(HBaseChangeWriter.class);

        //read
        XmlReader xmlReader = new XmlReader(snapshotFile, true, CompressionMethod.None);
        xmlReader.setSink(changeWriter);
        xmlReader.run();


        TableFactory tableFactory = objectGraph.getInstance(TableFactory.class);

        RelationBuilder relationBuilder = new RelationBuilder(tableFactory);
        List<EntityContainer> relation = relationBuilder.getRelation(relationId);

        EntityListDumper entityListDumper = new EntityListDumper(relation);
        writeHBaseDataToXml(entityListDumper, new File(pathname));

    }

    private void writeHBaseDataToXml(EntityListDumper dumpDataset, File actualResultFile) {

        XmlWriter writer = new XmlWriter(actualResultFile, CompressionMethod.None);


        //dataset sunk into sorter
        dumpDataset.setSink(writer);

        //sorter sunk into writer
        dumpDataset.process(null);
    }
}
