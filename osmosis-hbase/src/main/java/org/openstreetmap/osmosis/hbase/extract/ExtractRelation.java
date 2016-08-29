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
import java.util.List;

/**
 * Utility to extract a relation from db
 *
 * Created by willtemperley@gmail.com on 23-Aug-16.
 */
public class ExtractRelation {


    /**
     * http://www.openstreetmap.org/relation/1443024#map=9/-29.3151/29.5999
     *
     */
    public static void main(String[] args) throws IOException {

        String pathname = "/tmp/x.xml";
        Injector objectGraph = Guice.createInjector(new TableModule());
        long relationId = 2715959;

        TableFactory tableFactory = objectGraph.getInstance(TableFactory.class);

        RelationBuilder relationBuilder = new RelationBuilder(tableFactory);

        List<EntityContainer> relation = relationBuilder.getRelation(relationId);

        EntityListDumper entityListDumper = new EntityListDumper(relation);
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
