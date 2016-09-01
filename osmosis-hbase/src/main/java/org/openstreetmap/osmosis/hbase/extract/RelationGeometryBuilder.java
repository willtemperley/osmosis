package org.openstreetmap.osmosis.hbase.extract;

import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.xml.common.CompressionMethod;
import org.openstreetmap.osmosis.xml.v0_6.XmlReader;

import java.io.File;
import java.util.Map;

/**
 * Created by willtemperley@gmail.com on 26-Aug-16.
 */
public class RelationGeometryBuilder {


    public static void main(String[] args) {

        String x = "E:/osmosis/osmosis-hbase/src/test/resources/data/template/v0_6/Addo-relation-2715959.xml";
        File f = new File(x);
        XmlReader reader = new XmlReader(f, false, CompressionMethod.None);

        reader.setSink(new Sink() {
            @Override
            public void process(EntityContainer entityContainer) {
                System.out.println("entityContainer = " + entityContainer);
            }

            @Override
            public void initialize(Map<String, Object> metaData) {

            }

            @Override
            public void complete() {

            }

            @Override
            public void release() {

            }
        });

        reader.run();
    }
}
