package org.openstreetmap.osmosis.hbase;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.osmosis.dataset.v0_6.DumpDataset;
import org.openstreetmap.osmosis.hbase.reader.HBaseReader;
import org.openstreetmap.osmosis.testutil.AbstractDataTest;
import org.openstreetmap.osmosis.xml.common.CompressionMethod;
import org.openstreetmap.osmosis.xml.v0_6.XmlChangeReader;
import org.openstreetmap.osmosis.xml.v0_6.XmlReader;
import org.openstreetmap.osmosis.xml.v0_6.XmlWriter;

import java.io.File;
import java.io.IOException;

/**
 * Tests the loading and application of a dataset works correctly.
 *
 * The piplines have been hand-coded, as this allows mock tables to persist in-memory between operations
 *
 * Created by willtemperley@gmail.com on 13-Jul-16.
 */
public class HBaseChangesetTest extends AbstractDataTest {

    private Injector injector;

    @Before
    public void init() throws Exception {
        injector = Guice.createInjector(new MockHTableModule());
    }

    @After
    public void close() throws Exception {
    }

    @Test
    public void changeset() throws IOException {

        File snapshotFile;
        File changesetFile;
        File expectedResultFile;
        File actualResultFile;

        // Generate input files.
        snapshotFile = dataUtils.createDataFile("v0_6/db-snapshot.osm");
        changesetFile = dataUtils.createDataFile("v0_6/db-changeset.osc");
        expectedResultFile = dataUtils.createDataFile("v0_6/db-changeset-expected.osm");
        actualResultFile = dataUtils.newFile();

        //two for one
        HBaseChangeWriter changeWriter = injector.getInstance(HBaseChangeWriter.class);

        //read
        XmlReader xmlReader = new XmlReader(snapshotFile, true, CompressionMethod.None);
        xmlReader.setSink(changeWriter);
        xmlReader.run();

        //read change
        XmlChangeReader changeReader = new XmlChangeReader(changesetFile, true, CompressionMethod.None);
        changeReader.setChangeSink(changeWriter);
        changeReader.run();

        //read from mocktables and sort
        XmlWriter writer = new XmlWriter(actualResultFile, CompressionMethod.None);
        HBaseReader hBaseReader = injector.getInstance(HBaseReader.class);
        DumpDataset dumpDataset = new DumpDataset();

        //sort
        TestEntitySorter sorter = new TestEntitySorter();

        //dataset sunk into sorter
        dumpDataset.setSink(sorter);

        //sorter sunk into writer
        sorter.setSink(writer);

        sorter.complete();
        dumpDataset.process(hBaseReader);


//        // Load the database with the snapshot file.
//        Osmosis.run(
//                new String [] {
//                        "-q",
//                        "--read-xml-0.6",
//                        snapshotFile.getPath(),
//                        "--write-hbase-0.6"
//                }
//        );
//
//        // Apply the changeset file to the database.
//        Osmosis.run(
//                new String [] {
//                        "-q",
//                        "--read-xml-change-0.6",
//                        changesetFile.getPath(),
//                        "--write-hbase-change-0.6",
//                        "keepInvalidWays=false"
//                }
//        );
//
//        // Dump the database to an osm file.
//        Osmosis.run(
//                new String [] {
//                        "-q",
//                        "--read-hbase-0.6",
//                        "--dataset-dump-0.6",
//                        "--tag-sort-0.6",
//                        "--write-xml-0.6",
//                        actualResultFile.getPath()
//                }
//        );

        // Validate that the dumped file matches the expected result.
        dataUtils.compareFiles(expectedResultFile, actualResultFile);
    }
}
