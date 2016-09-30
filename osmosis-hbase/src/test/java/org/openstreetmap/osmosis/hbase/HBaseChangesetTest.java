package org.openstreetmap.osmosis.hbase;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.KeyValueSerialization;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.dataset.v0_6.DumpDataset;
import org.openstreetmap.osmosis.hbase.common.EntityDataAccess;
import org.openstreetmap.osmosis.hbase.common.TableFactory;
import org.openstreetmap.osmosis.hbase.mr.*;
import org.openstreetmap.osmosis.hbase.mr.writable.OsmEntityWritable;
import org.openstreetmap.osmosis.hbase.reader.HBaseReader;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.PbfRawBlob;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.PbfStreamSplitter;
import org.openstreetmap.osmosis.testutil.AbstractDataTest;
import org.openstreetmap.osmosis.xml.common.CompressionMethod;
import org.openstreetmap.osmosis.xml.v0_6.XmlChangeReader;
import org.openstreetmap.osmosis.xml.v0_6.XmlReader;
import org.openstreetmap.osmosis.xml.v0_6.XmlWriter;
import org.xmlunit.builder.DiffBuilder;
import org.xmlunit.diff.Diff;
import org.xmlunit.diff.Difference;

import java.io.*;
import java.util.List;

/**
 * Tests the loading and application of a changeset works correctly.
 *
 * The piplines have been hand-coded, as this allows mock tables to persist in-memory between operations
 *
 * Created by willtemperley@gmail.com on 13-Jul-16.
 */
public class HBaseChangesetTest extends AbstractDataTest {


    protected Injector injector;

    /**
     * Prints the difference between two xml files
     * @param control expected file
     * @param test observed file
     */
    public void diffFiles(File control, File test) throws IOException {

        Diff myDiff = DiffBuilder.compare(control).withTest(test)
                .build();

        Iterable<Difference> x = myDiff.getDifferences();
        for (Difference difference : x) {
            System.out.println(difference);
        }
    }


    /**
     * Loads a binary snapshot file into a set of mock HTables using MapReduce.
     *
     * This is intended to just test the mappers are creating the correct KeyValues.
     * Loading to HBase is conducted merely to allow subsequent querying and serialization to xml.
     * The serialized xml should be identical to the dataset xml.
     *
     * Note the db-snapshot.pbf test file was created from the db-snapshot.osm
     *
     * osmosis --read-xml file="/tmp/db-snapshot.osm" --write-pbf file="/tmp/db-snapshot.pbf"
     *
     */
    @Test
    public void testTableLoading() throws IOException {

        //Snapshot binary file which contains same info as the xml version
        File snapshotXmlFile = dataUtils.createDataFile("v0_6/db-snapshot.osm");
        File snapshotBinaryFile = new File("src/test/resources/data/template/v0_6/db-snapshot.pbf");
        File actualResultFile = dataUtils.newFile();

        //Seems to require three passes, can't see another way - can only create HFiles for one table
        loadTable(snapshotBinaryFile, "ways", new WayMapper());
        loadTable(snapshotBinaryFile, "nodes", new NodeMapper());
        loadTable(snapshotBinaryFile, "relations", new RelationMapper());

        //read from mocktables and sort
        writeHBaseDataToXml(actualResultFile);

        // Validate that the dumped file matches the expected result.
        diffFiles(snapshotXmlFile, actualResultFile);

        dataUtils.compareFiles(snapshotXmlFile, actualResultFile);

    }



    /**
     * Gets an hbase reader which will have the singleton tablefactory injected and writes out the data to xml for comparison purposes
     * 
     * @param actualResultFile the file to write to compare results of crud ops
     */
    private void writeHBaseDataToXml(File actualResultFile) {
        
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
    }

    /**
     *
     * @throws IOException
     */
    @Test
    public void mutation() throws IOException {

        File snapshotFile;

        // Generate input files.
        snapshotFile = dataUtils.createDataFile("v0_6/db-snapshot.osm");
        //two for one
        HBaseWriter changeWriter = injector.getInstance(HBaseWriter.class);

        //read
        XmlReader xmlReader = new XmlReader(snapshotFile, true, CompressionMethod.None);
        xmlReader.setSink(changeWriter);
        xmlReader.run();

        TableFactory hTableFact = injector.getInstance(TableFactory.class);
        MapReduceDriver<ImmutableBytesWritable, Result, ImmutableBytesWritable, OsmEntityWritable, ImmutableBytesWritable, Mutation> mapReduceDriver = MapReduceDriver.newMapReduceDriver();

        mapReduceDriver.setMapper(new WayTableLoader().new SiteGridMapper());
        mapReduceDriver.setReducer(new WayTableLoader().new SiteGridReducer());

        setupSerialization(mapReduceDriver);

        Table ways = hTableFact.getTable("ways");
        Table nodes = hTableFact.getTable("nodes");
        Scan scan = new Scan().addFamily(EntityDataAccess.data).addFamily(EntityDataAccess.tags);
        ResultScanner wayScanner = ways.getScanner(scan);
        ResultScanner nodeScanner = nodes.getScanner(scan);
        ImmutableBytesWritable row = new ImmutableBytesWritable();
        for (Result result : wayScanner) {
            row.set(result.getRow());
            mapReduceDriver.withInput(row, result);
        }
        for (Result result : nodeScanner) {
            row.set(result.getRow());
            mapReduceDriver.withInput(row, result);
        }

        mapReduceDriver.run();

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
        HBaseWriter changeWriter = injector.getInstance(HBaseWriter.class);

        //read
        XmlReader xmlReader = new XmlReader(snapshotFile, true, CompressionMethod.None);
        xmlReader.setSink(changeWriter);
        xmlReader.run();

        //read change
        XmlChangeReader changeReader = new XmlChangeReader(changesetFile, true, CompressionMethod.None);
        changeReader.setChangeSink(changeWriter);
        changeReader.run();

        //read from mocktables and sort
        writeHBaseDataToXml(actualResultFile);

        // Validate that the dumped file matches the expected result.
        dataUtils.compareFiles(expectedResultFile, actualResultFile);
    }

    @Before
    public void init() throws Exception {
        injector = Guice.createInjector(new MockHTableModule());
    }

    /**
     * Loads a table via mapreduce into the specified table
     *
     * @param snapshotBinaryFile the source file
     * @param tableName the table to load
     * @param mapper the mapper to use. Reducer is just a way of collecting the table insertions.
     * @throws IOException y
     */
    protected <T extends Entity> void loadTable(File snapshotBinaryFile, String tableName, OsmEntityMapper<T> mapper) throws IOException {

        TableFactory hTableFact = injector.getInstance(TableFactory.class);

        MapReduceDriver<Text, ArrayPrimitiveWritable, ImmutableBytesWritable, Cell, ImmutableBytesWritable, Put> mapReduceDriver = MapReduceDriver.newMapReduceDriver();
        Table mockTable = hTableFact.getTable(tableName);

        CellReducer cellReducer = new CellReducer();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(cellReducer);

        //Set up config with some settings that would normally be set in HFileOutputFormat2.configureIncrementalLoad();
        setupSerialization(mapReduceDriver);


        InputStream inputStream = new FileInputStream(snapshotBinaryFile);
        PbfStreamSplitter streamSplitter = new PbfStreamSplitter(new DataInputStream(inputStream));

        ArrayPrimitiveWritable arrayPrimitiveWritable = new ArrayPrimitiveWritable();
        Text text = new Text();

        while (streamSplitter.hasNext()) {
            PbfRawBlob blob = streamSplitter.next();
            arrayPrimitiveWritable.set(blob.getData());
            String type = blob.getType();
            text.set(type);
            mapReduceDriver.withInput(text, arrayPrimitiveWritable);
        }

        //Retrieve MR results
        List<Pair<ImmutableBytesWritable, Put>> results = mapReduceDriver.run();
        for (Pair<ImmutableBytesWritable, Put> cellPair : results) {
            mockTable.put(cellPair.getSecond());
        }
    }

    /*
    On a cluster this would already be set.
     */
    private void setupSerialization(MapReduceDriver<?, ?, ?, ?, ?, ?> mapReduceDriver) {
        Configuration configuration = mapReduceDriver.getConfiguration();
        configuration.setStrings("io.serializations", configuration.get("io.serializations"),
                MutationSerialization.class.getName(), ResultSerialization.class.getName(),
                KeyValueSerialization.class.getName());
    }
}
