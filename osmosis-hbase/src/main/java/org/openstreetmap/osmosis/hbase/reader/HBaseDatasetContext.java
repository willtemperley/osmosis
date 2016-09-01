package org.openstreetmap.osmosis.hbase.reader;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.openstreetmap.osmosis.core.OsmosisConstants;
import org.openstreetmap.osmosis.core.container.v0_6.DatasetContext;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.container.v0_6.EntityManager;
import org.openstreetmap.osmosis.core.domain.v0_6.*;
import org.openstreetmap.osmosis.core.lifecycle.ReleasableIterator;
import org.openstreetmap.osmosis.core.store.MultipleSourceIterator;
import org.openstreetmap.osmosis.hbase.common.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A very basic view on the entire dataset.  Not to be used on the full dataset!
 *
 * Created by willtemperley@gmail.com on 15-Jul-16.
 */
public class HBaseDatasetContext implements DatasetContext {

    private final TableFactory tableFactory;

    public HBaseDatasetContext(TableFactory tableFactory) {
        this.tableFactory = tableFactory;
    }

    @Override
    public void complete() {

    }

    @Override
    public EntityManager<Node> getNodeManager() {
        return null;
    }

    @Override
    public EntityManager<Way> getWayManager() {
        return null;
    }

    @Override
    public EntityManager<Relation> getRelationManager() {
        return null;
    }

    @Override
    public Node getNode(long id) {
        return null;
    }

    @Override
    public Way getWay(long id) {
        return null;
    }

    @Override
    public Relation getRelation(long id) {
        throw new NotImplementedException();
    }

    @Override
    public ReleasableIterator<EntityContainer> iterate() {

        try {

            List<ReleasableIterator<EntityContainer>> iterators = new ArrayList<ReleasableIterator<EntityContainer>>();

            iterators.add(new BoundsIterator(new Bound("Osmosis " + OsmosisConstants.VERSION)));
            iterators.add(getHBaseIterator("ways", new WaySerDe()));
            iterators.add(getHBaseIterator("nodes", new NodeSerDe()));
            iterators.add(getHBaseIterator("relations", new RelationSerDe()));

            return new MultipleSourceIterator<EntityContainer>(iterators);


        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    private <T extends Entity> HBaseIterator<T> getHBaseIterator(String tableName, EntitySerDe<T> serDe) throws IOException {
        Scan scan = new Scan();
        scan.addFamily(EntityDataAccess.data);
        scan.addFamily(EntityDataAccess.tags);
        Table table = tableFactory.getTable(tableName);
        return new HBaseIterator<T>(table.getScanner(scan), serDe);
    }

    @Override
    public ReleasableIterator<EntityContainer> iterateBoundingBox(double left, double right, double top, double bottom, boolean completeWays) {
        throw new NotImplementedException();
    }

    @Override
    public void release() {

    }
}
