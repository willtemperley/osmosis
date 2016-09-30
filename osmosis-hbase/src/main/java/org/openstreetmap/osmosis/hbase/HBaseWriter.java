// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.hbase;

import java.io.IOException;
import java.util.Map;

import com.google.inject.Inject;
import org.apache.hadoop.hbase.client.Table;
import org.openstreetmap.osmosis.core.container.v0_6.*;
import org.openstreetmap.osmosis.core.domain.v0_6.*;
import org.openstreetmap.osmosis.core.task.common.ChangeAction;
import org.openstreetmap.osmosis.core.task.v0_6.ChangeSink;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.hbase.common.*;


/**
 * A change sink writing to hbase tables. This aims to be suitable for
 * running at regular intervals with database overhead proportional to changeset
 * size.
 * 
 * @author willtemperley@gmail.com
 */
public class HBaseWriter implements ChangeSink, Sink {

    private final TableFactory tableFactory;

    private boolean keepInvalidWays;
    private final RelationDao relationDao;
    private final NodeDao nodeDao;
    private final WayDao wayDao;


    @Inject
	public HBaseWriter(TableFactory tableFactory) throws IOException {

	    this.tableFactory = tableFactory;

	    Table ways = tableFactory.getTable("ways");
        Table nodes = tableFactory.getTable("nodes");
        Table relations = tableFactory.getTable("relations");

        wayDao = new WayDao(ways);
        nodeDao = new NodeDao(nodes);
        relationDao = new RelationDao(relations);

	}
	

    /**
     * {@inheritDoc}
     */
    public void initialize(Map<String, Object> metaData) {
		// Do nothing.
    }


	/**
	 * {@inheritDoc}
	 */
	public void process(ChangeContainer change) {

		ChangeAction action = change.getAction();
        EntityContainer entityContainer = change.getEntityContainer();
        Entity entity = entityContainer.getEntity();

        process(entity, action);


	}


	private void process(Entity entity, ChangeAction action) {

        //bounds aren't processed
        EntityType entityType = entity.getType();

        if (entityType.equals(EntityType.Node)) {
            nodeDao.process((Node) entity, action);
        } else if (entityType.equals(EntityType.Way)) {
            wayDao.process((Way) entity, action);
        } else if (entityType.equals(EntityType.Relation)) {
            relationDao.process((Relation) entity, action);
        }
    }

    @Override
    public void process(EntityContainer entityContainer) {

        Entity entity = entityContainer.getEntity();

        process(entity, ChangeAction.Create);
    }

	
	/**
	 * {@inheritDoc}
	 */
	public void complete() {

	}
	
	
	/**
	 * {@inheritDoc}
	 */
	public void release() {

        tableFactory.close();

	}

    public void keepInvalidWays(boolean keepInvalidWays) {
        this.keepInvalidWays = keepInvalidWays;
    }
}
