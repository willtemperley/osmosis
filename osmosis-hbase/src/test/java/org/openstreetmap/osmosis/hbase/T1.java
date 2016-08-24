package org.openstreetmap.osmosis.hbase;

import org.junit.Test;
import org.openstreetmap.osmosis.hbase.common.RelationDao;

/**
 * Created by willtemperley@gmail.com on 24-Aug-16.
 */
public class T1 {

    @Test
    public void x() {

        long relationId = 1443024L;

        RelationDao relationDao = new RelationDao(null);

        relationDao.get(relationId);

        System.out.println("hi");
//        relationId.
    }
}
