package com.memsql.kafka.sink;

import com.memsql.kafka.utils.TableKey;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TableKeyTest {
    @Test
    public void primary() {
        assertEquals((new TableKey(TableKey.Type.PRIMARY, "name", "id1, id2")).toString(), "PRIMARY KEY `name`(id1, id2)");
        assertEquals((new TableKey(TableKey.Type.PRIMARY, "", "id1, id2")).toString(), "PRIMARY KEY (id1, id2)");
    }

    @Test
    public void columnstore() {
        assertEquals((new TableKey(TableKey.Type.COLUMNSTORE, "name", "id1, id2")).toString(), "KEY `name`(id1, id2) USING CLUSTERED COLUMNSTORE");
        assertEquals((new TableKey(TableKey.Type.COLUMNSTORE, "", "id1, id2")).toString(), "KEY (id1, id2) USING CLUSTERED COLUMNSTORE");
    }

    @Test
    public void shard() {
        assertEquals((new TableKey(TableKey.Type.SHARD, "name", "id1, id2")).toString(), "SHARD KEY `name`(id1, id2)");
        assertEquals((new TableKey(TableKey.Type.SHARD, "", "id1, id2")).toString(), "SHARD KEY (id1, id2)");
    }

    @Test
    public void unique() {
        assertEquals((new TableKey(TableKey.Type.UNIQUE, "name", "id1, id2")).toString(), "UNIQUE KEY `name`(id1, id2)");
        assertEquals((new TableKey(TableKey.Type.UNIQUE, "", "id1, id2")).toString(), "UNIQUE KEY (id1, id2)");
    }


    @Test
    public void key() {
        assertEquals((new TableKey(TableKey.Type.KEY, "name", "id1, id2")).toString(), "KEY `name`(id1, id2)");
        assertEquals((new TableKey(TableKey.Type.KEY, "", "id1, id2")).toString(), "KEY (id1, id2)");
    }
}
