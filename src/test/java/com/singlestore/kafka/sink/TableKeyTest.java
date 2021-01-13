package com.singlestore.kafka.sink;

import com.singlestore.kafka.utils.TableKey;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class TableKeyTest {

    @Test
    public void primary() {
        assertEquals((new TableKey(TableKey.Type.PRIMARY, "name", new ArrayList<>(Arrays.asList("id1", "id2")))).toString(), "PRIMARY KEY `name`(`id1`, `id2`)");
        assertEquals((new TableKey(TableKey.Type.PRIMARY, "", new ArrayList<>(Arrays.asList("id1", "id2")))).toString(), "PRIMARY KEY (`id1`, `id2`)");
    }

    @Test
    public void columnstore() {
        assertEquals((new TableKey(TableKey.Type.COLUMNSTORE, "name", new ArrayList<>(Arrays.asList("id1", "id2")))).toString(), "KEY `name`(`id1`, `id2`) USING CLUSTERED COLUMNSTORE");
        assertEquals((new TableKey(TableKey.Type.COLUMNSTORE, "", new ArrayList<>(Arrays.asList("id1", "id2")))).toString(), "KEY (`id1`, `id2`) USING CLUSTERED COLUMNSTORE");
    }

    @Test
    public void shard() {
        assertEquals((new TableKey(TableKey.Type.SHARD, "name", new ArrayList<>(Arrays.asList("id1", "id2")))).toString(), "SHARD KEY `name`(`id1`, `id2`)");
        assertEquals((new TableKey(TableKey.Type.SHARD, "", new ArrayList<>(Arrays.asList("id1", "id2")))).toString(), "SHARD KEY (`id1`, `id2`)");
    }

    @Test
    public void unique() {
        assertEquals((new TableKey(TableKey.Type.UNIQUE, "name", new ArrayList<>(Arrays.asList("id1", "id2")))).toString(), "UNIQUE KEY `name`(`id1`, `id2`)");
        assertEquals((new TableKey(TableKey.Type.UNIQUE, "", new ArrayList<>(Arrays.asList("id1", "id2")))).toString(), "UNIQUE KEY (`id1`, `id2`)");
    }


    @Test
    public void key() {
        assertEquals((new TableKey(TableKey.Type.KEY, "name", new ArrayList<>(Arrays.asList("id1", "id2")))).toString(), "KEY `name`(`id1`, `id2`)");
        assertEquals((new TableKey(TableKey.Type.KEY, "", new ArrayList<>(Arrays.asList("id1", "id2")))).toString(), "KEY (`id1`, `id2`)");
    }
}
