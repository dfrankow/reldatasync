package org.maventy.reldatasync;

import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;

public class TestDocument {
    @Test
    public void test1() {
        Document doc = new Document(
            new HashMap<String, Object>() {{
                put(Document.ID, "1");
                put("string1", "value1");
                put("int1", 10);
            }}
        );
        assertEquals("1", doc.get(Document.ID));
        assertEquals("value1", doc.get("string1"));
        assertEquals(10, doc.get("int1"));

        assertEquals(
                "{\"int1\":10,\"string1\":\"value1\",\"_id\":\"1\"}",
                doc.toJsonString());

        assertEquals(doc, doc);
        assertFalse(doc.equals(null));

        // Shallow clone works
        Document doc2 = doc.clone();
        // Not the same object
        assertNotSame(doc, doc2);
        // But equals is the same
        assertEquals(doc, doc2);

        // Deep copy also works
        Document doc3 = new Document(
                new HashMap<String, Object>() {{
                    put(Document.ID, "1");
                    // Use "new String" so the string is not "interned"
                    // (i.e. kept in a master list by the JVM), fixing ==
                    put("string1", new String("value1"));
                    put("int1", 10);
                }}
        );
        assertEquals(doc, doc3);
    }
}
