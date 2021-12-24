package org.maventy.reldatasync;

import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;

public class TestDocument {
    private Document testDoc() {
        return new Document(
                new HashMap<String, Object>() {{
                    // Use "new String" so the string is not "interned"
                    // (i.e. kept in a master list by the JVM), fixing ==
                    put(Document.ID, new String("1"));
                    put("string1", new String("value1"));
                    put("int1", 10);
                }}
        );
    }
    @Test
    public void testGet() {
        Document doc = testDoc();

        // Test get
        assertEquals("1", doc.get(Document.ID));
        assertEquals("value1", doc.get("string1"));
        assertEquals(10, doc.get("int1"));
    }

    @Test
    public void testToJsonString() {
        Document doc = testDoc();
        // Test toJsonString
        assertEquals(
                "{\"int1\":10,\"string1\":\"value1\",\"_id\":\"1\"}",
                doc.toJsonString());
    }

    @Test
    public void testEquals() {
        Document doc = testDoc();

        // Test assertEquals
        assertEquals(doc, doc);
        assertFalse(doc.equals(null));

        // Test assertEquals with shallow clone
        Document doc2 = doc.clone();
        // Not the same object
        assertNotSame(doc, doc2);
        // But equals is the same
        assertEquals(doc, doc2);

        // Test assertEquals with deep copy
        Document doc3 = testDoc();
        assertEquals(doc, doc3);
    }

    @Test
    public void testCompareTo() {
        Document doc = testDoc();
        Document doc2 = testDoc();

        assertEquals(0, doc.compareTo(doc2));

        // Compare to null
        assertEquals(1, doc.compareTo(null));

        // doc2 "int1" has larger value
        doc2.put("int1", 20);
        assertEquals(-1, doc.compareTo(doc2));

        // doc2 has fewer keys
        doc2 = testDoc();
        doc2.remove("int1");
        assertEquals(1, doc.compareTo(doc2));

        // doc "string1" has a smaller value
        doc = testDoc();
        doc2 = testDoc();
        doc.put("string1", "0");
        // It's actually -70
        assertTrue(doc.compareTo(doc2) < 0);
    }
}
