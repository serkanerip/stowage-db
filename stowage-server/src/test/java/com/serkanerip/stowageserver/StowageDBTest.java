package com.serkanerip.stowageserver;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class StowageDBTest {
    private StowageDB db;
    private ServerOptions options;
    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        // Using smaller segment size to make testing easier
        options = new ServerOptions(
            "no_host", 1, tempDir, 0.7, 512
        ); // 512B segment size, 70% compaction threshold
        db = new StowageDB(options);
    }

    @AfterEach
    void tearDown() {
        if (db != null) {
            db.shutdown();
        }
    }

    @Nested
    class BasicOperations {

        private static final byte[] KEY_1 = "key1".getBytes(StandardCharsets.UTF_8);

        @Test
        void shouldPutAndGetValue() {
            var value = "test-value".getBytes();
            db.put(KEY_1, value);
            assertArrayEquals(value, db.get(KEY_1));
        }

        @Test
        void shouldReturnNullForNonexistentKey() {
            assertNull(db.get("nonexistent-key"));
        }

        @Test
        void shouldOverwriteExistingValue() {
            byte[] value1 = "value1".getBytes();
            byte[] value2 = "value2".getBytes();

            db.put(KEY_1, value1);
            db.put(KEY_1, value2);

            assertArrayEquals(value2, db.get(KEY_1));
        }

        @Test
        void shouldDeleteValue() {
            byte[] value = "test-value".getBytes();
            db.put(KEY_1, value);
            db.delete(KEY_1);
            System.out.println(db.get(KEY_1));
            assertNull(db.get(KEY_1));
        }
    }

    @Nested
    class SegmentManagement {
        @Test
        void shouldCreateNewSegmentWhenSizeLimitReached() {
            String valuePrefix = "test-value-with-sufficient-length-to-fill-segments-";
            Map<String, SegmentStats> initialStats = db.getSegmentStats();
            long initialTotalDataSize = initialStats.values().stream()
                .mapToLong(SegmentStats::getTotalDataSize)
                .sum();

            // Write enough data to fill multiple segments
            for (int i = 0; i < 50; i++) {
                String value = valuePrefix + "i".repeat(100); // Create large values
                db.put("key" + i, value.getBytes());
            }

            Map<String, SegmentStats> currentStats = db.getSegmentStats();
            long currentTotalDataSize = currentStats.values().stream()
                .mapToLong(SegmentStats::getTotalDataSize)
                .sum();

            assertTrue(currentTotalDataSize > initialTotalDataSize,
                "Total data size should increase");
            assertTrue(currentStats.size() > initialStats.size(),
                "Number of segments should increase");
        }
    }

    @Nested
    class CompactionTests {
        @Test
        void shouldCompactSegmentsWhenThresholdReached() {
            String valuePrefix = "test-value-with-sufficient-length-to-fill-segments-";

            // Write enough data to create multiple segments
            for (int i = 0; i < 100; i++) {
                String value = valuePrefix + "i".repeat(50);
                db.put("key" + i, value.getBytes());
            }

            // Overwrite most of the data to create stale entries
            for (int i = 0; i < 70; i++) {
                String newValue = valuePrefix + "new" + "i".repeat(50);
                db.put("key" + i, newValue.getBytes());
            }

            // Write more data to create a new active segment
            for (int i = 100; i < 150; i++) {
                String value = valuePrefix + "i".repeat(50);
                db.put("key" + i, value.getBytes());
            }

            Map<String, SegmentStats> currentStats = db.getSegmentStats();

            // Verify data integrity
            for (int i = 0; i < 150; i++) {
                byte[] value = db.get("key" + i);
                assertNotNull(value);
                String strValue = new String(value);
                if (i < 70) {
                    assertTrue(strValue.contains("new"),
                        "Overwritten values should contain 'new'");
                } else {
                    assertFalse(strValue.contains("new"),
                        "Non-overwritten values should not contain 'new'");
                }
            }

            // Check obsolete data ratios
            double totalObsoleteRatio = currentStats.values().stream()
                .mapToDouble(SegmentStats::obsoleteDataRatio)
                .average()
                .orElse(0.0);

            assertTrue(totalObsoleteRatio < 0.7,
                "Obsolete data ratio should be below threshold after compaction");
        }
    }

    @Nested
    class StartupRecoveryTests {
        @Test
        void shouldMaintainConsistencyAfterRestart() {
            String valuePrefix = "test-value-with-sufficient-length-to-fill-segments-";

            // Write data and trigger multiple segments
            for (int i = 0; i < 100; i++) {
                db.put("key" + i, (valuePrefix + i).getBytes());
            }

            Map<String, SegmentStats> statsBeforeRestart = db.getSegmentStats();
            long keyCountBeforeRestart = statsBeforeRestart.values().stream()
                .mapToLong(stats -> stats.getTotalKeyCount() - stats.getObsoleteKeyCount())
                .sum();

            // Restart DB
            db.shutdown();
            db = new StowageDB(options);

            for (int i = 0; i < 100; i++) {
                assertArrayEquals((valuePrefix + i).getBytes(), db.get("key" + i));
            }

            Map<String, SegmentStats> statsAfterRestart = db.getSegmentStats();
            long keyCountAfterRestart = statsAfterRestart.values().stream()
                .mapToLong(stats -> stats.getTotalKeyCount() - stats.getObsoleteKeyCount())
                .sum();

            assertEquals(keyCountBeforeRestart, keyCountAfterRestart,
                "Valid key count should be maintained after restart");
        }
    }

    @Nested
    class ErrorHandlingTests {
        @Test
        void shouldHandleNullKeyGracefully() {
            var value = "value".getBytes();
            assertThrows(
                IllegalArgumentException.class, () -> db.put((byte[]) null, value)
            );
        }

        @Test
        void shouldHandleNullValueGracefully() {
            assertThrows(IllegalArgumentException.class, () ->
                db.put("key", null));
        }

        @Test
        void shouldHandleEmptyKeyGracefully() {
            var value = "value".getBytes();
            assertThrows(
                IllegalArgumentException.class, () -> db.put("", value)
            );
        }

        @Test
        void shouldHandleDecommissionOfNonexistentSegment() {
            assertDoesNotThrow(() -> db.decommission(Long.MAX_VALUE));
        }
    }
}