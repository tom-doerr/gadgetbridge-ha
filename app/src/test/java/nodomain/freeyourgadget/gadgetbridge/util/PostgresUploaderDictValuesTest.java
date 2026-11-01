package nodomain.freeyourgadget.gadgetbridge.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PostgresUploaderDictValuesTest {
    private static final String PREFIX = PostgresUploader.PWV_PREFIX;
    private static final long START = 1_793_000_000_000L;

    private static List<PostgresUploader.Row> rows(final byte[] value) {
        final List<PostgresUploader.Row> rows = new ArrayList<>();
        PostgresUploader.addDictValueRows(rows, "AA:BB", PREFIX, 400017430, (byte) 0xa, value, START);
        return rows;
    }

    private static List<PostgresUploader.Row> named(final List<PostgresUploader.Row> rows, final String metric) {
        return rows.stream().filter(r -> r.metric.equals(metric)).collect(Collectors.toList());
    }

    @Test
    public void theMetricNameKeepsTheFineTunesExclusion() {
        assertTrue("the sidecar export excludes metrics starting with ecg", PREFIX.startsWith("ecg"));
    }

    @Test
    public void anEightByteValueIsItsDoubleAndItsBytes() {
        final byte[] value = ByteBuffer.allocate(8).putDouble(7.5).array();
        final List<PostgresUploader.Row> rows = rows(value);
        final String field = PREFIX + "400017430_10";
        assertEquals(7.5, named(rows, field).get(0).value, 0.0);
        assertEquals(START, named(rows, field).get(0).timestampMillis);
        assertEquals(8.0, named(rows, field + "_length").get(0).value, 0.0);
        final List<PostgresUploader.Row> raw = named(rows, field + "_raw");
        assertEquals(8, raw.size());
        for (int i = 0; i < 8; i++) {
            assertEquals(value[i] & 0xff, raw.get(i).value, 0.0);
            assertEquals(START + i, raw.get(i).timestampMillis);
        }
    }

    @Test
    public void anyOtherValueIsItsBytesOnlyZerosAndHighBytesIncluded() {
        final List<PostgresUploader.Row> rows = rows(new byte[]{0, (byte) 0xff, 3});
        final String field = PREFIX + "400017430_10";
        assertTrue(named(rows, field).isEmpty());
        final List<PostgresUploader.Row> raw = named(rows, field + "_raw");
        assertEquals(3, raw.size());
        assertEquals(0.0, raw.get(0).value, 0.0);
        assertEquals(255.0, raw.get(1).value, 0.0);
        assertEquals(3.0, named(rows, field + "_length").get(0).value, 0.0);
    }

    @Test
    public void anEightByteValueThatIsNoNumberKeepsItsBytes() {
        final List<PostgresUploader.Row> rows = rows(ByteBuffer.allocate(8).putDouble(Double.NaN).array());
        final String field = PREFIX + "400017430_10";
        assertTrue(named(rows, field).isEmpty());
        assertEquals(8, named(rows, field + "_raw").size());
    }

    @Test
    public void aHugeValueIsCutButItsLengthSaysSo() {
        final List<PostgresUploader.Row> rows = rows(new byte[PostgresUploader.MAX_RAW_BYTES + 5]);
        final String field = PREFIX + "400017430_10";
        assertEquals(PostgresUploader.MAX_RAW_BYTES, named(rows, field + "_raw").size());
        assertEquals(PostgresUploader.MAX_RAW_BYTES + 5.0, named(rows, field + "_length").get(0).value, 0.0);
    }

    @Test
    public void aMissingValueAddsNothing() {
        assertTrue(rows(null).isEmpty());
        assertFalse(rows(new byte[0]).isEmpty());
    }
}
