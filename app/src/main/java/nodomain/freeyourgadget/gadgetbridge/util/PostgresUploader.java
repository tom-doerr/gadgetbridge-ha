/*  Copyright (C) 2026 Tom Doerr

    This file is part of Gadgetbridge.

    Gadgetbridge is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published
    by the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    Gadgetbridge is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>. */
package nodomain.freeyourgadget.gadgetbridge.util;

import android.content.SharedPreferences;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import nodomain.freeyourgadget.gadgetbridge.GBApplication;
import nodomain.freeyourgadget.gadgetbridge.database.DBHandler;
import nodomain.freeyourgadget.gadgetbridge.devices.huawei.HuaweiHrvValueSampleProvider;
import nodomain.freeyourgadget.gadgetbridge.devices.huawei.HuaweiSampleProvider;
import nodomain.freeyourgadget.gadgetbridge.devices.huawei.HuaweiSleepApneaSampleProvider;
import nodomain.freeyourgadget.gadgetbridge.devices.huawei.HuaweiSleepStageSampleProvider;
import nodomain.freeyourgadget.gadgetbridge.devices.huawei.HuaweiSleepStatsSampleProvider;
import nodomain.freeyourgadget.gadgetbridge.devices.huawei.HuaweiStressSampleProvider;
import nodomain.freeyourgadget.gadgetbridge.devices.huawei.HuaweiTemperatureSampleProvider;
import nodomain.freeyourgadget.gadgetbridge.entities.DaoSession;
import nodomain.freeyourgadget.gadgetbridge.entities.HuaweiActivitySample;
import nodomain.freeyourgadget.gadgetbridge.entities.HuaweiHrvValueSample;
import nodomain.freeyourgadget.gadgetbridge.entities.HuaweiSleepApneaSample;
import nodomain.freeyourgadget.gadgetbridge.entities.HuaweiSleepStageSample;
import nodomain.freeyourgadget.gadgetbridge.entities.HuaweiSleepStatsSample;
import nodomain.freeyourgadget.gadgetbridge.entities.HuaweiStressSample;
import nodomain.freeyourgadget.gadgetbridge.entities.HuaweiTemperatureSample;
import nodomain.freeyourgadget.gadgetbridge.entities.HuaweiWorkoutDataSample;
import nodomain.freeyourgadget.gadgetbridge.entities.HuaweiWorkoutDataSampleDao;
import nodomain.freeyourgadget.gadgetbridge.entities.HuaweiWorkoutSummarySample;
import nodomain.freeyourgadget.gadgetbridge.entities.HuaweiWorkoutSummarySampleDao;
import nodomain.freeyourgadget.gadgetbridge.impl.GBDevice;
import nodomain.freeyourgadget.gadgetbridge.model.ActivitySample;

/**
 * Uploads the complete sample history to a PostgreSQL database in a long/narrow
 * schema: (device_address, metric, sample_ts, value, unit). Unlike the Home
 * Assistant uploader this sink receives everything, including high-volume series
 * that do not fit HA's sensor model (per-minute temperature, sleep stages,
 * workout point data). A persisted per-source cursor makes the backfill
 * incremental and resumable; ON CONFLICT DO NOTHING makes re-runs idempotent.
 */
public final class PostgresUploader {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresUploader.class);
    private static final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();
    private static final AtomicBoolean RUNNING = new AtomicBoolean(false);

    private static final long WINDOW_MS = 30L * 24 * 60 * 60 * 1000;
    private static final int MAX_WINDOWS_PER_RUN = 24;
    private static final int BATCH_SIZE = 1000;
    private static final long HISTORY_START_MS = 1546300800000L; // 2019-01-01, before any supported device

    private PostgresUploader() {
    }

    public static void requestSync(final GBDevice device) {
        if (!GBApplication.getPrefs().getBoolean(GBPrefs.PG_UPLOAD_ENABLED, false)) {
            return;
        }
        if (!RUNNING.compareAndSet(false, true)) {
            return; // a sync is already queued or running
        }
        EXECUTOR.execute(() -> {
            try {
                syncAll(device);
            } finally {
                RUNNING.set(false);
            }
        });
    }

    private static void syncAll(final GBDevice device) {
        final Config config = Config.read();
        if (!config.isUsable()) {
            return;
        }

        try {
            Class.forName("org.postgresql.Driver");
        } catch (final ClassNotFoundException e) {
            LOG.error("PostgreSQL driver not available", e);
            return;
        }

        final Properties props = new Properties();
        props.setProperty("user", config.user);
        props.setProperty("password", config.password);
        props.setProperty("connectTimeout", "10");
        props.setProperty("socketTimeout", "120");

        try (Connection connection = DriverManager.getConnection(config.url, props)) {
            connection.setAutoCommit(false);
            ensureTable(connection, config.table);

            try (DBHandler db = GBApplication.acquireDB()) {
                final DaoSession session = db.getDaoSession();
                final PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO " + config.table + " (device_address, metric, sample_ts, value, unit)"
                                + " VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING");

                syncWindowedSource(connection, insert, device, "activity", (rows, fromMs, toMs) -> {
                    final HuaweiSampleProvider provider = new HuaweiSampleProvider(device, session);
                    for (final HuaweiActivitySample sample : provider.getAllActivitySamples((int) (fromMs / 1000), (int) (toMs / 1000))) {
                        final long ts = sample.getTimestamp();
                        addRow(rows, device, "heart_rate", unsigned(sample.getHeartRate()), "bpm", ts);
                        addRow(rows, device, "resting_heart_rate", unsigned(sample.getRestingHeartRate()), "bpm", ts);
                        addRow(rows, device, "oxygen_saturation", unsigned(sample.getSpo()), "%", ts);
                        addRow(rows, device, "steps", sample.getSteps(), "steps", ts);
                        addRow(rows, device, "distance", sample.getDistance(), "m", ts);
                        addRow(rows, device, "calories", sample.getCalories(), "kcal", ts);
                        addRow(rows, device, "raw_intensity", sample.getRawIntensity(), null, ts);
                    }
                });

                syncWindowedSource(connection, insert, device, "hrv", (rows, fromMs, toMs) -> {
                    for (final HuaweiHrvValueSample sample : new HuaweiHrvValueSampleProvider(device, session).getAllSamples(fromMs, toMs)) {
                        addRow(rows, device, "hrv_rmssd", sample.getValue(), "ms", seconds(sample.getTimestamp()));
                    }
                });

                syncWindowedSource(connection, insert, device, "temperature", (rows, fromMs, toMs) -> {
                    for (final HuaweiTemperatureSample sample : new HuaweiTemperatureSampleProvider(device, session).getAllSamples(fromMs, toMs)) {
                        final double celsius = Math.round(sample.getTemperature() * 10.0) / 10.0;
                        addRow(rows, device, "skin_temperature", celsius, "°C", seconds(sample.getTimestamp()));
                    }
                });

                syncWindowedSource(connection, insert, device, "stress", (rows, fromMs, toMs) -> {
                    for (final HuaweiStressSample sample : new HuaweiStressSampleProvider(device, session).getAllSamples(fromMs, toMs)) {
                        final long ts = seconds(sample.getTimestamp());
                        addRow(rows, device, "stress", sample.getStress(), null, ts);
                        addRowAllowZero(rows, device, "stress_level", sample.getLevel(), null, ts);
                    }
                });

                syncWindowedSource(connection, insert, device, "sleep_stage", (rows, fromMs, toMs) -> {
                    for (final HuaweiSleepStageSample sample : new HuaweiSleepStageSampleProvider(device, session).getAllSamples(fromMs, toMs)) {
                        addRowAllowZero(rows, device, "sleep_stage", sample.getStage(), null, seconds(sample.getTimestamp()));
                    }
                });

                syncWindowedSource(connection, insert, device, "sleep_stats", (rows, fromMs, toMs) -> {
                    for (final HuaweiSleepStatsSample sample : new HuaweiSleepStatsSampleProvider(device, session).getAllSamples(fromMs, toMs)) {
                        final long risingTime = sample.getRisingTime();
                        final long ts = seconds(risingTime > 0 ? risingTime : sample.getTimestamp());
                        addRow(rows, device, "sleep_score", sample.getSleepScore(), null, ts);
                        addRow(rows, device, "sleep_efficiency", sample.getSleepEfficiency(), "%", ts);
                        addRow(rows, device, "sleep_latency_min", sample.getSleepLatency(), "min", ts);
                        addRow(rows, device, "sleep_deep_part", sample.getDeepPart(), null, ts);
                        addRowAllowZero(rows, device, "sleep_snore_freq", sample.getSnoreFreq(), null, ts);
                        addRow(rows, device, "sleep_avg_hrv", sample.getAvgHrv(), "ms", ts);
                        addRow(rows, device, "sleep_avg_breath_rate", sample.getAvgBreathRate(), "breaths/min", ts);
                        addRow(rows, device, "sleep_avg_spo2", sample.getAvgOxygenSaturation(), "%", ts);
                        addRow(rows, device, "sleep_avg_heart_rate", sample.getAvgHeartRate(), "bpm", ts);
                        final long bedTime = sample.getBedTime();
                        if (bedTime > 0 && risingTime > bedTime) {
                            final double hours = Math.round((seconds(risingTime) - seconds(bedTime)) / 36.0) / 100.0;
                            addRow(rows, device, "sleep_duration", hours, "h", ts);
                        }
                    }
                });

                syncWindowedSource(connection, insert, device, "sleep_apnea", (rows, fromMs, toMs) -> {
                    for (final HuaweiSleepApneaSample sample : new HuaweiSleepApneaSampleProvider(device, session).getAllSamples(fromMs, toMs)) {
                        addRowAllowZero(rows, device, "sleep_apnea_level", sample.getLevel(), null, seconds(sample.getTimestamp()));
                    }
                });

                syncWindowedSource(connection, insert, device, "workout_summary", (rows, fromMs, toMs) -> {
                    final List<HuaweiWorkoutSummarySample> summaries = session.getHuaweiWorkoutSummarySampleDao().queryBuilder()
                            .where(HuaweiWorkoutSummarySampleDao.Properties.StartTimestamp.ge(fromMs / 1000),
                                    HuaweiWorkoutSummarySampleDao.Properties.StartTimestamp.lt(toMs / 1000))
                            .list();
                    for (final HuaweiWorkoutSummarySample sample : summaries) {
                        final long ts = sample.getStartTimestamp();
                        addRow(rows, device, "workout_calories", sample.getCalories(), "kcal", ts);
                        addRow(rows, device, "workout_distance", sample.getDistance(), "m", ts);
                        addRow(rows, device, "workout_steps", sample.getStepCount(), "steps", ts);
                        addRow(rows, device, "workout_duration", sample.getDuration(), "s", ts);
                        addRowAllowZero(rows, device, "workout_type", sample.getType(), null, ts);
                    }
                });

                syncWindowedSource(connection, insert, device, "workout_data", (rows, fromMs, toMs) -> {
                    final List<HuaweiWorkoutDataSample> points = session.getHuaweiWorkoutDataSampleDao().queryBuilder()
                            .where(HuaweiWorkoutDataSampleDao.Properties.Timestamp.ge(fromMs / 1000),
                                    HuaweiWorkoutDataSampleDao.Properties.Timestamp.lt(toMs / 1000))
                            .list();
                    for (final HuaweiWorkoutDataSample sample : points) {
                        final long ts = sample.getTimestamp();
                        addRow(rows, device, "workout_heart_rate", sample.getHeartRate() & 0xff, "bpm", ts);
                        addRow(rows, device, "workout_speed", sample.getSpeed(), "cm/s", ts);
                        addRow(rows, device, "workout_step_rate", sample.getStepRate() & 0xff, "spm", ts);
                        addRow(rows, device, "workout_cadence", sample.getCadence(), "rpm", ts);
                    }
                });

                insert.close();
            }
        } catch (final Exception e) {
            LOG.warn("PostgreSQL upload failed", e);
        }
    }

    private interface WindowCollector {
        void collect(List<Row> rows, long fromMs, long toMs) throws Exception;
    }

    /**
     * Walk [cursor, now] in windows, inserting each window's rows in one
     * transaction and advancing the persisted cursor only after commit. Work per
     * run is capped so a single sync never blocks the executor for hours; the
     * next sync resumes where this one stopped.
     */
    private static void syncWindowedSource(
            final Connection connection,
            final PreparedStatement insert,
            final GBDevice device,
            final String source,
            final WindowCollector collector
    ) {
        final String cursorKey = "pg_cursor_" + sanitize(source + "_" + device.getAddress());
        long cursorMs = GBApplication.getPrefs().getLong(cursorKey, HISTORY_START_MS);
        final long horizonMs = System.currentTimeMillis() + 60_000;

        int windows = 0;
        long inserted = 0;
        try {
            while (cursorMs < horizonMs && windows < MAX_WINDOWS_PER_RUN) {
                final long windowEndMs = Math.min(cursorMs + WINDOW_MS, horizonMs);
                final List<Row> rows = new ArrayList<>();
                collector.collect(rows, cursorMs, windowEndMs);
                inserted += insertRows(insert, rows);
                connection.commit();

                if (windowEndMs >= horizonMs) {
                    // Caught up: park the cursor two days back so windows that may
                    // still receive late samples (watch syncs deliver the recent
                    // past) get re-scanned next run; ON CONFLICT keeps it idempotent.
                    final long settledMs = System.currentTimeMillis() - 2L * 24 * 60 * 60 * 1000;
                    persistCursor(cursorKey, Math.max(cursorMs, settledMs));
                    break;
                }
                cursorMs = windowEndMs;
                persistCursor(cursorKey, cursorMs);
                windows++;
            }
            if (inserted > 0) {
                LOG.info("PostgreSQL: inserted {} rows for source {}", inserted, source);
            }
        } catch (final Exception e) {
            LOG.warn("PostgreSQL sync failed for source {}", source, e);
            try {
                connection.rollback();
            } catch (final Exception ignored) {
            }
        }
    }

    private static long insertRows(final PreparedStatement insert, final List<Row> rows) throws Exception {
        long total = 0;
        int pending = 0;
        for (final Row row : rows) {
            insert.setString(1, row.deviceAddress);
            insert.setString(2, row.metric);
            insert.setTimestamp(3, new Timestamp(row.timestampSeconds * 1000L));
            insert.setDouble(4, row.value);
            insert.setString(5, row.unit);
            insert.addBatch();
            if (++pending >= BATCH_SIZE) {
                total += executed(insert.executeBatch());
                pending = 0;
            }
        }
        if (pending > 0) {
            total += executed(insert.executeBatch());
        }
        return total;
    }

    private static long executed(final int[] results) {
        long count = 0;
        for (final int result : results) {
            if (result > 0 || result == Statement.SUCCESS_NO_INFO) {
                count++;
            }
        }
        return count;
    }

    private static void ensureTable(final Connection connection, final String table) throws Exception {
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS " + table + " ("
                    + "device_address TEXT NOT NULL,"
                    + "metric TEXT NOT NULL,"
                    + "sample_ts TIMESTAMPTZ NOT NULL,"
                    + "value DOUBLE PRECISION NOT NULL,"
                    + "unit TEXT,"
                    + "PRIMARY KEY (device_address, metric, sample_ts))");
            statement.execute("CREATE INDEX IF NOT EXISTS " + table + "_metric_ts_idx ON " + table + " (metric, sample_ts)");
        }
        connection.commit();
    }

    private static void addRow(final List<Row> rows, final GBDevice device, final String metric, final double value, final String unit, final long timestampSeconds) {
        if (Double.isNaN(value) || value <= 0 || value == ActivitySample.NOT_MEASURED || timestampSeconds <= 0) {
            return;
        }
        rows.add(new Row(device.getAddress(), metric, value, unit, timestampSeconds));
    }

    private static void addRowAllowZero(final List<Row> rows, final GBDevice device, final String metric, final double value, final String unit, final long timestampSeconds) {
        if (Double.isNaN(value) || value < 0 || timestampSeconds <= 0) {
            return;
        }
        rows.add(new Row(device.getAddress(), metric, value, unit, timestampSeconds));
    }

    private static int unsigned(final int value) {
        return value == ActivitySample.NOT_MEASURED ? ActivitySample.NOT_MEASURED : value & 0xff;
    }

    private static long seconds(final long timestamp) {
        return timestamp > 10_000_000_000L ? timestamp / 1000L : timestamp;
    }

    private static void persistCursor(final String key, final long value) {
        final SharedPreferences.Editor editor = GBApplication.getPrefs().getPreferences().edit();
        editor.putLong(key, value);
        editor.apply();
    }

    private static String sanitize(final String input) {
        return input.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]+", "_");
    }

    private static final class Row {
        final String deviceAddress;
        final String metric;
        final double value;
        final String unit;
        final long timestampSeconds;

        private Row(final String deviceAddress, final String metric, final double value, final String unit, final long timestampSeconds) {
            this.deviceAddress = deviceAddress;
            this.metric = metric;
            this.value = value;
            this.unit = unit;
            this.timestampSeconds = timestampSeconds;
        }
    }

    private static final class Config {
        final String url;
        final String user;
        final String password;
        final String table;

        private Config(final String url, final String user, final String password, final String table) {
            this.url = url == null ? "" : url.trim();
            this.user = user == null ? "" : user.trim();
            this.password = password == null ? "" : password;
            this.table = sanitizeTable(table);
        }

        static Config read() {
            final GBPrefs prefs = GBApplication.getPrefs();
            return new Config(
                    prefs.getString(GBPrefs.PG_UPLOAD_URL, "jdbc:postgresql://ha:5432/gadgetbridge"),
                    prefs.getString(GBPrefs.PG_UPLOAD_USER, ""),
                    prefs.getString(GBPrefs.PG_UPLOAD_PASSWORD, ""),
                    prefs.getString(GBPrefs.PG_UPLOAD_TABLE, "gadgetbridge_samples")
            );
        }

        boolean isUsable() {
            return url.startsWith("jdbc:postgresql://") && !user.isEmpty();
        }

        private static String sanitizeTable(final String table) {
            final String sanitized = table == null ? "" : table.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]", "");
            return sanitized.isEmpty() ? "gadgetbridge_samples" : sanitized;
        }
    }
}
