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

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import nodomain.freeyourgadget.gadgetbridge.GBApplication;
import nodomain.freeyourgadget.gadgetbridge.entities.DaoSession;
import nodomain.freeyourgadget.gadgetbridge.entities.HuaweiActivitySample;
import nodomain.freeyourgadget.gadgetbridge.entities.HuaweiHrvValueSample;
import nodomain.freeyourgadget.gadgetbridge.impl.GBDevice;
import nodomain.freeyourgadget.gadgetbridge.model.ActivitySample;

public final class HomeAssistantUploader {
    private static final Logger LOG = LoggerFactory.getLogger(HomeAssistantUploader.class);
    private static final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();

    private static final String DEFAULT_HA_URL = "http://ha:8123";
    private static final String DEFAULT_ENTITY_PREFIX = "gadgetbridge_huawei";

    private HomeAssistantUploader() {
    }

    public static void uploadHuaweiActivitySamples(final GBDevice device, final List<HuaweiActivitySample> samples) {
        if (samples == null || samples.isEmpty() || !isEnabled()) {
            return;
        }

        final List<HuaweiActivitySample> copy = new ArrayList<>(samples);
        EXECUTOR.execute(() -> {
            final Config config = Config.read();
            if (!config.isUsable()) {
                return;
            }

            final List<MetricSample> metricSamples = new ArrayList<>();
            for (final HuaweiActivitySample sample : copy) {
                final long timestamp = sample.getTimestamp();
                addIntMetric(metricSamples, config, device, "heart_rate", unsignedOrNotMeasured(sample.getHeartRate()), "bpm", timestamp);
                addIntMetric(metricSamples, config, device, "resting_heart_rate", unsignedOrNotMeasured(sample.getRestingHeartRate()), "bpm", timestamp);
                addIntMetric(metricSamples, config, device, "oxygen_saturation", unsignedOrNotMeasured(sample.getSpo()), "%", timestamp);
                addIntMetric(metricSamples, config, device, "steps", sample.getSteps(), "steps", timestamp);
                addIntMetric(metricSamples, config, device, "distance", sample.getDistance(), "m", timestamp);
                addIntMetric(metricSamples, config, device, "calories", sample.getCalories(), "kcal", timestamp);
            }
            uploadMetricSamples(config, device, metricSamples);
            backfillStoredSamples(device);
        });
    }

    /**
     * Sync hooks only fire for newly parsed samples, so data recorded before HA
     * upload was configured (or while it was failing) would never reach Home
     * Assistant. After each activity upload, re-read the last week of stored
     * samples; the per-metric last-uploaded timestamps keep this idempotent.
     */
    private static void backfillStoredSamples(final GBDevice device) {
        final Config config = Config.read();
        if (!config.isUsable()) {
            return;
        }

        try (nodomain.freeyourgadget.gadgetbridge.database.DBHandler db = GBApplication.acquireDB()) {
            final DaoSession session = db.getDaoSession();
            final long nowMs = System.currentTimeMillis();
            final long fromMs = nowMs - 7L * 24 * 60 * 60 * 1000;
            final long toMs = nowMs + 2L * 24 * 60 * 60 * 1000;
            final List<MetricSample> metricSamples = new ArrayList<>();

            for (final HuaweiHrvValueSample sample :
                    new nodomain.freeyourgadget.gadgetbridge.devices.huawei.HuaweiHrvValueSampleProvider(device, session).getAllSamples(fromMs, toMs)) {
                addIntMetric(metricSamples, config, device, "hrv_rmssd", sample.getValue(), "ms", normalizeTimestamp(sample.getTimestamp()));
            }

            for (final nodomain.freeyourgadget.gadgetbridge.entities.HuaweiTemperatureSample sample :
                    new nodomain.freeyourgadget.gadgetbridge.devices.huawei.HuaweiTemperatureSampleProvider(device, session).getAllSamples(fromMs, toMs)) {
                final double celsius = Math.round(sample.getTemperature() * 10.0) / 10.0;
                addDoubleMetric(metricSamples, config, device, "skin_temperature", celsius, "°C", normalizeTimestamp(sample.getTimestamp()));
            }

            for (final nodomain.freeyourgadget.gadgetbridge.entities.HuaweiStressSample sample :
                    new nodomain.freeyourgadget.gadgetbridge.devices.huawei.HuaweiStressSampleProvider(device, session).getAllSamples(fromMs, toMs)) {
                final long timestamp = normalizeTimestamp(sample.getTimestamp());
                addIntMetric(metricSamples, config, device, "stress", sample.getStress(), null, timestamp);
                addDoubleMetric(metricSamples, config, device, "stress_level", sample.getLevel(), null, timestamp);
            }

            for (final nodomain.freeyourgadget.gadgetbridge.entities.HuaweiSleepStatsSample sample :
                    new nodomain.freeyourgadget.gadgetbridge.devices.huawei.HuaweiSleepStatsSampleProvider(device, session).getAllSamples(fromMs, toMs)) {
                final long risingTime = sample.getRisingTime();
                final long timestamp = normalizeTimestamp(risingTime > 0 ? risingTime : sample.getTimestamp());
                addIntMetric(metricSamples, config, device, "sleep_score", sample.getSleepScore(), null, timestamp);
                addIntMetric(metricSamples, config, device, "sleep_efficiency", sample.getSleepEfficiency(), "%", timestamp);
                addIntMetric(metricSamples, config, device, "sleep_latency_min", sample.getSleepLatency(), "min", timestamp);
                addIntMetric(metricSamples, config, device, "sleep_avg_hrv", sample.getAvgHrv(), "ms", timestamp);
                addIntMetric(metricSamples, config, device, "sleep_avg_breath_rate", sample.getAvgBreathRate(), "breaths/min", timestamp);
                addIntMetric(metricSamples, config, device, "sleep_avg_spo2", sample.getAvgOxygenSaturation(), "%", timestamp);
                addIntMetric(metricSamples, config, device, "sleep_avg_heart_rate", sample.getAvgHeartRate(), "bpm", timestamp);
                addDoubleMetric(metricSamples, config, device, "sleep_snore_freq", sample.getSnoreFreq(), null, timestamp);
                final long bedTime = sample.getBedTime();
                if (bedTime > 0 && risingTime > bedTime) {
                    final double hours = Math.round((normalizeTimestamp(risingTime) - normalizeTimestamp(bedTime)) / 36.0) / 100.0;
                    addDoubleMetric(metricSamples, config, device, "sleep_duration", hours, "h", timestamp);
                }
            }

            for (final nodomain.freeyourgadget.gadgetbridge.entities.HuaweiSleepApneaSample sample :
                    new nodomain.freeyourgadget.gadgetbridge.devices.huawei.HuaweiSleepApneaSampleProvider(device, session).getAllSamples(fromMs, toMs)) {
                addDoubleMetric(metricSamples, config, device, "sleep_apnea_level", sample.getLevel(), null, normalizeTimestamp(sample.getTimestamp()));
            }

            uploadMetricSamples(config, device, metricSamples);
        } catch (final Exception e) {
            LOG.warn("Failed to backfill stored samples for Home Assistant", e);
        }
    }

    public static void uploadHuaweiHrvSamples(final GBDevice device, final List<HuaweiHrvValueSample> samples) {
        if (samples == null || samples.isEmpty() || !isEnabled()) {
            return;
        }

        final List<HuaweiHrvValueSample> copy = new ArrayList<>(samples);
        EXECUTOR.execute(() -> {
            final Config config = Config.read();
            if (!config.isUsable()) {
                return;
            }

            final List<MetricSample> metricSamples = new ArrayList<>();
            for (final HuaweiHrvValueSample sample : copy) {
                // HuaweiHrvValueSample timestamps are in milliseconds; normalize to epoch seconds
                long timestampSeconds = sample.getTimestamp();
                if (timestampSeconds > 10_000_000_000L) {
                    timestampSeconds /= 1000L;
                }
                addIntMetric(metricSamples, config, device, "hrv_rmssd", sample.getValue(), "ms", timestampSeconds);
            }
            uploadMetricSamples(config, device, metricSamples);
        });
    }

    private static boolean isEnabled() {
        return GBApplication.getPrefs().getBoolean(GBPrefs.HA_UPLOAD_ENABLED, false);
    }

    /** Serialize integral doubles as plain integers so sensor states stay "38", not "38.0". */
    private static Object jsonNumber(final double value) {
        if (!Double.isInfinite(value) && !Double.isNaN(value) && value == Math.rint(value)) {
            return (long) value;
        }
        return value;
    }

    /** Huawei sample tables store epoch milliseconds; the upload pipeline uses epoch seconds. */
    private static long normalizeTimestamp(final long timestamp) {
        return timestamp > 10_000_000_000L ? timestamp / 1000L : timestamp;
    }

    private static void addDoubleMetric(
            final List<MetricSample> metricSamples,
            final Config config,
            final GBDevice device,
            final String metric,
            final double value,
            final String unit,
            final long timestamp
    ) {
        if (Double.isNaN(value) || value < 0 || timestamp <= 0) {
            return;
        }
        if (timestamp <= getLastUploadedTimestamp(config, device, metric)) {
            return;
        }

        metricSamples.add(new MetricSample(metric, value, unit, timestamp));
    }

    private static void addIntMetric(
            final List<MetricSample> metricSamples,
            final Config config,
            final GBDevice device,
            final String metric,
            final int value,
            final String unit,
            final long timestamp
    ) {
        if (value == ActivitySample.NOT_MEASURED || value <= 0 || timestamp <= 0) {
            return;
        }
        if (timestamp <= getLastUploadedTimestamp(config, device, metric)) {
            return;
        }

        metricSamples.add(new MetricSample(metric, value, unit, timestamp));
    }

    private static void uploadMetricSamples(final Config config, final GBDevice device, final List<MetricSample> metricSamples) {
        if (metricSamples.isEmpty()) {
            return;
        }

        uploadLatestStates(config, device, metricSamples);
    }

    /**
     * Update sensor.<prefix>_<metric> via Home Assistant's built-in /api/states
     * REST API with the newest value per metric. HA records history at receipt
     * time, so exact per-sample timestamps survive only as attributes; the
     * PostgreSQL sink is what stores the full timestamped series.
     */
    private static void uploadLatestStates(final Config config, final GBDevice device, final List<MetricSample> metricSamples) {
        final Map<String, MetricSample> latestByMetric = new HashMap<>();
        for (final MetricSample sample : metricSamples) {
            final MetricSample current = latestByMetric.get(sample.metric);
            if (current == null || sample.timestamp > current.timestamp) {
                latestByMetric.put(sample.metric, sample);
            }
        }

        for (final MetricSample sample : latestByMetric.values()) {
            if (postState(config, device, sample)) {
                setLastUploadedTimestamp(config, device, sample.metric, sample.timestamp);
            }
        }
    }

    private static boolean postState(final Config config, final GBDevice device, final MetricSample sample) {
        HttpURLConnection connection = null;
        try {
            final String entityId = "sensor." + sanitizeEntityPart(config.entityPrefix + "_" + sample.metric);

            final JSONObject attributes = new JSONObject();
            attributes.put("unit_of_measurement", sample.unit);
            attributes.put("friendly_name", (config.entityPrefix + " " + sample.metric).replace('_', ' '));
            attributes.put("state_class", "measurement");
            attributes.put("source", "gadgetbridge");
            attributes.put("source_device", device.getAliasOrName());
            attributes.put("source_address", device.getAddress());
            attributes.put("sample_timestamp", Instant.ofEpochSecond(sample.timestamp).toString());
            attributes.put("sample_timestamp_unix", sample.timestamp);

            final JSONObject payload = new JSONObject();
            payload.put("state", jsonNumber(sample.value));
            payload.put("attributes", attributes);

            final URL url = new URL(config.baseUrl + "/api/states/" + entityId);
            connection = (HttpURLConnection) url.openConnection();
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(10000);
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("Authorization", "Bearer " + config.token);
            connection.setRequestProperty("Content-Type", "application/json; charset=utf-8");

            final byte[] body = payload.toString().getBytes(StandardCharsets.UTF_8);
            connection.setFixedLengthStreamingMode(body.length);
            try (OutputStream outputStream = connection.getOutputStream()) {
                outputStream.write(body);
            }

            final int responseCode = connection.getResponseCode();
            if (responseCode < 200 || responseCode >= 300) {
                LOG.warn("Home Assistant state update failed for {} from {}: HTTP {}", entityId, device.getAliasOrName(), responseCode);
                return false;
            }
            return true;
        } catch (final JSONException e) {
            LOG.warn("Failed to build Home Assistant state payload", e);
            return false;
        } catch (final IOException e) {
            LOG.warn("Home Assistant state update failed", e);
            return false;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private static int unsignedOrNotMeasured(final int value) {
        if (value == ActivitySample.NOT_MEASURED) {
            return ActivitySample.NOT_MEASURED;
        }
        return value & 0xff;
    }

    private static long getLastUploadedTimestamp(final Config config, final GBDevice device, final String metric) {
        return GBApplication.getPrefs().getLong(lastTimestampKey(config, device, metric), 0L);
    }

    private static void setLastUploadedTimestamp(final Config config, final GBDevice device, final String metric, final long timestamp) {
        final SharedPreferences.Editor editor = GBApplication.getPrefs().getPreferences().edit();
        editor.putLong(lastTimestampKey(config, device, metric), timestamp);
        editor.apply();
    }

    private static String lastTimestampKey(final Config config, final GBDevice device, final String metric) {
        return "ha_upload_last_" + sanitizeEntityPart(config.entityPrefix + "_" + device.getAddress() + "_" + metric);
    }

    private static String sanitizeEntityPart(final String input) {
        String sanitized = input.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]+", "_");
        sanitized = sanitized.replaceAll("_+", "_").replaceAll("^_+|_+$", "");
        return sanitized.isEmpty() ? DEFAULT_ENTITY_PREFIX : sanitized;
    }

    private static final class MetricSample {
        final String metric;
        final double value;
        final String unit;
        final long timestamp;

        private MetricSample(final String metric, final double value, final String unit, final long timestamp) {
            this.metric = metric;
            this.value = value;
            this.unit = unit;
            this.timestamp = timestamp;
        }
    }

    private static final class Config {
        final String baseUrl;
        final String token;
        final String entityPrefix;

        private Config(final String baseUrl, final String token, final String entityPrefix) {
            this.baseUrl = trimTrailingSlash(baseUrl);
            this.token = token == null ? "" : token.trim();
            this.entityPrefix = sanitizeEntityPart(entityPrefix == null || entityPrefix.isEmpty() ? DEFAULT_ENTITY_PREFIX : entityPrefix);
        }

        static Config read() {
            final GBPrefs prefs = GBApplication.getPrefs();
            return new Config(
                    prefs.getString(GBPrefs.HA_UPLOAD_URL, DEFAULT_HA_URL),
                    prefs.getString(GBPrefs.HA_UPLOAD_TOKEN, ""),
                    prefs.getString(GBPrefs.HA_UPLOAD_ENTITY_PREFIX, DEFAULT_ENTITY_PREFIX)
            );
        }

        boolean isUsable() {
            return !baseUrl.isEmpty() && !token.isEmpty();
        }

        private static String trimTrailingSlash(final String value) {
            if (value == null) {
                return DEFAULT_HA_URL;
            }
            String trimmed = value.trim();
            while (trimmed.endsWith("/")) {
                trimmed = trimmed.substring(0, trimmed.length() - 1);
            }
            return trimmed.isEmpty() ? DEFAULT_HA_URL : trimmed;
        }
    }
}
