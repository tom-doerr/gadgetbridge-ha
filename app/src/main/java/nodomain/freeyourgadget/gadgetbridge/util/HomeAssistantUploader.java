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

import org.json.JSONArray;
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
import nodomain.freeyourgadget.gadgetbridge.entities.HuaweiActivitySample;
import nodomain.freeyourgadget.gadgetbridge.entities.HuaweiHrvValueSample;
import nodomain.freeyourgadget.gadgetbridge.impl.GBDevice;
import nodomain.freeyourgadget.gadgetbridge.model.ActivitySample;

public final class HomeAssistantUploader {
    private static final Logger LOG = LoggerFactory.getLogger(HomeAssistantUploader.class);
    private static final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();

    private static final String DEFAULT_HA_URL = "http://ha:8123";
    private static final String DEFAULT_ENTITY_PREFIX = "gadgetbridge_huawei";
    private static final String INGEST_PATH = "/api/gadgetbridge_ingest/samples";
    private static final int MAX_SAMPLES_PER_REQUEST = 500;

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
        });
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
                addIntMetric(metricSamples, config, device, "hrv_rmssd", sample.getValue(), "ms", sample.getTimestamp());
            }
            uploadMetricSamples(config, device, metricSamples);
        });
    }

    private static boolean isEnabled() {
        return GBApplication.getPrefs().getBoolean(GBPrefs.HA_UPLOAD_ENABLED, false);
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

        for (int start = 0; start < metricSamples.size(); start += MAX_SAMPLES_PER_REQUEST) {
            final int end = Math.min(start + MAX_SAMPLES_PER_REQUEST, metricSamples.size());
            final List<MetricSample> chunk = metricSamples.subList(start, end);
            if (postBatch(config, device, chunk)) {
                markUploaded(config, device, chunk);
            }
        }
    }

    private static JSONObject buildPayload(final Config config, final GBDevice device, final List<MetricSample> metricSamples) throws JSONException {
        final JSONObject payload = new JSONObject();
        final JSONObject deviceJson = new JSONObject();
        deviceJson.put("name", device.getAliasOrName());
        deviceJson.put("address", device.getAddress());

        final JSONArray samples = new JSONArray();
        for (final MetricSample sample : metricSamples) {
            final JSONObject sampleJson = new JSONObject();
            sampleJson.put("metric", sample.metric);
            sampleJson.put("value", sample.value);
            sampleJson.put("unit", sample.unit);
            sampleJson.put("timestamp", Instant.ofEpochSecond(sample.timestamp).toString());
            sampleJson.put("timestamp_unix", sample.timestamp);
            samples.put(sampleJson);
        }

        payload.put("source", "gadgetbridge");
        payload.put("entity_prefix", config.entityPrefix);
        payload.put("device", deviceJson);
        payload.put("samples", samples);
        return payload;
    }

    private static int unsignedOrNotMeasured(final int value) {
        if (value == ActivitySample.NOT_MEASURED) {
            return ActivitySample.NOT_MEASURED;
        }
        return value & 0xff;
    }

    private static boolean postBatch(final Config config, final GBDevice device, final List<MetricSample> metricSamples) {
        HttpURLConnection connection = null;
        try {
            final JSONObject payload = buildPayload(config, device, metricSamples);
            final URL url = new URL(config.baseUrl + INGEST_PATH);
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
                LOG.warn("Home Assistant batch upload failed for {} samples from {}: HTTP {}", metricSamples.size(), device.getAliasOrName(), responseCode);
                return false;
            }
            return true;
        } catch (final JSONException e) {
            LOG.warn("Failed to build Home Assistant batch payload", e);
            return false;
        } catch (final IOException e) {
            LOG.warn("Home Assistant batch upload failed", e);
            return false;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private static void markUploaded(final Config config, final GBDevice device, final List<MetricSample> metricSamples) {
        final Map<String, Long> latestByMetric = new HashMap<>();
        for (final MetricSample sample : metricSamples) {
            final Long current = latestByMetric.get(sample.metric);
            if (current == null || sample.timestamp > current) {
                latestByMetric.put(sample.metric, sample.timestamp);
            }
        }

        for (final Map.Entry<String, Long> entry : latestByMetric.entrySet()) {
            setLastUploadedTimestamp(config, device, entry.getKey(), entry.getValue());
        }
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
        final int value;
        final String unit;
        final long timestamp;

        private MetricSample(final String metric, final int value, final String unit, final long timestamp) {
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
