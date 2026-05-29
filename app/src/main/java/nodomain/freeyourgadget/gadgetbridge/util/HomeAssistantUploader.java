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
import java.util.List;
import java.util.Locale;
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

            for (final HuaweiActivitySample sample : copy) {
                final long timestamp = sample.getTimestamp();
                uploadIntMetric(config, device, "heart_rate", unsignedOrNotMeasured(sample.getHeartRate()), "bpm", timestamp);
                uploadIntMetric(config, device, "resting_heart_rate", unsignedOrNotMeasured(sample.getRestingHeartRate()), "bpm", timestamp);
                uploadIntMetric(config, device, "oxygen_saturation", unsignedOrNotMeasured(sample.getSpo()), "%", timestamp);
                uploadIntMetric(config, device, "steps", sample.getSteps(), "steps", timestamp);
                uploadIntMetric(config, device, "distance", sample.getDistance(), "m", timestamp);
                uploadIntMetric(config, device, "calories", sample.getCalories(), "kcal", timestamp);
            }
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

            for (final HuaweiHrvValueSample sample : copy) {
                uploadIntMetric(config, device, "hrv_rmssd", sample.getValue(), "ms", sample.getTimestamp());
            }
        });
    }

    private static boolean isEnabled() {
        return GBApplication.getPrefs().getBoolean(GBPrefs.HA_UPLOAD_ENABLED, false);
    }

    private static void uploadIntMetric(
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

        final String entityId = "sensor." + sanitizeEntityPart(config.entityPrefix + "_" + metric);
        final JSONObject attributes = new JSONObject();
        try {
            attributes.put("friendly_name", friendlyName(config.entityPrefix, metric));
            attributes.put("unit_of_measurement", unit);
            attributes.put("state_class", "measurement");
            attributes.put("source", "Gadgetbridge");
            attributes.put("source_device", device.getAliasOrName());
            attributes.put("source_address", device.getAddress());
            attributes.put("sample_timestamp", Instant.ofEpochSecond(timestamp).toString());
            attributes.put("sample_timestamp_unix", timestamp);
            attributes.put("metric", metric);
        } catch (final JSONException e) {
            LOG.warn("Failed to build Home Assistant attributes", e);
            return;
        }

        final JSONObject payload = new JSONObject();
        try {
            payload.put("state", Integer.toString(value));
            payload.put("attributes", attributes);
        } catch (final JSONException e) {
            LOG.warn("Failed to build Home Assistant payload", e);
            return;
        }

        if (postState(config, entityId, payload)) {
            setLastUploadedTimestamp(config, device, metric, timestamp);
        }
    }

    private static int unsignedOrNotMeasured(final int value) {
        if (value == ActivitySample.NOT_MEASURED) {
            return ActivitySample.NOT_MEASURED;
        }
        return value & 0xff;
    }

    private static boolean postState(final Config config, final String entityId, final JSONObject payload) {
        HttpURLConnection connection = null;
        try {
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
                LOG.warn("Home Assistant upload failed for {}: HTTP {}", entityId, responseCode);
                return false;
            }
            return true;
        } catch (final IOException e) {
            LOG.warn("Home Assistant upload failed for {}", entityId, e);
            return false;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
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

    private static String friendlyName(final String prefix, final String metric) {
        return prefix.replace('_', ' ') + " " + metric.replace('_', ' ');
    }

    private static String sanitizeEntityPart(final String input) {
        String sanitized = input.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]+", "_");
        sanitized = sanitized.replaceAll("_+", "_").replaceAll("^_+|_+$", "");
        return sanitized.isEmpty() ? DEFAULT_ENTITY_PREFIX : sanitized;
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
