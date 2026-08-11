# Gadgetbridge HA

A fork of [Gadgetbridge](https://codeberg.org/Freeyourgadget/Gadgetbridge) that uploads
watch sensor data to **Home Assistant** and to **PostgreSQL**.

Upstream Gadgetbridge deliberately ships without the `INTERNET` permission — keeping your
health data on the device is a core part of its design. This fork breaks that rule on
purpose, so it is a permanent divergence rather than something that could be merged
upstream. If you do not want your watch data leaving the phone, use upstream Gadgetbridge.

Developed against a **Huawei Watch GT 6 Pro**; the upload hooks sit in the Huawei sync
path, so other vendors are not wired up.

## What it adds

### Home Assistant

After each sync, the newest value per metric is pushed to Home Assistant's built-in
`POST /api/states/<entity_id>` REST API. No custom component or add-on is required.
Entities are named `sensor.<prefix>_<metric>`, and each carries the true measurement
time in the `sample_timestamp` / `sample_timestamp_unix` attributes.

Metrics: `heart_rate`, `resting_heart_rate`, `oxygen_saturation`, `steps`, `distance`,
`calories`, `hrv_rmssd`, `skin_temperature`, `stress`, `stress_level`, `sleep_score`,
`sleep_efficiency`, `sleep_latency_min`, `sleep_avg_hrv`, `sleep_avg_breath_rate`,
`sleep_avg_spo2`, `sleep_avg_heart_rate`, `sleep_snore_freq`, `sleep_duration`,
`sleep_apnea_level`.

Because HA timestamps a state change when it arrives, its recorder history shows upload
time rather than measurement time, and intermediate samples in a batch are not kept.
Use the PostgreSQL sink if you want the real series.

**Setup** — Settings → *Home Assistant upload*:

| Field | Value |
|---|---|
| URL | e.g. `http://homeassistant.local:8123` (an IP is more robust than a hostname if the phone's DNS varies) |
| Token | HA → your user (bottom-left) → *Security* → *Long-lived access tokens* → *Create token* |
| Entity prefix | default `gadgetbridge_huawei` |

### PostgreSQL

The full sample history — including the high-volume series that do not fit HA's sensor
model — is batch-upserted into one long-format table:

```sql
gadgetbridge_samples (device_address text, metric text, sample_ts timestamptz,
                      value double precision, unit text,
                      primary key (device_address, metric, sample_ts))
```

The table and its `(metric, sample_ts)` index are created automatically on first run.
Beyond the HA metrics above this also stores `raw_intensity`, the per-minute
`sleep_stage` timeline, `sleep_deep_part`, and workout data (`workout_heart_rate`,
`workout_speed`, `workout_step_rate`, `workout_cadence`, plus per-workout summaries).

A persisted per-source cursor walks the history in 30-day windows, capped per run, so
the first syncs backfill everything without blocking the phone and later syncs stay
incremental. Inserts use `ON CONFLICT DO NOTHING`, so re-runs are idempotent.

**Setup** — Settings → *PostgreSQL upload*: JDBC URL
(`jdbc:postgresql://host:5432/dbname`), user, password, table name. The server must
accept connections from the phone.

The long format keeps Grafana panels to one query each:

```sql
SELECT sample_ts AS time, value FROM gadgetbridge_samples
WHERE metric = 'hrv_rmssd' ORDER BY 1;
```

## Building

Standard Gadgetbridge build (`./gradlew assembleMainlineDebug`), with two fork-specific
constraints:

- **`minSdk` is 26** (upstream: 23), because the PostgreSQL JDBC driver needs it.
- **pgjdbc is pinned to 42.2.8.** Releases from 42.2.9 onward call
  `java.lang.management.ManagementFactory` on every connect, which Android does not
  ship, producing `NoClassDefFoundError`. Do not bump this without re-testing on device.

Debug builds use the application ID suffix `.ha` and are labelled *Gadgetbridge HA* with
a purple icon, so the fork installs alongside a stock Gadgetbridge instead of replacing
it. Note that only one app can hold the watch's Bluetooth connection at a time — if both
are installed and running they will fight over it, so disable the one you are not using.

Uploads run after each sync. A sync can be triggered externally with:

```
am broadcast --user 0 -p nodomain.freeyourgadget.gadgetbridge.ha \
  -a nodomain.freeyourgadget.gadgetbridge.command.ACTIVITY_SYNC
```

(requires *Intent API* → *Allow activity sync via intents* in settings)

## Credit and licence

All of the hard work — the Bluetooth protocol reverse engineering that makes any of this
possible — belongs to the [Gadgetbridge](https://codeberg.org/Freeyourgadget/Gadgetbridge)
project and its contributors. This fork only adds two upload sinks on top.

Licensed under the **GNU Affero General Public License v3.0**, the same licence as
upstream. See [LICENSE](LICENSE); artwork terms are in [LICENSE.artwork](LICENSE.artwork).
