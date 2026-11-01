package nodomain.freeyourgadget.gadgetbridge.service.devices.huawei.p2p.dictionarysync;

import android.content.Context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import nodomain.freeyourgadget.gadgetbridge.GBApplication;
import nodomain.freeyourgadget.gadgetbridge.database.DBHandler;
import nodomain.freeyourgadget.gadgetbridge.database.DBHelper;
import nodomain.freeyourgadget.gadgetbridge.devices.huawei.HuaweiState;
import nodomain.freeyourgadget.gadgetbridge.entities.DaoSession;
import nodomain.freeyourgadget.gadgetbridge.entities.Device;
import nodomain.freeyourgadget.gadgetbridge.entities.HuaweiDictData;
import nodomain.freeyourgadget.gadgetbridge.entities.HuaweiDictDataDao;
import nodomain.freeyourgadget.gadgetbridge.entities.HuaweiDictDataValues;
import nodomain.freeyourgadget.gadgetbridge.entities.HuaweiDictDataValuesDao;
import nodomain.freeyourgadget.gadgetbridge.entities.User;
import nodomain.freeyourgadget.gadgetbridge.impl.GBDevice;
import nodomain.freeyourgadget.gadgetbridge.service.devices.huawei.p2p.HuaweiP2PDataDictionarySyncService;

/**
 * Arterial stiffness results. The fields of this class have not been decoded yet, so every value is kept
 * exactly as the watch sent it, in the generic dictionary tables (no schema change), and logged.
 * PostgresUploader sends each value's raw bytes, so the fields can be named from real records.
 */
public class HuaweiDictionarySyncArterialStiffness implements HuaweiDictionarySyncInterface {
    private final Logger LOG = LoggerFactory.getLogger(HuaweiDictionarySyncArterialStiffness.class);

    public static final int ARTERIAL_STIFFNESS_CLASS = 400017;

    @Override
    public int getDataClass() {
        return ARTERIAL_STIFFNESS_CLASS;
    }

    @Override
    public boolean supports(HuaweiState state) {
        return state.supportsArterialStiffnessDetection();
    }

    @Override
    public long getLastDataSyncTimestamp(GBDevice gbDevice) {
        try (DBHandler db = GBApplication.acquireDB()) {
            final DaoSession session = db.getDaoSession();
            final Device device = DBHelper.findDevice(gbDevice, session);
            if (device == null) {
                return 0;
            }
            final List<HuaweiDictData> newest = session.getHuaweiDictDataDao().queryBuilder()
                    .where(HuaweiDictDataDao.Properties.DeviceId.eq(device.getId()),
                            HuaweiDictDataDao.Properties.DictClass.eq(ARTERIAL_STIFFNESS_CLASS))
                    .orderDesc(HuaweiDictDataDao.Properties.StartTimestamp)
                    .limit(1)
                    .list();
            if (newest.isEmpty()) {
                return 0;
            }
            final HuaweiDictData record = newest.get(0);
            long last = record.getStartTimestamp();
            if (record.getEndTimestamp() != null) {
                last = Math.max(last, record.getEndTimestamp());
            }
            if (record.getModifyTimestamp() != null) {
                last = Math.max(last, record.getModifyTimestamp());
            }
            return last;
        } catch (Exception e) {
            LOG.warn("Exception for getting arterial stiffness start time", e);
        }
        return 0;
    }

    @Override
    public void handleData(Context context, GBDevice gbDevice, List<HuaweiP2PDataDictionarySyncService.DictData> dictData) {
        try (DBHandler db = GBApplication.acquireDB()) {
            final DaoSession session = db.getDaoSession();
            final Device device = DBHelper.getDevice(gbDevice, session);
            final User user = DBHelper.getUser(session);
            final HuaweiDictDataDao records = session.getHuaweiDictDataDao();
            final HuaweiDictDataValuesDao values = session.getHuaweiDictDataValuesDao();
            for (HuaweiP2PDataDictionarySyncService.DictData dt : dictData) {
                LOG.info("Arterial stiffness record: {}", dt);
                final List<HuaweiDictData> known = records.queryBuilder()
                        .where(HuaweiDictDataDao.Properties.DeviceId.eq(device.getId()),
                                HuaweiDictDataDao.Properties.DictClass.eq(ARTERIAL_STIFFNESS_CLASS),
                                HuaweiDictDataDao.Properties.StartTimestamp.eq(dt.getStartTimestamp()))
                        .limit(1)
                        .list();
                final HuaweiDictData record = known.isEmpty() ? new HuaweiDictData() : known.get(0);
                record.setDeviceId(device.getId());
                record.setUserId(user.getId());
                record.setDictClass(ARTERIAL_STIFFNESS_CLASS);
                record.setStartTimestamp(dt.getStartTimestamp());
                record.setEndTimestamp(dt.getEndTimestamp());
                record.setModifyTimestamp(dt.getModifyTimestamp());
                records.insertOrReplace(record);
                for (HuaweiP2PDataDictionarySyncService.DictData.DictDataValue val : dt.getData()) {
                    values.insertOrReplace(new HuaweiDictDataValues(record.getDictId(), val.getDataType(), val.getTag(), val.getValue()));
                }
            }
        } catch (Exception e) {
            LOG.error("Cannot save arterial stiffness records, continue", e);
        }
    }
}
