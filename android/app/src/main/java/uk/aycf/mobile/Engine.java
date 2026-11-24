package uk.aycf.mobile;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.content.Context;
import android.content.Intent;
import android.security.keystore.KeyGenParameterSpec;
import android.security.keystore.KeyProperties;
import android.util.AtomicFile;
import android.util.Base64;
import com.chaquo.python.PyObject;
import com.chaquo.python.Python;
import com.chaquo.python.android.AndroidPlatform;
import java.io.File;
import java.io.FileOutputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

public final class Engine {
    private static Context context;
    private static PyObject runtime;
    private static final String CHANNEL = "aycf_flights";

    static synchronized Python python(Context ctx) {
        context = ctx.getApplicationContext();
        if (!Python.isStarted()) Python.start(new AndroidPlatform(context));
        return Python.getInstance();
    }

    static synchronized PyObject runtime(Context ctx) throws Exception {
        if (runtime == null) {
            PyObject mod = python(ctx).getModule("mobile_runtime");
            mod.callAttr("configure", ctx.getFilesDir().getAbsolutePath(), sessionKey());
            runtime = mod;
        }
        return runtime;
    }

    // Wrap the legacy vault key with a non-exportable Android Keystore key.
    private static String sessionKey() throws Exception {
        String alias = "aycf-session-v1";
        AtomicFile file = new AtomicFile(new File(context.getNoBackupFilesDir(), "session-key"));
        KeyStore store = KeyStore.getInstance("AndroidKeyStore");
        store.load(null);
        SecretKey key = (SecretKey) store.getKey(alias, null);
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        byte[] raw;
        if (file.getBaseFile().exists()) {
            byte[] data = file.readFully();
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(128, data, 0, 12));
            raw = cipher.doFinal(data, 12, data.length - 12);
        } else {
            if (key == null) {
                KeyGenerator generator = KeyGenerator.getInstance("AES", "AndroidKeyStore");
                generator.init(new KeyGenParameterSpec.Builder(alias, KeyProperties.PURPOSE_ENCRYPT | KeyProperties.PURPOSE_DECRYPT)
                    .setBlockModes(KeyProperties.BLOCK_MODE_GCM).setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE).build());
                key = generator.generateKey();
            }
            raw = new byte[32];
            new SecureRandom().nextBytes(raw);
            cipher.init(Cipher.ENCRYPT_MODE, key);
            byte[] encrypted = cipher.doFinal(raw);
            FileOutputStream output = file.startWrite();
            try {
                output.write(cipher.getIV());
                output.write(encrypted);
                file.finishWrite(output);
            } catch (Exception e) { file.failWrite(output); throw e; }
        }
        String encoded = Base64.encodeToString(raw, Base64.URL_SAFE | Base64.NO_WRAP);
        Arrays.fill(raw, (byte) 0);
        return encoded;
    }

    public static void requestAction(String action) {
        if ("repair".equals(action)) {
            context.startActivity(new Intent(context, WizzSettingsActivity.class).addFlags(Intent.FLAG_ACTIVITY_NEW_TASK));
        } else {
            ScanJob.request(context, action);
        }
    }

    static NotificationManager notifications(Context ctx) {
        NotificationManager manager = ctx.getSystemService(NotificationManager.class);
        manager.createNotificationChannel(new NotificationChannel(CHANNEL, "Flight watches", NotificationManager.IMPORTANCE_HIGH));
        return manager;
    }

    public static boolean notificationsEnabled() {
        NotificationManager manager = notifications(context);
        return manager.areNotificationsEnabled() && manager.getNotificationChannel(CHANNEL).getImportance() != NotificationManager.IMPORTANCE_NONE;
    }

    public static boolean notifySignIn(String body) {
        if (!notificationsEnabled()) return false;
        PendingIntent open = PendingIntent.getActivity(context, 42,
            new Intent(context, WizzSettingsActivity.class), PendingIntent.FLAG_IMMUTABLE | PendingIntent.FLAG_UPDATE_CURRENT);
        Notification notice = new Notification.Builder(context, CHANNEL)
            .setSmallIcon(android.R.drawable.ic_dialog_info).setContentTitle("Wizz needs your attention")
            .setContentText(body).setStyle(new Notification.BigTextStyle().bigText(body))
            .setAutoCancel(true).setContentIntent(open).build();
        notifications(context).notify(42, notice);
        return true;
    }

    public static boolean notifyWatch(int id, String title, String body) {
        if (!notificationsEnabled()) return false;
        PendingIntent open = PendingIntent.getActivity(context, 1, new Intent(context, MainActivity.class)
            .putExtra("watches", true).addFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP), PendingIntent.FLAG_IMMUTABLE | PendingIntent.FLAG_UPDATE_CURRENT);
        Notification notice = new Notification.Builder(context, CHANNEL)
            .setSmallIcon(android.R.drawable.ic_dialog_info).setContentTitle(title).setContentText(body)
            .setStyle(new Notification.BigTextStyle().bigText(body)).setAutoCancel(true).setContentIntent(open).build();
        notifications(context).notify(id, notice);
        return true;
    }
}
