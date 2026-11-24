package uk.aycf.mobile;

import android.app.Activity;
import android.app.AlertDialog;
import android.content.Intent;
import android.os.Bundle;
import android.text.InputType;
import android.view.View;
import android.view.WindowManager;
import android.widget.Button;
import android.widget.EditText;
import android.widget.LinearLayout;
import android.widget.TextView;
import com.chaquo.python.PyObject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class WizzSettingsActivity extends Activity {
    private final ExecutorService worker = Executors.newSingleThreadExecutor();
    private TextView status, account;
    private EditText email, password;
    private LinearLayout fields;
    private Button save, forget, reconnect, change, verify;
    private boolean stored, busy;

    @Override public void onCreate(Bundle state) {
        super.onCreate(state);
        getWindow().addFlags(WindowManager.LayoutParams.FLAG_SECURE);
        android.widget.ScrollView scroll = new android.widget.ScrollView(this);
        LinearLayout box = new LinearLayout(this);
        box.setOrientation(LinearLayout.VERTICAL);
        int pad = (int)(20 * getResources().getDisplayMetrics().density);
        box.setPadding(pad, pad, pad, pad); box.setFitsSystemWindows(true);
        scroll.addView(box); setContentView(scroll);
        TextView title = new TextView(this);
        title.setText("Wizz account"); title.setTextSize(26); box.addView(title);
        TextView explanation = new TextView(this);
        explanation.setText("Enter your login once. AYCF keeps it encrypted on this phone and reconnects before scans. Wizz may occasionally ask you to complete verification.");
        explanation.setPadding(0, pad / 2, 0, pad); box.addView(explanation);
        account = new TextView(this); account.setTextSize(18); box.addView(account);
        status = new TextView(this); status.setText("Loading your account…");
        status.setPadding(0, pad / 2, 0, pad / 2);
        status.setAccessibilityLiveRegion(View.ACCESSIBILITY_LIVE_REGION_POLITE); box.addView(status);
        fields = new LinearLayout(this); fields.setOrientation(LinearLayout.VERTICAL); box.addView(fields);
        email = new EditText(this); email.setHint("Wizz email"); email.setSingleLine();
        email.setInputType(InputType.TYPE_CLASS_TEXT | InputType.TYPE_TEXT_VARIATION_EMAIL_ADDRESS);
        email.setSaveEnabled(false); fields.addView(email);
        password = new EditText(this); password.setHint("Wizz password"); password.setSingleLine();
        password.setInputType(InputType.TYPE_CLASS_TEXT | InputType.TYPE_TEXT_VARIATION_PASSWORD);
        password.setSaveEnabled(false); fields.addView(password);
        save = button(fields, "Save and connect", this::save);
        reconnect = button(box, "Reconnect with saved login", () -> connect(false));
        verify = button(box, "Complete Wizz verification", () ->
            startActivityForResult(new Intent(this, SignInActivity.class), 1));
        change = button(box, "Change login", () -> {
            boolean edit = fields.getVisibility() != View.VISIBLE;
            fields.setVisibility(edit ? View.VISIBLE : View.GONE);
            change.setText(edit ? "Cancel changes" : "Change login");
            password.setText("");
        });
        forget = button(box, "Forget saved login", () -> new AlertDialog.Builder(this)
            .setMessage("Remove the saved email and password from this phone? The current wallet stays connected until its session expires.")
            .setNegativeButton("Cancel", null).setPositiveButton("Forget", (d, w) -> forget()).show());
        setBusy(true); load();
    }

    private Button button(LinearLayout box, String title, Runnable action) {
        Button button = new Button(this); button.setAllCaps(false); button.setText(title);
        button.setOnClickListener(v -> action.run()); box.addView(button); return button;
    }

    private void setBusy(boolean value) {
        busy = value;
        for (Button button : new Button[]{save, forget, reconnect, change, verify}) button.setEnabled(!value);
        email.setEnabled(!value); password.setEnabled(!value);
    }

    private void render(PyObject settings) {
        stored = settings.get("saved").toBoolean();
        String address = settings.get("email").toString();
        account.setText(stored ? address + "\nLogin saved securely" : "Connect your Wizz account");
        email.setText(address); password.setText("");
        fields.setVisibility(stored ? View.GONE : View.VISIBLE);
        change.setText("Change login");
        for (Button button : new Button[]{reconnect, change, forget}) button.setVisibility(stored ? View.VISIBLE : View.GONE);
        verify.setText(stored ? "Complete Wizz verification" : "Open Wizz sign-in");
        String detail = settings.get("message").toString();
        status.setText(settings.get("needs_verification").toBoolean() ? detail : stored ?
            "You do not need to enter your password again. AYCF reconnects automatically when possible." :
            "Save your email and password to enable automatic sign-in.");
        setBusy(false);
    }

    private void load() {
        worker.execute(() -> {
            try {
                PyObject settings = Engine.runtime(this).callAttr("login_settings");
                runOnUiThread(() -> { if (!isDestroyed()) render(settings); });
            } catch (Exception e) { showError("Saved login could not be read. Save your login again."); }
        });
    }

    private void save() {
        if (email.getText().toString().trim().isEmpty() || password.getText().toString().isEmpty()) {
            status.setText("Enter both your Wizz email and password."); return;
        }
        Engine.notifications(this);
        if (android.os.Build.VERSION.SDK_INT >= 33 &&
                checkSelfPermission(android.Manifest.permission.POST_NOTIFICATIONS) != android.content.pm.PackageManager.PERMISSION_GRANTED)
            requestPermissions(new String[]{android.Manifest.permission.POST_NOTIFICATIONS}, 10);
        connect(true);
    }

    private void connect(boolean saveFirst) {
        if (busy) return;
        String address = email.getText().toString().trim();
        String secret = saveFirst ? password.getText().toString() : "";
        setBusy(true); status.setText("Connecting to Wizz… This can take up to 90 seconds.");
        worker.execute(() -> {
            try {
                PyObject runtime = Engine.runtime(this);
                if (saveFirst) {
                    runtime.callAttr("set_login", address, secret);
                    PyObject saved = runtime.callAttr("login_settings");
                    runOnUiThread(() -> {
                        if (isDestroyed()) return;
                        render(saved); setBusy(true); setResult(RESULT_OK);
                        status.setText("Login saved. Connecting to Wizz… This can take up to 90 seconds.");
                    });
                }
                PyObject result = runtime.callAttr("connect_saved_login");
                boolean connected = result.get("connected").toBoolean();
                String connectedMessage = "Connected. Your login is remembered and your flight scan is queued.";
                if (connected) {
                    try { ScanJob.request(this, "morning"); }
                    catch (RuntimeException e) { connectedMessage = "Connected. Your login is remembered. Open Scan status to start a scan."; }
                }
                final String notice = connectedMessage;
                runOnUiThread(() -> {
                    if (isDestroyed()) return;
                    render(result); setResult(RESULT_OK);
                    status.setText(connected ? notice :
                        result.get("message").toString());
                });
            } catch (Exception e) { showError("The connection check could not finish. Any saved login is kept. Try Reconnect or complete Wizz verification."); }
        });
    }

    private void forget() {
        setBusy(true);
        worker.execute(() -> {
            try {
                Engine.runtime(this).callAttr("forget_login");
                PyObject result = Engine.runtime(this).callAttr("login_settings");
                runOnUiThread(() -> {
                    if (isDestroyed()) return;
                    render(result); setResult(RESULT_OK); status.setText("Saved email and password removed.");
                });
            } catch (Exception e) { showError("Could not remove the saved login. Try again."); }
        });
    }

    private void showError(String message) {
        runOnUiThread(() -> { if (!isDestroyed()) { password.setText(""); status.setText(message); setBusy(false); } });
    }

    @Override protected void onActivityResult(int code, int result, Intent data) {
        super.onActivityResult(code, result, data);
        if (code == 1) { if (result == RESULT_OK) setResult(RESULT_OK); load(); }
    }

    @Override public void onDestroy() {
        password.setText(""); worker.shutdown(); super.onDestroy();
    }
}
