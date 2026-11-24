package uk.aycf.mobile;

import android.app.Activity;
import android.app.AlertDialog;
import android.content.Intent;
import android.os.Bundle;
import android.text.InputType;
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
    private TextView status;
    private EditText email, password;
    private Button save, forget;

    @Override public void onCreate(Bundle state) {
        super.onCreate(state);
        getWindow().addFlags(WindowManager.LayoutParams.FLAG_SECURE);
        android.widget.ScrollView scroll = new android.widget.ScrollView(this);
        LinearLayout box = new LinearLayout(this);
        box.setOrientation(LinearLayout.VERTICAL);
        int pad = (int)(16 * getResources().getDisplayMetrics().density);
        box.setPadding(pad, pad, pad, pad);
        box.setFitsSystemWindows(true);
        scroll.addView(box); setContentView(scroll);
        TextView title = new TextView(this);
        title.setText("Remember Wizz login"); title.setTextSize(24); box.addView(title);
        TextView explanation = new TextView(this);
        explanation.setText("Save your Wizz email and password encrypted on this phone. AYCF checks the connection before a due scan and uses your saved login when the session expires. Wizz may ask you to complete verification.");
        box.addView(explanation);
        status = new TextView(this); status.setText("Loading login settings…"); box.addView(status);
        email = new EditText(this); email.setHint("Wizz email"); email.setSingleLine();
        email.setInputType(InputType.TYPE_CLASS_TEXT | InputType.TYPE_TEXT_VARIATION_EMAIL_ADDRESS);
        email.setSaveEnabled(false); box.addView(email);
        password = new EditText(this); password.setHint("Wizz password"); password.setSingleLine();
        password.setInputType(InputType.TYPE_CLASS_TEXT | InputType.TYPE_TEXT_VARIATION_PASSWORD);
        password.setSaveEnabled(false); box.addView(password);
        save = new Button(this); save.setText("Save on this phone and connect"); box.addView(save);
        forget = new Button(this); forget.setText("Forget saved login"); box.addView(forget);
        Button open = new Button(this); open.setText("Open Wizz sign-in"); box.addView(open);
        open.setOnClickListener(v -> startActivity(new Intent(this, SignInActivity.class)));
        save.setEnabled(false); forget.setEnabled(false);
        save.setOnClickListener(v -> save());
        forget.setOnClickListener(v -> new AlertDialog.Builder(this)
            .setMessage("Remove the saved email and password from this phone?")
            .setNegativeButton("Cancel", null).setPositiveButton("Forget", (d, w) -> forget()).show());
        worker.execute(() -> {
            try {
                PyObject settings = Engine.runtime(this).callAttr("login_settings");
                String address = settings.get("email").toString();
                boolean stored = settings.get("saved").toBoolean();
                runOnUiThread(() -> {
                    if (isDestroyed()) return;
                    email.setText(address); save.setEnabled(true); forget.setEnabled(stored);
                    status.setText(stored ? "Login saved on this phone. Automatic recovery is enabled." : "Enter your login to enable automatic recovery.");
                });
            } catch (Exception e) { showError("Saved login could not be read. You can save it again."); }
        });
    }

    private void save() {
        String address = email.getText().toString().trim();
        String secret = password.getText().toString();
        if (address.isEmpty() || secret.isEmpty()) { status.setText("Enter both your Wizz email and password."); return; }
        Engine.notifications(this);
        if (android.os.Build.VERSION.SDK_INT >= 33 &&
                checkSelfPermission(android.Manifest.permission.POST_NOTIFICATIONS) != android.content.pm.PackageManager.PERMISSION_GRANTED)
            requestPermissions(new String[]{android.Manifest.permission.POST_NOTIFICATIONS}, 10);
        save.setEnabled(false); forget.setEnabled(false); status.setText("Saving login…");
        worker.execute(() -> {
            try {
                Engine.runtime(this).callAttr("set_login", address, secret);
                runOnUiThread(() -> {
                    if (isDestroyed()) return;
                    password.setText(""); save.setEnabled(true); forget.setEnabled(true);
                    setResult(RESULT_OK);
                    status.setText("Login saved. The connection check is queued. Open Scan status for progress.");
                    try { ScanJob.request(this, "morning"); }
                    catch (RuntimeException e) { status.setText("Login saved. Start a scan from the planner when ready."); }
                });
            } catch (Exception e) { showError("Could not save login. Check the available storage and try again."); }
        });
    }

    private void forget() {
        save.setEnabled(false); forget.setEnabled(false);
        worker.execute(() -> {
            try {
                Engine.runtime(this).callAttr("forget_login");
                runOnUiThread(() -> {
                    if (isDestroyed()) return;
                    email.setText(""); password.setText(""); save.setEnabled(true);
                    setResult(RESULT_OK);
                    status.setText("Saved email and password removed.");
                });
            } catch (Exception e) { showError("Could not remove the saved login. Try again."); }
        });
    }

    private void showError(String message) {
        runOnUiThread(() -> {
            if (isDestroyed()) return;
            status.setText(message); save.setEnabled(true); forget.setEnabled(true);
        });
    }

    @Override public void onDestroy() {
        password.setText("");
        worker.shutdown();
        super.onDestroy();
    }
}
