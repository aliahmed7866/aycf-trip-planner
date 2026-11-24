package uk.aycf.mobile;

import android.Manifest;
import android.app.Activity;
import android.app.AlertDialog;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.graphics.Color;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.provider.Settings;
import android.webkit.CookieManager;
import android.webkit.URLUtil;
import android.webkit.WebResourceRequest;
import android.webkit.WebResourceError;
import android.webkit.WebChromeClient;
import android.webkit.WebView;
import android.webkit.WebViewClient;
import android.widget.Button;
import android.widget.LinearLayout;
import android.widget.ProgressBar;
import android.widget.TextView;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class MainActivity extends Activity {
    private static final int PICK = 1, SAVE = 2, LOGIN = 3, LOGIN_SETTINGS = 4;
    private final ExecutorService io = Executors.newSingleThreadExecutor();
    private String origin, download;
    private LinearLayout box;
    private WebView web;
    private ProgressBar pageProgress;
    private LinearLayout pageError;
    private Uri savedPage;

    @Override public void onCreate(Bundle state) {
        super.onCreate(state);
        if (state != null) {
            download = state.getString("download");
            savedPage = state.getParcelable("page");
        }
        if (new File(getFilesDir(), "planner").isDirectory()) openPlanner(); else welcome();
    }

    private int dp(int value) { return (int)(getResources().getDisplayMetrics().density * value + .5); }
    private void reset() {
        box = new LinearLayout(this);
        box.setOrientation(LinearLayout.VERTICAL);
        box.setFitsSystemWindows(true);
        box.setBackgroundColor(Color.rgb(11,21,25));
        setContentView(box);
    }

    private TextView label(String text, int size) {
        TextView view = new TextView(this);
        view.setText(text); view.setTextSize(size); view.setTextColor(Color.WHITE);
        view.setPadding(dp(16), dp(12), dp(16), dp(12));
        return view;
    }
    private Button button(String text, Runnable run) {
        Button b = new Button(this); b.setText(text); b.setAllCaps(false);
        b.setOnClickListener(v -> run.run()); return b;
    }

    private void welcome() {
        reset();
        box.setPadding(dp(18), dp(32), dp(18), dp(18));
        box.addView(label("AYCF Flight Planner", 27));
        box.addView(label("Bring your saved flights, watches and preferences from Termux, or start a new planner on this phone.", 17));
        box.addView(button("Import Termux backup", () -> startActivityForResult(
            new Intent(Intent.ACTION_OPEN_DOCUMENT).setType("application/zip").addCategory(Intent.CATEGORY_OPENABLE), PICK)));
        box.addView(button("Start fresh", () -> new AlertDialog.Builder(this)
            .setMessage("Start with an empty planner? Importing a Termux backup is available during initial setup.")
            .setNegativeButton("Back", null).setPositiveButton("Start fresh", (d,w) -> openPlanner()).show()));
    }

    private void busy(String status) {
        reset();
        box.addView(label(status, 20)); box.addView(new ProgressBar(this));
    }

    private void openPlanner() {
        busy("Opening your planner…");
        io.execute(() -> {
            try {
                String[] parts = Engine.runtime(this).callAttr("start_console").toString().split("\n", 2);
                runOnUiThread(() -> {
                    if (isDestroyed() || isFinishing()) return;
                    origin = parts[0]; reset();
                    LinearLayout bar = new LinearLayout(this);
                    bar.setGravity(android.view.Gravity.CENTER_VERTICAL);
                    bar.addView(label("AYCF · Your travel pass", 17), new LinearLayout.LayoutParams(0, -2, 1));
                    bar.addView(button("Menu", this::menu));
                    box.addView(bar);
                    pageProgress = new ProgressBar(this, null, android.R.attr.progressBarStyleHorizontal);
                    pageProgress.setMax(100);
                    box.addView(pageProgress, new LinearLayout.LayoutParams(-1, dp(3)));
                    pageError = new LinearLayout(this);
                    pageError.setOrientation(LinearLayout.VERTICAL);
                    pageError.addView(label("This page could not load. Your saved flights are still on this phone.", 16));
                    pageError.addView(button("Open planner", () -> web.loadUrl(origin)));
                    pageError.setVisibility(android.view.View.GONE);
                    box.addView(pageError);
                    web = new WebView(this);
                    web.setBackgroundColor(Color.rgb(11,21,25));
                    web.getSettings().setJavaScriptEnabled(true);
                    web.getSettings().setDomStorageEnabled(true);
                    web.getSettings().setAllowFileAccess(false);
                    web.getSettings().setAllowContentAccess(false);
                    web.setWebChromeClient(new WebChromeClient() {
                        @Override public void onProgressChanged(WebView view, int progress) {
                            pageProgress.setProgress(progress);
                            pageProgress.setVisibility(progress < 100 ? android.view.View.VISIBLE : android.view.View.INVISIBLE);
                        }
                    });
                    web.setWebViewClient(new WebViewClient() {
                        @Override public void onPageStarted(WebView view, String url, android.graphics.Bitmap icon) {
                            pageError.setVisibility(android.view.View.GONE);
                        }
                        @Override public void onReceivedError(WebView view, WebResourceRequest request, WebResourceError error) {
                            if (request.isForMainFrame()) pageError.setVisibility(android.view.View.VISIBLE);
                        }
                        @Override public boolean shouldOverrideUrlLoading(WebView view, WebResourceRequest request) {
                            Uri url = request.getUrl();
                            if (isLocal(url)) return false;
                            if (request.isForMainFrame() && request.hasGesture() && "https".equals(url.getScheme())) {
                                try { startActivity(new Intent(Intent.ACTION_VIEW, url)); } catch (android.content.ActivityNotFoundException ignored) {}
                            }
                            return true;
                        }
                    });
                    web.setDownloadListener((url, ua, disposition, type, length) -> {
                        if (!isLocal(Uri.parse(url))) return;
                        download = url;
                        Intent save = new Intent(Intent.ACTION_CREATE_DOCUMENT).addCategory(Intent.CATEGORY_OPENABLE)
                            .setType(type == null ? "application/octet-stream" : type)
                            .putExtra(Intent.EXTRA_TITLE, URLUtil.guessFileName(url, disposition, type));
                        startActivityForResult(save, SAVE);
                    });
                    box.addView(web, new LinearLayout.LayoutParams(-1, 0, 1));
                    String dest = getIntent().getBooleanExtra("watches", false) ? origin + "watches" : origin;
                    if (savedPage != null && savedPage.getEncodedPath() != null && savedPage.getEncodedPath().startsWith("/")) {
                        dest = origin.substring(0, origin.length()-1) + savedPage.getEncodedPath();
                        if (savedPage.getEncodedQuery() != null) dest += "?" + savedPage.getEncodedQuery();
                    }
                    final String landing = dest;
                    CookieManager.getInstance().setCookie(origin, "aycf_device=" + parts[1] + "; Path=/; HttpOnly; SameSite=Strict",
                        ok -> { if (Boolean.TRUE.equals(ok)) web.loadUrl(landing); else showError("Android could not open the private planner session."); });
                    ScanJob.schedule(this);
                });
            } catch (Exception ex) {
                android.util.Log.e("AYCF", "Planner startup failed", ex);
                runOnUiThread(() -> { if (!isDestroyed()) { busy("Planner could not open"); box.addView(button("Try again", this::openPlanner)); } });
            }
        });
    }

    private boolean isLocal(Uri uri) {
        return origin != null && "http".equals(uri.getScheme()) && "127.0.0.1".equals(uri.getHost())
            && uri.getUserInfo() == null && uri.getPort() == Uri.parse(origin).getPort();
    }

    private void menu() {
        new AlertDialog.Builder(this).setItems(new String[]{"Wizz account", "Flight alerts", "Manchester connections", "Scan status", "Optional fare API key"},
            (dialog, item) -> {
                if (item == 0) startActivityForResult(new Intent(this, WizzSettingsActivity.class), LOGIN_SETTINGS);
                if (item == 1) {
                    Engine.notifications(this);
                    if (Build.VERSION.SDK_INT >= 33 && checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED)
                        requestPermissions(new String[]{Manifest.permission.POST_NOTIFICATIONS}, 10);
                    else startActivity(new Intent(Settings.ACTION_APP_NOTIFICATION_SETTINGS).putExtra(Settings.EXTRA_APP_PACKAGE, getPackageName()));
                }
                if (item == 2) web.loadUrl(origin + "feeder-trips");
                if (item == 4) fareKey();
                if (item == 3) web.loadUrl(origin + "system");
            }).show();
    }

    private void fareKey() {
        android.widget.EditText field = new android.widget.EditText(this);
        field.setSingleLine(); field.setInputType(129); field.setHint("Your SerpApi key");
        new AlertDialog.Builder(this).setTitle("Manchester fare checks").setView(field).setMessage("For automatic cash prices only. Enter your SerpApi key, or leave blank to remove it.")
            .setNegativeButton("Cancel", null).setPositiveButton("Save", (d,w) -> {
                String key = field.getText().toString().trim();
                io.execute(() -> {
                    try {
                        Engine.runtime(this).callAttr("set_fare_key", key);
                        runOnUiThread(() -> { if (!isDestroyed() && web != null) web.loadUrl(origin + "feeder-trips"); });
                    }
                    catch (Exception e) { runOnUiThread(() -> showError("Could not save the fare key.")); }
                });
            }).show();
    }

    private void showError(String text) {
        if (!isFinishing() && !isDestroyed()) new AlertDialog.Builder(this).setMessage(text).setPositiveButton("OK", null).show();
    }

    @Override protected void onActivityResult(int code, int result, Intent data) {
        super.onActivityResult(code, result, data);
        if (result != RESULT_OK) return;
        if (code == LOGIN || code == LOGIN_SETTINGS) { if (web != null) web.loadUrl(origin + "system"); return; }
        if (data == null || data.getData() == null) return;
        Uri selected = data.getData();
        if (code == PICK) {
            busy("Checking and importing your backup…");
            io.execute(() -> {
                File zip = new File(getCacheDir(), "termux-import.zip");
                try {
                    try (InputStream in = getContentResolver().openInputStream(selected); OutputStream out = new FileOutputStream(zip)) {
                        copy(in, out, 2L << 30);
                    }
                    Engine.python(this).getModule("transfer").callAttr("restore_backup", zip.toString(), new File(getFilesDir(), "planner").toString());
                    runOnUiThread(this::openPlanner);
                } catch (Exception e) {
                    runOnUiThread(() -> { welcome(); showError("Import failed. Choose the ZIP made by the AYCF transfer script. Your Termux data is safe."); });
                } finally { zip.delete(); }
            });
        } else if (code == SAVE && download != null && isLocal(Uri.parse(download))) {
            String url = download; download = null;
            String cookie = CookieManager.getInstance().getCookie(url);
            io.execute(() -> {
                HttpURLConnection conn = null;
                try {
                    conn = (HttpURLConnection)new URL(url).openConnection();
                    conn.setInstanceFollowRedirects(false); conn.setRequestProperty("Cookie", cookie);
                    if (conn.getResponseCode() != 200) throw new java.io.IOException("Download failed");
                    try (InputStream in = conn.getInputStream(); OutputStream out = getContentResolver().openOutputStream(selected)) {
                        copy(in, out, 64L << 20);
                    }
                } catch (Exception ex) { runOnUiThread(() -> showError("The download could not be saved.")); }
                finally { if (conn != null) conn.disconnect(); }
            });
        }
    }

    private static void copy(InputStream in, OutputStream out, long limit) throws java.io.IOException {
        long total = 0; byte[] block = new byte[65536]; int length;
        while ((length = in.read(block)) != -1) {
            total += length;
            if (total > limit) throw new java.io.IOException("File exceeds supported size");
            out.write(block, 0, length);
        }
    }

    @Override protected void onSaveInstanceState(Bundle out) {
        super.onSaveInstanceState(out); out.putString("download", download);
        if (web != null && web.getUrl() != null) out.putParcelable("page", Uri.parse(web.getUrl()));
    }
    @Override public void onBackPressed() {
        if (web != null && web.canGoBack()) web.goBack(); else super.onBackPressed();
    }
    @Override protected void onDestroy() {
        if (web != null) web.destroy(); io.shutdown(); super.onDestroy();
    }
}
