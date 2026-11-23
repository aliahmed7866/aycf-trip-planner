package uk.aycf.mobile;

import android.app.Activity;
import android.net.Uri;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.os.SystemClock;
import android.view.View;
import android.webkit.CookieManager;
import android.webkit.WebChromeClient;
import android.webkit.WebResourceError;
import android.webkit.WebResourceRequest;
import android.webkit.WebView;
import android.webkit.WebViewClient;
import android.widget.Button;
import android.widget.LinearLayout;
import android.widget.TextView;
import android.widget.Toast;
import com.chaquo.python.PyObject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class SignInActivity extends Activity {
    private static final String WALLET = "https://multipass.wizzair.com/en/w6/subscriptions/spa/private-page/wallets";
    private static final long CHECK_MS = 20_000;
    private static final String PROMPT = "Sign in on the Wizz page, then tap Connect wallet.";
    private final Handler ui = new Handler(Looper.getMainLooper());
    private final ExecutorService worker = Executors.newSingleThreadExecutor();
    private final Runnable timeout = () -> stopCheck("The wallet check timed out. Check your connection and tap Connect wallet to retry.");
    private WebView web;
    private TextView status;
    private Button connect;
    private Button reopen;
    private int attempt;
    private Future<?> checking;
    private boolean saving;
    private boolean autoChecked;

    @Override public void onCreate(Bundle state) {
        super.onCreate(state);
        getWindow().addFlags(android.view.WindowManager.LayoutParams.FLAG_SECURE);
        LinearLayout box = new LinearLayout(this);
        box.setFitsSystemWindows(true);
        box.setOrientation(LinearLayout.VERTICAL);

        status = new TextView(this);
        int pad = (int)(12 * getResources().getDisplayMetrics().density);
        status.setPadding(pad, pad, pad, 0);
        status.setAccessibilityLiveRegion(View.ACCESSIBILITY_LIVE_REGION_POLITE);
        status.setText(PROMPT);
        connect = new Button(this);
        connect.setText("Connect wallet");
        connect.setOnClickListener(v -> {
            if (checking != null) stopCheck("Check cancelled. You can try again from your Wizz wallet.");
            else connect();
        });
        reopen = new Button(this);
        reopen.setText("Open Wizz wallet");
        reopen.setVisibility(View.GONE);
        reopen.setOnClickListener(v -> {
            status.setText(PROMPT); reopen.setVisibility(View.GONE); web.loadUrl(WALLET);
        });

        web = new WebView(this);
        web.getSettings().setJavaScriptEnabled(true);
        web.getSettings().setDomStorageEnabled(true);
        web.getSettings().setAllowContentAccess(false);
        web.getSettings().setAllowFileAccess(false);
        web.setWebChromeClient(new WebChromeClient());
        web.setWebViewClient(new WebViewClient() {
            @Override public void onPageFinished(WebView view, String url) {
                Uri page = Uri.parse(url);
                if (!autoChecked && checking == null && !saving && "https".equals(page.getScheme())
                        && "multipass.wizzair.com".equals(page.getHost())
                        && page.getPath() != null && page.getPath().contains("/private-page/")) {
                    autoChecked = true;
                    connect();
                }
            }
            @Override public boolean shouldOverrideUrlLoading(WebView view, WebResourceRequest req) {
                return !"https".equals(req.getUrl().getScheme());
            }
            @Override public void onReceivedError(WebView view, WebResourceRequest req, WebResourceError err) {
                if (req.isForMainFrame() && checking == null && !saving)
                    ready("The Wizz page could not load. Check your internet connection and reopen the wallet.");
            }
        });
        box.addView(status);
        box.addView(connect);
        box.addView(reopen);
        Button remember = new Button(this);
        remember.setText("Remember Wizz login");
        remember.setOnClickListener(v -> startActivity(new android.content.Intent(this, WizzSettingsActivity.class)));
        box.addView(remember);
        box.addView(web, new LinearLayout.LayoutParams(-1, 0, 1));
        setContentView(box);
        web.loadUrl(WALLET);
    }

    private void ready(String message) {
        status.setText(message);
        connect.setEnabled(true);
        connect.setText("Connect wallet");
        reopen.setVisibility(View.VISIBLE);
    }

    private void stopCheck(String message) {
        ++attempt;
        ui.removeCallbacks(timeout);
        if (checking != null) checking.cancel(true);
        checking = null;
        ready(message);
    }

    private void connect() {
        Uri page = Uri.parse(web.getUrl() == null ? "" : web.getUrl());
        if (!"https".equals(page.getScheme()) || !"multipass.wizzair.com".equals(page.getHost())
                || (page.getPort() != -1 && page.getPort() != 443)) {
            ready("Finish signing in on the Wizz page, then open your Multipass wallet."); return;
        }
        String session = CookieManager.getInstance().getCookie(WALLET);
        if (session == null || session.isEmpty()) {
            ready("Finish signing in on the Wizz page and open your Multipass wallet."); return;
        }
        final int id = ++attempt;
        long end = SystemClock.elapsedRealtime() + CHECK_MS;
        status.setText("Checking your Wizz wallet…");
        connect.setText("Cancel check");
        reopen.setVisibility(View.GONE);
        ui.postDelayed(timeout, CHECK_MS);
        checking = worker.submit(() -> {
            try {
                PyObject runtime = Engine.runtime(this);
                double seconds = Math.max(0, end - SystemClock.elapsedRealtime()) / 1000.0;
                PyObject result = runtime.callAttr("check_account", session, seconds);
                PyObject problem = result.callAttr("get", "message");
                String message = problem == null ? null : problem.toString();
                ui.post(() -> {
                    if (!current(id)) return;
                    if (SystemClock.elapsedRealtime() >= end) { timeout.run(); return; }
                    ui.removeCallbacks(timeout);
                    checking = null;
                    if (message != null) { ready(message); return; }
                    save(runtime, result, id);
                });
            } catch (Exception e) {
                ui.post(() -> {
                    if (current(id)) stopCheck("AYCF could not check the wallet on this phone. Reopen the wallet and try again.");
                });
            }
        });
    }

    private boolean current(int id) {
        return id == attempt && !isFinishing() && !isDestroyed();
    }

    // Only a result accepted by the visible activity may replace the saved session.
    private void save(PyObject runtime, PyObject result, int id) {
        saving = true;
        status.setText("Saving your verified wallet…");
        connect.setEnabled(false);
        worker.execute(() -> {
            try {
                runtime.callAttr("save_checked_account", result);
            } catch (Exception e) {
                ui.post(() -> {
                    if (current(id)) { saving = false; ready("AYCF could not save the wallet on this phone. Check the available storage and try again."); }
                });
                return;
            }
            String notice;
            try {
                ScanJob.request(this, "morning");
                notice = "Wizz wallet connected. See Scan status for flight availability.";
            } catch (RuntimeException e) {
                notice = "Wizz wallet connected. Open Scan status to start a scan.";
            }
            final String message = notice;
            ui.post(() -> {
                if (!current(id)) return;
                Toast.makeText(this, message, Toast.LENGTH_LONG).show();
                setResult(RESULT_OK); finish();
            });
        });
    }

    @Override protected void onStop() {
        super.onStop();
        CookieManager.getInstance().flush();
    }

    @Override public void onDestroy() {
        ++attempt;
        ui.removeCallbacksAndMessages(null);
        if (checking != null) checking.cancel(true);
        worker.shutdown();
        web.destroy();
        super.onDestroy();
    }
}
