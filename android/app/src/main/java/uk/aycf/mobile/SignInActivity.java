package uk.aycf.mobile;

import android.app.Activity;
import android.app.AlertDialog;
import android.net.Uri;
import android.os.Bundle;
import android.webkit.CookieManager;
import android.webkit.WebResourceRequest;
import android.webkit.WebView;
import android.webkit.WebViewClient;
import android.widget.Button;
import android.widget.LinearLayout;

public final class SignInActivity extends Activity {
    private static final String WALLET = "https://multipass.wizzair.com/en/w6/subscriptions/spa/private-page/wallets";
    private WebView web;
    private Button connect;

    @Override public void onCreate(Bundle state) {
        super.onCreate(state);
        getWindow().addFlags(android.view.WindowManager.LayoutParams.FLAG_SECURE);
        LinearLayout box = new LinearLayout(this);
        box.setFitsSystemWindows(true);
        box.setOrientation(LinearLayout.VERTICAL);
        connect = new Button(this);
        connect.setText("Connect after signing in to Wizz");
        web = new WebView(this);
        web.getSettings().setJavaScriptEnabled(true);
        web.getSettings().setDomStorageEnabled(true);
        web.getSettings().setAllowContentAccess(false);
        web.getSettings().setAllowFileAccess(false);
        web.setWebViewClient(new WebViewClient() {
            @Override public boolean shouldOverrideUrlLoading(WebView view, WebResourceRequest req) {
                return !"https".equals(req.getUrl().getScheme());
            }
        });
        connect.setOnClickListener(v -> connect());
        box.addView(connect);
        box.addView(web, new LinearLayout.LayoutParams(-1, 0, 1));
        setContentView(box);
        web.loadUrl(WALLET);
    }

    private void connect() {
        Uri page = Uri.parse(web.getUrl() == null ? "" : web.getUrl());
        if (!"https".equals(page.getScheme()) || !"multipass.wizzair.com".equals(page.getHost())) {
            message("Finish signing in and return to the Wizz Multipass page first."); return;
        }
        String session = CookieManager.getInstance().getCookie(WALLET);
        connect.setEnabled(false);
        connect.setText("Checking flight availability…");
        new Thread(() -> {
            try {
                Engine.runtime(this).callAttr("connect_account", session == null ? "" : session);
                ScanJob.request(this, "morning");
                runOnUiThread(() -> {setResult(RESULT_OK); finish();});
            } catch (Exception err) {
                runOnUiThread(() -> {
                    if (isFinishing() || isDestroyed()) return;
                    connect.setEnabled(true); connect.setText("Connect after signing in to Wizz");
                    message("Wizz did not confirm availability. Check your connection and complete a flight search in the Wizz page before trying again. Your existing saved flights are safe.");
                });
            }
        }, "Verify Wizz account").start();
    }

    private void message(String text) {
        new AlertDialog.Builder(this).setMessage(text).setPositiveButton("OK", null).show();
    }
    @Override public void onDestroy() { web.destroy(); super.onDestroy(); }
}
