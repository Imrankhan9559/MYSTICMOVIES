package com.mysticmovies.app

import android.content.Intent
import android.net.Uri
import android.os.Bundle
import android.view.View
import android.webkit.WebChromeClient
import android.webkit.WebResourceRequest
import android.webkit.WebSettings
import android.webkit.WebView
import android.webkit.WebViewClient
import android.widget.Button
import android.widget.ProgressBar
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity

class LoginActivity : AppCompatActivity() {
    private lateinit var webView: WebView
    private lateinit var progressBar: ProgressBar
    private lateinit var tvTitle: TextView
    private var initialUrl: String = ""

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_webview)

        webView = findViewById(R.id.webView)
        progressBar = findViewById(R.id.progressBar)
        tvTitle = findViewById(R.id.tvTitle)
        findViewById<Button>(R.id.btnBack).setOnClickListener {
            if (webView.canGoBack()) {
                webView.goBack()
            } else {
                finish()
            }
        }

        val target = intent?.getStringExtra("target_url").orEmpty().trim()
        val titleText = intent?.getStringExtra("title_text").orEmpty().trim()
        tvTitle.text = if (titleText.isNotBlank()) titleText else "Login"
        initialUrl = if (target.isNotBlank()) absoluteUrl(target) else absoluteUrl("/login")

        if (consumeAuthCallback(intent)) {
            return
        }

        setupWebView()
        webView.loadUrl(initialUrl)
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        setIntent(intent)
        consumeAuthCallback(intent)
    }

    private fun setupWebView() {
        val s = webView.settings
        s.javaScriptEnabled = true
        s.domStorageEnabled = true
        s.loadsImagesAutomatically = true
        s.allowFileAccess = true
        s.cacheMode = WebSettings.LOAD_DEFAULT
        s.mediaPlaybackRequiresUserGesture = false

        webView.webChromeClient = object : WebChromeClient() {
            override fun onProgressChanged(view: WebView?, newProgress: Int) {
                progressBar.visibility = if (newProgress in 1..99) View.VISIBLE else View.GONE
            }
        }
        webView.webViewClient = object : WebViewClient() {
            override fun shouldOverrideUrlLoading(view: WebView?, request: WebResourceRequest?): Boolean {
                val url = request?.url?.toString().orEmpty()
                if (url.isBlank()) return false
                if (isGoogleAuthUrl(url)) {
                    openExternal(ensureAppGoogleFlow(url))
                    return true
                }
                if (url.startsWith("mysticmovies://")) {
                    return consumeAuthCallback(Intent(Intent.ACTION_VIEW, Uri.parse(url)))
                }
                if (url.startsWith("tg://") || url.contains("t.me/")) {
                    openExternal(url)
                    return true
                }
                if (url.startsWith("mailto:") || url.startsWith("tel:")) {
                    openExternal(url)
                    return true
                }
                return false
            }
        }
    }

    private fun isGoogleAuthUrl(url: String): Boolean {
        val lower = url.trim().lowercase()
        return lower.contains("/auth/google") || lower.contains("accounts.google.com")
    }

    private fun ensureAppGoogleFlow(url: String): String {
        return try {
            val uri = Uri.parse(url)
            val path = uri.path.orEmpty()
            if (!path.contains("/auth/google")) return url
            if (!uri.getQueryParameter("app").isNullOrBlank()) return url
            uri.buildUpon().appendQueryParameter("app", "1").build().toString()
        } catch (_: Exception) {
            url
        }
    }

    private fun consumeAuthCallback(incoming: Intent?): Boolean {
        val data = incoming?.data ?: return false
        val scheme = data.scheme.orEmpty().lowercase()
        val host = data.host.orEmpty().lowercase()
        if (scheme != "mysticmovies" || host != "auth") return false

        val token = data.getQueryParameter("token").orEmpty().trim()
        if (token.isBlank()) {
            finish()
            return true
        }
        AppRuntimeState.saveAuthToken(this, token)
        setResult(RESULT_OK)
        finish()
        return true
    }

    private fun openExternal(url: String) {
        try {
            startActivity(Intent(Intent.ACTION_VIEW, Uri.parse(url)))
        } catch (_: Exception) {
            // Ignore.
        }
    }

    @Deprecated("Deprecated in Java")
    override fun onBackPressed() {
        if (webView.canGoBack()) {
            webView.goBack()
            return
        }
        super.onBackPressed()
    }

    override fun onDestroy() {
        webView.stopLoading()
        webView.loadUrl("about:blank")
        webView.webViewClient = WebViewClient()
        webView.webChromeClient = WebChromeClient()
        webView.clearHistory()
        webView.clearCache(true)
        webView.destroy()
        super.onDestroy()
    }
}
