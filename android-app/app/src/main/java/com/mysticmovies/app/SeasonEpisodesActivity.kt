package com.mysticmovies.app

import android.app.DownloadManager
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Bundle
import android.os.Environment
import android.view.LayoutInflater
import android.view.View
import android.widget.Button
import android.widget.LinearLayout
import android.widget.TextView
import android.widget.Toast
import androidx.appcompat.app.AppCompatActivity
import androidx.lifecycle.lifecycleScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import okhttp3.Request
import org.json.JSONArray
import org.json.JSONObject

private data class EpisodeLink(
    val label: String,
    val streamUrl: String,
    val viewUrl: String,
    val downloadUrl: String,
    val telegramUrl: String,
    val telegramStartUrl: String,
    val telegramDeepLink: String,
    val watchTogetherUrl: String,
)

private data class EpisodeUi(
    val episode: Int,
    val title: String,
    val qualities: List<EpisodeLink>,
)

class SeasonEpisodesActivity : AppCompatActivity() {
    companion object {
        const val EXTRA_CONTENT_TITLE = "content_title"
        const val EXTRA_SEASON_NUMBER = "season_number"
        const val EXTRA_EPISODES_JSON = "episodes_json"
        const val EXTRA_IS_LOGGED_IN = "is_logged_in"
    }

    private val client = createApiHttpClient()
    private lateinit var tvTopbar: TextView
    private lateinit var tvHeaderTitle: TextView
    private lateinit var tvStatus: TextView
    private lateinit var tvFooterText: TextView
    private lateinit var btnBack: Button
    private lateinit var btnNavSearch: Button
    private lateinit var btnNavHome: Button
    private lateinit var btnNavDownloads: Button
    private lateinit var btnNavProfile: Button
    private lateinit var episodeContainer: LinearLayout

    private var contentTitle = ""
    private var seasonNo = 1
    private var isUserLoggedIn = false
    private var loginRequested = false
    private var openProfileAfterLogin = false
    private var pendingExpandEpisode = -1
    private var episodes: List<EpisodeUi> = emptyList()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_season_episodes)
        bindViews()
        bindActions()
        applyRuntimeUi()

        contentTitle = intent?.getStringExtra(EXTRA_CONTENT_TITLE).orEmpty().ifBlank { "Content" }
        seasonNo = intent?.getIntExtra(EXTRA_SEASON_NUMBER, 1) ?: 1
        isUserLoggedIn = intent?.getBooleanExtra(EXTRA_IS_LOGGED_IN, false) ?: false
        episodes = parseEpisodes(intent?.getStringExtra(EXTRA_EPISODES_JSON).orEmpty())

        tvHeaderTitle.text = "$contentTitle • Season $seasonNo"
        bindEpisodes()
    }

    override fun onResume() {
        super.onResume()
        applyRuntimeUi()
        if (loginRequested) {
            loginRequested = false
            lifecycleScope.launch {
                val loggedIn = withContext(Dispatchers.IO) { fetchSessionLoggedIn() }
                isUserLoggedIn = loggedIn
                if (loggedIn && openProfileAfterLogin) {
                    openProfileAfterLogin = false
                    startActivity(Intent(this@SeasonEpisodesActivity, ProfileActivity::class.java))
                    return@launch
                }
                bindEpisodes()
            }
        }
    }

    private fun bindViews() {
        tvTopbar = findViewById(R.id.tvTopbar)
        tvHeaderTitle = findViewById(R.id.tvHeaderTitle)
        tvStatus = findViewById(R.id.tvStatus)
        tvFooterText = findViewById(R.id.tvFooterText)
        btnBack = findViewById(R.id.btnBack)
        btnNavSearch = findViewById(R.id.btnNavSearch)
        btnNavHome = findViewById(R.id.btnNavHome)
        btnNavDownloads = findViewById(R.id.btnNavDownloads)
        btnNavProfile = findViewById(R.id.btnNavProfile)
        episodeContainer = findViewById(R.id.episodeContainer)
    }

    private fun bindActions() {
        btnBack.setOnClickListener { finish() }
        btnNavSearch.setOnClickListener {
            ensureLoggedInThen { startActivity(Intent(this, SearchActivity::class.java)) }
        }
        btnNavHome.setOnClickListener {
            startActivity(Intent(this, MainActivity::class.java).apply {
                addFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP)
            })
        }
        btnNavDownloads.setOnClickListener {
            startActivity(Intent(this, DownloadsActivity::class.java))
        }
        btnNavProfile.setOnClickListener {
            lifecycleScope.launch {
                val loggedIn = withContext(Dispatchers.IO) { fetchSessionLoggedIn() }
                isUserLoggedIn = loggedIn
                if (loggedIn) {
                    openProfileAfterLogin = false
                    startActivity(Intent(this@SeasonEpisodesActivity, ProfileActivity::class.java))
                } else {
                    openProfileAfterLogin = true
                    openLogin()
                }
            }
        }
    }

    private fun applyRuntimeUi() {
        tvTopbar.text = AppRuntimeState.ui.topbarText.ifBlank { "Welcome to Mystic Movies" }
        tvFooterText.text = AppRuntimeState.ui.footerText.ifBlank { "MysticMovies" }
    }

    private fun bindEpisodes() {
        episodeContainer.removeAllViews()
        if (episodes.isEmpty()) {
            tvStatus.text = "No episodes available."
            return
        }
        tvStatus.text = if (isUserLoggedIn) "Select episode to view links." else "Login required to access episode links."

        episodes.forEach { row ->
            val item = LayoutInflater.from(this).inflate(R.layout.item_episode_row, episodeContainer, false)
            val epTitle = item.findViewById<TextView>(R.id.tvEpisodeTitle)
            val epMeta = item.findViewById<TextView>(R.id.tvEpisodeMeta)
            val openButton = item.findViewById<Button>(R.id.btnOpenEpisode)
            val actions = item.findViewById<LinearLayout>(R.id.episodeActionsContainer)

            epTitle.text = if (row.title.isNotBlank()) "Episode ${row.episode} - ${row.title}" else "Episode ${row.episode}"
            val qualitySummary = row.qualities.map { it.label }.filter { it.isNotBlank() }
            epMeta.text = if (qualitySummary.isEmpty()) "No quality links" else qualitySummary.joinToString(" - ")

            fun toggleActions() {
                if (actions.visibility == View.VISIBLE) {
                    actions.visibility = View.GONE
                    return
                }
                actions.visibility = View.VISIBLE
                actions.removeAllViews()
                populateActions(actions, row)
            }

            openButton.setOnClickListener { toggleActions() }
            episodeContainer.addView(item)

            if (pendingExpandEpisode == row.episode) {
                pendingExpandEpisode = -1
                actions.visibility = View.VISIBLE
                populateActions(actions, row)
            }
        }
    }

    private fun populateActions(container: LinearLayout, row: EpisodeUi) {
        if (!isUserLoggedIn) {
            val loginCard = LayoutInflater.from(this).inflate(R.layout.item_login_access_row, container, false)
            loginCard.findViewById<TextView>(R.id.tvAccessMessage).text = "Login to access watch, download, watch together and Telegram options."
            loginCard.findViewById<Button>(R.id.btnLoginAccess).setOnClickListener {
                pendingExpandEpisode = row.episode
                openLogin()
            }
            container.addView(loginCard)
            return
        }
        if (row.qualities.isEmpty()) {
            val noData = TextView(this).apply {
                text = "No links available for this episode."
                setTextColor(0xFF9CA3AF.toInt())
                textSize = 12f
            }
            container.addView(noData)
            return
        }
        row.qualities.forEach { link ->
            val view = LayoutInflater.from(this).inflate(R.layout.item_link_row, container, false)
            view.findViewById<TextView>(R.id.tvLinkLabel).text = link.label.ifBlank { "HD" }
            view.findViewById<Button>(R.id.btnWatch).setOnClickListener {
                val stream = link.streamUrl.ifBlank { link.viewUrl }
                openPlayer(stream, "$contentTitle S$seasonNo E${row.episode} ${link.label}".trim())
            }
            view.findViewById<Button>(R.id.btnDownload).setOnClickListener {
                enqueueDownload(link.downloadUrl, "$contentTitle-S$seasonNo-E${row.episode}", link.label)
            }
            view.findViewById<Button>(R.id.btnTelegram).setOnClickListener {
                openTelegram(link)
            }
            view.findViewById<Button>(R.id.btnWatchTogether).setOnClickListener {
                openInAppWeb(link.watchTogetherUrl, "Watch Together")
            }
            container.addView(view)
        }
    }

    private fun parseEpisodes(raw: String): List<EpisodeUi> {
        if (raw.isBlank()) return emptyList()
        return try {
            val array = JSONArray(raw)
            val rows = mutableListOf<EpisodeUi>()
            for (i in 0 until array.length()) {
                val row = array.optJSONObject(i) ?: continue
                val qualities = mutableListOf<EpisodeLink>()
                val qualityRows = row.optJSONArray("qualities")
                if (qualityRows != null) {
                    for (q in 0 until qualityRows.length()) {
                        val qRow = qualityRows.optJSONObject(q) ?: continue
                        qualities.add(
                            EpisodeLink(
                                label = qRow.optString("label"),
                                streamUrl = qRow.optString("stream_url"),
                                viewUrl = qRow.optString("view_url"),
                                downloadUrl = qRow.optString("download_url"),
                                telegramUrl = qRow.optString("telegram_url"),
                                telegramStartUrl = qRow.optString("telegram_start_url"),
                                telegramDeepLink = qRow.optString("telegram_deep_link"),
                                watchTogetherUrl = qRow.optString("watch_together_url"),
                            )
                        )
                    }
                }
                rows.add(EpisodeUi(row.optInt("episode"), row.optString("title"), qualities))
            }
            rows
        } catch (_: Exception) {
            emptyList()
        }
    }

    private fun ensureLoggedInThen(action: () -> Unit) {
        lifecycleScope.launch {
            val loggedIn = withContext(Dispatchers.IO) { fetchSessionLoggedIn() }
            isUserLoggedIn = loggedIn
            if (loggedIn) action() else openLogin()
        }
    }

    private fun fetchSessionLoggedIn(): Boolean {
        for (base in apiBaseCandidates()) {
            try {
                val url = "${base.trimEnd('/')}/app-api/session".toHttpUrlOrNull() ?: continue
                val req = Request.Builder().url(url).get().build()
                client.newCall(req).execute().use { res ->
                    if (!res.isSuccessful) return@use
                    val root = JSONObject(res.body?.string().orEmpty())
                    if (!root.optBoolean("ok", false)) return@use
                    AppRuntimeState.apiBaseUrl = base.trimEnd('/')
                    return root.optBoolean("logged_in", false)
                }
            } catch (_: Exception) {
                // Try next base.
            }
        }
        return false
    }

    private fun openLogin() {
        loginRequested = true
        startActivity(Intent(this, LoginActivity::class.java).apply {
            putExtra("target_url", absoluteUrl("/login"))
            putExtra("title_text", "Login")
        })
    }

    private fun openPlayer(rawUrl: String, label: String) {
        val absolute = absoluteUrl(rawUrl)
        if (absolute.isBlank()) {
            Toast.makeText(this, "Play link is not available.", Toast.LENGTH_SHORT).show()
            return
        }
        startActivity(Intent(this, PlayerActivity::class.java).apply {
            putExtra(PlayerActivity.EXTRA_STREAM_URL, absolute)
            putExtra(PlayerActivity.EXTRA_TITLE, label)
        })
    }

    private fun enqueueDownload(rawUrl: String, title: String, quality: String) {
        val url = absoluteUrl(rawUrl)
        if (url.isBlank()) {
            Toast.makeText(this, "Download link is not available.", Toast.LENGTH_SHORT).show()
            return
        }
        try {
            val cleanTitle = title.replace(Regex("[^a-zA-Z0-9._-]+"), "_").take(60)
            val cleanQuality = quality.ifBlank { "HD" }.replace(Regex("[^a-zA-Z0-9._-]+"), "_").take(12)
            val fileName = "${cleanTitle}_${cleanQuality}_${System.currentTimeMillis()}.mp4"
            val req = DownloadManager.Request(Uri.parse(url))
                .setAllowedOverMetered(true)
                .setAllowedOverRoaming(true)
                .setTitle("$title $quality")
                .setDescription("Downloading in MysticMovies")
                .setNotificationVisibility(DownloadManager.Request.VISIBILITY_VISIBLE_NOTIFY_COMPLETED)
                .setDestinationInExternalFilesDir(this, Environment.DIRECTORY_DOWNLOADS, fileName)
            val dm = getSystemService(Context.DOWNLOAD_SERVICE) as DownloadManager
            dm.enqueue(req)
            Toast.makeText(this, "Download started. Check Downloads.", Toast.LENGTH_SHORT).show()
        } catch (_: Exception) {
            Toast.makeText(this, "Unable to start download.", Toast.LENGTH_SHORT).show()
        }
    }

    private fun openTelegram(link: EpisodeLink) {
        if (link.telegramDeepLink.isNotBlank()) {
            openTelegramIntent(link.telegramDeepLink)
            return
        }
        val startUrl = if (link.telegramStartUrl.isNotBlank()) {
            absoluteUrl(link.telegramStartUrl)
        } else {
            val token = parseShareToken(link.telegramUrl, "t")
            if (token.isBlank()) "" else absoluteUrl("/app-api/telegram-start/$token")
        }
        if (startUrl.isBlank()) {
            Toast.makeText(this, "Telegram link is not available.", Toast.LENGTH_SHORT).show()
            return
        }
        lifecycleScope.launch {
            val deepLink = withContext(Dispatchers.IO) { fetchTelegramDeepLink(startUrl) }
            if (deepLink.isBlank()) {
                Toast.makeText(this@SeasonEpisodesActivity, "Unable to open Telegram.", Toast.LENGTH_SHORT).show()
                return@launch
            }
            openTelegramIntent(deepLink)
        }
    }

    private fun fetchTelegramDeepLink(url: String): String {
        return try {
            val req = Request.Builder().url(url).get().build()
            client.newCall(req).execute().use { res ->
                if (!res.isSuccessful) return ""
                val body = JSONObject(res.body?.string().orEmpty())
                if (!body.optBoolean("ok")) return ""
                body.optString("deep_link").trim()
            }
        } catch (_: Exception) {
            ""
        }
    }

    private fun openTelegramIntent(url: String) {
        try {
            val intent = Intent(Intent.ACTION_VIEW, Uri.parse(url))
            intent.setPackage("org.telegram.messenger")
            startActivity(intent)
        } catch (_: Exception) {
            try {
                startActivity(Intent(Intent.ACTION_VIEW, Uri.parse(url)))
            } catch (_: Exception) {
                Toast.makeText(this, "Telegram app not found.", Toast.LENGTH_SHORT).show()
            }
        }
    }

    private fun openInAppWeb(rawUrl: String, titleText: String) {
        val target = absoluteUrl(rawUrl)
        if (target.isBlank()) return
        startActivity(Intent(this, LoginActivity::class.java).apply {
            putExtra("target_url", target)
            putExtra("title_text", titleText)
        })
    }
}
