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
import android.widget.ImageView
import android.widget.LinearLayout
import android.widget.ProgressBar
import android.widget.TextView
import android.widget.Toast
import androidx.appcompat.app.AppCompatActivity
import androidx.lifecycle.lifecycleScope
import coil.load
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import okhttp3.Request
import org.json.JSONArray
import org.json.JSONObject

private data class MovieLink(
    val label: String,
    val streamUrl: String,
    val viewUrl: String,
    val downloadUrl: String,
    val telegramUrl: String,
    val telegramStartUrl: String,
    val telegramDeepLink: String,
    val watchTogetherUrl: String,
)

private data class SeasonLink(
    val season: Int,
    val episodeCount: Int,
    val qualities: List<String>,
    val previewUrl: String,
    val previewStreamUrl: String,
    val previewTelegramStartUrl: String,
)

class ContentDetailActivity : AppCompatActivity() {
    private val client = createApiHttpClient()

    private lateinit var tvTopbar: TextView
    private lateinit var tvHeaderTitle: TextView
    private lateinit var imgHeaderLogo: ImageView
    private lateinit var tvAnnouncement: TextView
    private lateinit var tvFooterText: TextView
    private lateinit var btnBack: Button
    private lateinit var btnHome: Button
    private lateinit var btnDownloads: Button

    private lateinit var progressBar: ProgressBar
    private lateinit var poster: ImageView
    private lateinit var title: TextView
    private lateinit var meta: TextView
    private lateinit var description: TextView
    private lateinit var trailerButton: Button
    private lateinit var movieLinksContainer: LinearLayout
    private lateinit var seasonLinksContainer: LinearLayout
    private lateinit var statusText: TextView

    private var contentTitle: String = ""

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_content_detail)

        bindViews()
        bindActions()
        applyRuntimeUi()

        val contentKey = intent?.getStringExtra("content_key").orEmpty().trim()
        if (contentKey.isBlank()) {
            finish()
            return
        }

        loadContent(contentKey)
    }

    private fun bindViews() {
        tvTopbar = findViewById(R.id.tvTopbar)
        tvHeaderTitle = findViewById(R.id.tvHeaderTitle)
        imgHeaderLogo = findViewById(R.id.imgHeaderLogo)
        tvAnnouncement = findViewById(R.id.tvAnnouncement)
        tvFooterText = findViewById(R.id.tvFooterText)
        btnBack = findViewById(R.id.btnBack)
        btnHome = findViewById(R.id.btnHome)
        btnDownloads = findViewById(R.id.btnDownloads)

        progressBar = findViewById(R.id.progressBar)
        poster = findViewById(R.id.imgPoster)
        title = findViewById(R.id.tvTitle)
        meta = findViewById(R.id.tvMeta)
        description = findViewById(R.id.tvDescription)
        trailerButton = findViewById(R.id.btnTrailer)
        movieLinksContainer = findViewById(R.id.movieLinksContainer)
        seasonLinksContainer = findViewById(R.id.seasonLinksContainer)
        statusText = findViewById(R.id.tvStatus)
    }

    private fun bindActions() {
        btnBack.setOnClickListener { finish() }
        btnHome.setOnClickListener {
            startActivity(Intent(this, MainActivity::class.java).apply {
                addFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP)
            })
        }
        btnDownloads.setOnClickListener {
            startActivity(Intent(this, DownloadsActivity::class.java))
        }
    }

    private fun applyRuntimeUi() {
        val ui = AppRuntimeState.ui
        tvTopbar.text = ui.topbarText.ifBlank { "Welcome to Mystic Movies" }
        tvHeaderTitle.text = ui.siteName.ifBlank { "mysticmovies" }
        tvFooterText.text = ui.footerText.ifBlank { "MysticMovies" }

        if (ui.logoUrl.isNotBlank()) {
            imgHeaderLogo.visibility = View.VISIBLE
            imgHeaderLogo.load(ui.logoUrl) {
                crossfade(true)
                error(android.R.drawable.sym_def_app_icon)
                placeholder(android.R.drawable.sym_def_app_icon)
            }
        } else {
            imgHeaderLogo.visibility = View.GONE
        }

        if (AppRuntimeState.notifications.isNotEmpty()) {
            tvAnnouncement.visibility = View.VISIBLE
            tvAnnouncement.text = AppRuntimeState.notifications.first()
        } else if (AppRuntimeState.adsMessage.isNotBlank()) {
            tvAnnouncement.visibility = View.VISIBLE
            tvAnnouncement.text = AppRuntimeState.adsMessage
        } else {
            tvAnnouncement.visibility = View.GONE
        }
    }

    private fun loadContent(contentKey: String) {
        progressBar.visibility = View.VISIBLE
        statusText.visibility = View.GONE

        lifecycleScope.launch {
            val payload = withContext(Dispatchers.IO) { fetchContent(contentKey) }
            progressBar.visibility = View.GONE

            if (payload == null) {
                statusText.visibility = View.VISIBLE
                statusText.text = "Unable to load content details."
                return@launch
            }

            bindContent(payload)
        }
    }

    private fun fetchContent(contentKey: String): JSONObject? {
        for (base in apiBaseCandidates()) {
            try {
                val url = "${base.trimEnd('/')}/app-api/content/$contentKey".toHttpUrlOrNull() ?: continue
                val req = Request.Builder().url(url).get().build()
                client.newCall(req).execute().use { res ->
                    if (!res.isSuccessful) return@use
                    val root = JSONObject(res.body?.string().orEmpty())
                    if (!root.optBoolean("ok")) return@use
                    AppRuntimeState.apiBaseUrl = base.trimEnd('/')
                    return root
                }
            } catch (_: Exception) {
                // Try next base URL.
            }
        }
        return null
    }

    private fun bindContent(root: JSONObject) {
        val item = root.optJSONObject("item") ?: JSONObject()
        val movieLinks = parseMovieLinks(root.optJSONArray("movie_links"))
        val seasonLinks = parseSeasonLinks(root.optJSONArray("series_links"))

        val itemTitle = item.optString("title").ifBlank { "Untitled" }
        contentTitle = itemTitle
        val itemYear = item.optString("year")
        val itemType = if (item.optString("type").equals("series", ignoreCase = true)) "WEB SERIES" else "MOVIE"
        title.text = itemTitle
        meta.text = if (itemYear.isNotBlank()) "$itemYear | $itemType" else itemType
        description.text = item.optString("description").ifBlank { "Description not available." }

        val imageUrl = item.optString("poster").ifBlank { item.optString("backdrop") }
        poster.load(imageUrl) {
            crossfade(true)
            placeholder(android.R.drawable.ic_menu_report_image)
            error(android.R.drawable.ic_menu_report_image)
        }

        val trailerUrl = item.optString("trailer_url")
        val trailerKey = item.optString("trailer_key")
        val trailerTarget = when {
            trailerUrl.isNotBlank() -> trailerUrl
            trailerKey.isNotBlank() -> "https://www.youtube.com/watch?v=$trailerKey"
            else -> ""
        }
        trailerButton.visibility = if (trailerTarget.isNotBlank()) View.VISIBLE else View.GONE
        trailerButton.setOnClickListener {
            if (trailerTarget.isBlank()) return@setOnClickListener
            openInAppWeb(trailerTarget, "Trailer")
        }

        bindMovieLinks(movieLinks)
        bindSeasonLinks(seasonLinks)
    }

    private fun bindMovieLinks(rows: List<MovieLink>) {
        movieLinksContainer.removeAllViews()
        if (rows.isEmpty()) {
            val noData = TextView(this).apply {
                text = "No direct movie links available."
                setTextColor(0xFF9CA3AF.toInt())
            }
            movieLinksContainer.addView(noData)
            return
        }

        rows.forEach { row ->
            val view = LayoutInflater.from(this).inflate(R.layout.item_link_row, movieLinksContainer, false)
            val label = view.findViewById<TextView>(R.id.tvLinkLabel)
            val watchButton = view.findViewById<Button>(R.id.btnWatch)
            val downloadButton = view.findViewById<Button>(R.id.btnDownload)
            val telegramButton = view.findViewById<Button>(R.id.btnTelegram)
            val watchTogetherButton = view.findViewById<Button>(R.id.btnWatchTogether)

            label.text = row.label.ifBlank { "Quality" }
            watchButton.setOnClickListener {
                val stream = row.streamUrl.ifBlank { row.viewUrl }
                openPlayer(stream, "${contentTitle} ${row.label}".trim())
            }
            downloadButton.setOnClickListener {
                enqueueDownload(row.downloadUrl, contentTitle, row.label)
            }
            telegramButton.setOnClickListener {
                openTelegram(row)
            }
            watchTogetherButton.setOnClickListener {
                openInAppWeb(row.watchTogetherUrl, "Watch Together")
            }

            movieLinksContainer.addView(view)
        }
    }

    private fun bindSeasonLinks(rows: List<SeasonLink>) {
        seasonLinksContainer.removeAllViews()
        if (rows.isEmpty()) {
            val noData = TextView(this).apply {
                text = "No season links available."
                setTextColor(0xFF9CA3AF.toInt())
            }
            seasonLinksContainer.addView(noData)
            return
        }

        rows.forEach { row ->
            val view = LayoutInflater.from(this).inflate(R.layout.item_season_row, seasonLinksContainer, false)
            val info = view.findViewById<TextView>(R.id.tvSeasonInfo)
            val openButton = view.findViewById<Button>(R.id.btnOpenSeason)

            val qualityText = if (row.qualities.isNotEmpty()) row.qualities.joinToString(" - ") else "HD"
            info.text = "Season ${row.season} - ${row.episodeCount} Episodes - $qualityText"
            openButton.isEnabled = row.previewUrl.isNotBlank()
            openButton.setOnClickListener {
                val stream = row.previewStreamUrl.ifBlank { row.previewUrl }
                openPlayer(stream, "$contentTitle S${row.season}")
            }

            seasonLinksContainer.addView(view)
        }
    }

    private fun parseMovieLinks(array: JSONArray?): List<MovieLink> {
        if (array == null) return emptyList()
        val rows = mutableListOf<MovieLink>()
        for (i in 0 until array.length()) {
            val row = array.optJSONObject(i) ?: continue
            rows.add(
                MovieLink(
                    label = row.optString("label"),
                    streamUrl = row.optString("stream_url"),
                    viewUrl = row.optString("view_url"),
                    downloadUrl = row.optString("download_url"),
                    telegramUrl = row.optString("telegram_url"),
                    telegramStartUrl = row.optString("telegram_start_url"),
                    telegramDeepLink = row.optString("telegram_deep_link"),
                    watchTogetherUrl = row.optString("watch_together_url"),
                )
            )
        }
        return rows
    }

    private fun parseSeasonLinks(array: JSONArray?): List<SeasonLink> {
        if (array == null) return emptyList()
        val rows = mutableListOf<SeasonLink>()
        for (i in 0 until array.length()) {
            val row = array.optJSONObject(i) ?: continue
            rows.add(
                SeasonLink(
                    season = row.optInt("season"),
                    episodeCount = row.optInt("episode_count"),
                    qualities = readStringArray(row.optJSONArray("qualities")),
                    previewUrl = row.optString("preview_view_url"),
                    previewStreamUrl = row.optString("preview_stream_url"),
                    previewTelegramStartUrl = row.optString("preview_telegram_start_url"),
                )
            )
        }
        return rows
    }

    private fun readStringArray(array: JSONArray?): List<String> {
        if (array == null) return emptyList()
        val rows = mutableListOf<String>()
        for (i in 0 until array.length()) {
            val value = array.optString(i).trim()
            if (value.isNotBlank()) rows.add(value)
        }
        return rows
    }

    private fun openPlayer(rawUrl: String, label: String) {
        val absolute = absoluteUrl(rawUrl)
        if (absolute.isBlank()) {
            Toast.makeText(this, "Play link is not available.", Toast.LENGTH_SHORT).show()
            return
        }
        val intent = Intent(this, PlayerActivity::class.java).apply {
            putExtra(PlayerActivity.EXTRA_STREAM_URL, absolute)
            putExtra(PlayerActivity.EXTRA_TITLE, label)
        }
        startActivity(intent)
    }

    private fun enqueueDownload(rawUrl: String, title: String, quality: String) {
        val url = absoluteUrl(rawUrl)
        if (url.isBlank()) {
            Toast.makeText(this, "Download link is not available.", Toast.LENGTH_SHORT).show()
            return
        }
        try {
            val cleanTitle = title.ifBlank { "mystic_content" }
                .replace(Regex("[^a-zA-Z0-9._-]+"), "_")
                .take(60)
            val cleanQuality = quality.ifBlank { "HD" }
                .replace(Regex("[^a-zA-Z0-9._-]+"), "_")
                .take(12)
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

    private fun openTelegram(link: MovieLink) {
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
                Toast.makeText(this@ContentDetailActivity, "Unable to open Telegram.", Toast.LENGTH_SHORT).show()
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
        val intent = Intent(this, LoginActivity::class.java).apply {
            putExtra("target_url", target)
            putExtra("title_text", titleText)
        }
        startActivity(intent)
    }
}
