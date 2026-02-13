package com.mysticmovies.app

import android.content.Intent
import android.net.Uri
import android.os.Bundle
import android.view.LayoutInflater
import android.view.View
import android.widget.Button
import android.widget.ImageView
import android.widget.LinearLayout
import android.widget.ProgressBar
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import androidx.lifecycle.lifecycleScope
import coil.load
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import okhttp3.OkHttpClient
import okhttp3.Request
import org.json.JSONArray
import org.json.JSONObject
import java.util.concurrent.TimeUnit

private data class MovieLink(
    val label: String,
    val viewUrl: String,
    val downloadUrl: String,
    val telegramUrl: String,
)

private data class SeasonLink(
    val season: Int,
    val episodeCount: Int,
    val qualities: List<String>,
    val previewUrl: String,
)

class ContentDetailActivity : AppCompatActivity() {
    private val client = OkHttpClient.Builder()
        .connectTimeout(20, TimeUnit.SECONDS)
        .readTimeout(30, TimeUnit.SECONDS)
        .writeTimeout(20, TimeUnit.SECONDS)
        .build()

    private lateinit var progressBar: ProgressBar
    private lateinit var poster: ImageView
    private lateinit var title: TextView
    private lateinit var meta: TextView
    private lateinit var description: TextView
    private lateinit var trailerButton: Button
    private lateinit var movieLinksContainer: LinearLayout
    private lateinit var seasonLinksContainer: LinearLayout
    private lateinit var statusText: TextView

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_content_detail)

        progressBar = findViewById(R.id.progressBar)
        poster = findViewById(R.id.imgPoster)
        title = findViewById(R.id.tvTitle)
        meta = findViewById(R.id.tvMeta)
        description = findViewById(R.id.tvDescription)
        trailerButton = findViewById(R.id.btnTrailer)
        movieLinksContainer = findViewById(R.id.movieLinksContainer)
        seasonLinksContainer = findViewById(R.id.seasonLinksContainer)
        statusText = findViewById(R.id.tvStatus)

        val contentKey = intent?.getStringExtra("content_key").orEmpty().trim()
        if (contentKey.isBlank()) {
            finish()
            return
        }

        loadContent(contentKey)
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
        return try {
            val base = "${BuildConfig.API_BASE_URL.trimEnd('/')}/app-api/content/$contentKey".toHttpUrlOrNull()
                ?: return null
            val req = Request.Builder().url(base).get().build()
            client.newCall(req).execute().use { res ->
                if (!res.isSuccessful) return null
                val root = JSONObject(res.body?.string().orEmpty())
                if (!root.optBoolean("ok")) return null
                root
            }
        } catch (_: Exception) {
            null
        }
    }

    private fun bindContent(root: JSONObject) {
        val item = root.optJSONObject("item") ?: JSONObject()
        val movieLinks = parseMovieLinks(root.optJSONArray("movie_links"))
        val seasonLinks = parseSeasonLinks(root.optJSONArray("series_links"))

        val itemTitle = item.optString("title").ifBlank { "Untitled" }
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
        trailerButton.visibility = if (trailerUrl.isNotBlank()) View.VISIBLE else View.GONE
        trailerButton.setOnClickListener {
            openExternalUrl(trailerUrl)
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

            label.text = row.label.ifBlank { "Quality" }
            watchButton.setOnClickListener { openRelativeUrl(row.viewUrl) }
            downloadButton.setOnClickListener { openRelativeUrl(row.downloadUrl) }
            telegramButton.setOnClickListener { openRelativeUrl(row.telegramUrl) }

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

            val qualityText = if (row.qualities.isNotEmpty()) row.qualities.joinToString(" • ") else "HD"
            info.text = "Season ${row.season} • ${row.episodeCount} Episodes • $qualityText"
            openButton.isEnabled = row.previewUrl.isNotBlank()
            openButton.setOnClickListener { openRelativeUrl(row.previewUrl) }

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
                    viewUrl = row.optString("view_url"),
                    downloadUrl = row.optString("download_url"),
                    telegramUrl = row.optString("telegram_url"),
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

    private fun openRelativeUrl(rawUrl: String) {
        val cleaned = rawUrl.trim()
        if (cleaned.isBlank()) return
        val absolute = if (cleaned.startsWith("http://") || cleaned.startsWith("https://")) {
            cleaned
        } else {
            val path = if (cleaned.startsWith("/")) cleaned else "/$cleaned"
            "${BuildConfig.API_BASE_URL.trimEnd('/')}$path"
        }
        openExternalUrl(absolute)
    }

    private fun openExternalUrl(url: String) {
        if (url.isBlank()) return
        try {
            startActivity(Intent(Intent.ACTION_VIEW, Uri.parse(url)))
        } catch (_: Exception) {
            // Ignore when no app can handle URL.
        }
    }
}
