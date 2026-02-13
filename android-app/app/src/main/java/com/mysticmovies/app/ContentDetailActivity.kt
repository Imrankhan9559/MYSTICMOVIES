package com.mysticmovies.app

import android.app.DownloadManager
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Bundle
import android.os.Environment
import android.text.SpannableString
import android.text.Spanned
import android.text.style.ForegroundColorSpan
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

private data class EpisodeRow(
    val episode: Int,
    val title: String,
    val qualities: List<MovieLink>,
)

private data class SeasonLink(
    val season: Int,
    val episodeCount: Int,
    val qualities: List<String>,
    val previewUrl: String,
    val previewStreamUrl: String,
    val previewTelegramStartUrl: String,
    val episodes: List<EpisodeRow>,
)

private data class CastEntry(
    val name: String,
    val role: String,
    val image: String,
    val castPath: String,
)

class ContentDetailActivity : AppCompatActivity() {
    private val client = createApiHttpClient()

    private lateinit var tvTopbar: TextView
    private lateinit var tvHeaderTitle: TextView
    private lateinit var imgHeaderLogo: ImageView
    private lateinit var tvAnnouncement: TextView
    private lateinit var tvFooterText: TextView
    private lateinit var btnBack: Button
    private lateinit var btnHeaderRequest: Button
    private lateinit var btnNavSearch: Button
    private lateinit var btnNavHome: Button
    private lateinit var btnNavDownloads: Button
    private lateinit var btnNavProfile: Button

    private lateinit var progressBar: ProgressBar
    private lateinit var poster: ImageView
    private lateinit var title: TextView
    private lateinit var meta: TextView
    private lateinit var description: TextView
    private lateinit var trailerButton: Button
    private lateinit var shareButton: Button
    private lateinit var castContainer: LinearLayout
    private lateinit var tvMovieLinksTitle: TextView
    private lateinit var movieLinksContainer: LinearLayout
    private lateinit var tvSeasonLinksTitle: TextView
    private lateinit var seasonLinksContainer: LinearLayout
    private lateinit var statusText: TextView

    private var contentTitle: String = ""
    private var detailPath: String = ""
    private var detailUrl: String = ""
    private var fallbackSlug: String = ""
    private var targetContentKey: String = ""
    private var isUserLoggedIn = false
    private var loginRequested = false
    private var openProfileAfterLogin = false
    private var contentLoaded = false

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_content_detail)
        bindViews()
        bindActions()
        applyRuntimeUi()

        targetContentKey = intent?.getStringExtra("content_key").orEmpty().trim()
        if (targetContentKey.isBlank()) {
            finish()
            return
        }
        refreshSessionAndLoad(autoOpenLogin = false)
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
                    startActivity(Intent(this@ContentDetailActivity, ProfileActivity::class.java))
                    return@launch
                }
                if (contentLoaded) {
                    loadContent(targetContentKey)
                } else {
                    refreshSessionAndLoad(autoOpenLogin = false)
                }
            }
        }
    }

    private fun bindViews() {
        tvTopbar = findViewById(R.id.tvTopbar)
        tvHeaderTitle = findViewById(R.id.tvHeaderTitle)
        imgHeaderLogo = findViewById(R.id.imgHeaderLogo)
        tvAnnouncement = findViewById(R.id.tvAnnouncement)
        tvFooterText = findViewById(R.id.tvFooterText)
        btnBack = findViewById(R.id.btnBack)
        btnHeaderRequest = findViewById(R.id.btnHeaderRequest)
        btnNavSearch = findViewById(R.id.btnNavSearch)
        btnNavHome = findViewById(R.id.btnNavHome)
        btnNavDownloads = findViewById(R.id.btnNavDownloads)
        btnNavProfile = findViewById(R.id.btnNavProfile)

        progressBar = findViewById(R.id.progressBar)
        poster = findViewById(R.id.imgPoster)
        title = findViewById(R.id.tvTitle)
        meta = findViewById(R.id.tvMeta)
        description = findViewById(R.id.tvDescription)
        trailerButton = findViewById(R.id.btnTrailer)
        shareButton = findViewById(R.id.btnShare)
        castContainer = findViewById(R.id.castContainer)
        tvMovieLinksTitle = findViewById(R.id.tvMovieLinksTitle)
        movieLinksContainer = findViewById(R.id.movieLinksContainer)
        tvSeasonLinksTitle = findViewById(R.id.tvSeasonLinksTitle)
        seasonLinksContainer = findViewById(R.id.seasonLinksContainer)
        statusText = findViewById(R.id.tvStatus)
    }

    private fun bindActions() {
        btnBack.setOnClickListener { finish() }
        btnHeaderRequest.setOnClickListener {
            ensureLoggedInThen { openRequestContent() }
        }
        btnNavSearch.setOnClickListener {
            ensureLoggedInThen {
                startActivity(Intent(this, SearchActivity::class.java))
            }
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
                    startActivity(Intent(this@ContentDetailActivity, ProfileActivity::class.java))
                } else {
                    openProfileAfterLogin = true
                    openLogin()
                }
            }
        }
        shareButton.setOnClickListener { shareContentLink() }
    }

    private fun applyRuntimeUi() {
        val ui = AppRuntimeState.ui
        tvTopbar.text = ui.topbarText.ifBlank { "Welcome to Mystic Movies" }
        setBrandTitle()
        tvFooterText.text = ui.footerText.ifBlank { "MysticMovies" }

        if (ui.logoUrl.isNotBlank()) {
            imgHeaderLogo.visibility = View.VISIBLE
            imgHeaderLogo.load(resolveImageUrl(ui.logoUrl)) {
                crossfade(true)
                error(android.R.drawable.sym_def_app_icon)
                placeholder(android.R.drawable.sym_def_app_icon)
            }
        } else {
            imgHeaderLogo.visibility = View.VISIBLE
            imgHeaderLogo.setImageResource(R.mipmap.ic_launcher)
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

    private fun setBrandTitle() {
        val brand = "MysticMovies"
        val span = SpannableString(brand)
        span.setSpan(ForegroundColorSpan(0xFFFFFFFF.toInt()), 0, 6, Spanned.SPAN_EXCLUSIVE_EXCLUSIVE)
        span.setSpan(ForegroundColorSpan(0xFFFACC15.toInt()), 6, brand.length, Spanned.SPAN_EXCLUSIVE_EXCLUSIVE)
        tvHeaderTitle.text = span
    }

    private fun refreshSessionAndLoad(autoOpenLogin: Boolean) {
        lifecycleScope.launch {
            val loggedIn = withContext(Dispatchers.IO) { fetchSessionLoggedIn() }
            isUserLoggedIn = loggedIn
            if (!loggedIn && autoOpenLogin) {
                openLogin()
                return@launch
            }
            loadContent(targetContentKey)
        }
    }

    private fun ensureLoggedInThen(action: () -> Unit) {
        lifecycleScope.launch {
            val loggedIn = withContext(Dispatchers.IO) { fetchSessionLoggedIn() }
            isUserLoggedIn = loggedIn
            if (loggedIn) {
                action()
            } else {
                openLogin()
            }
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
            contentLoaded = true
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
        val castRows = parseCastRows(item.optJSONArray("cast_profiles"))

        val itemTitle = item.optString("title").ifBlank { "Untitled" }
        contentTitle = itemTitle
        fallbackSlug = item.optString("slug")
        detailPath = root.optString("detail_path")
        detailUrl = root.optString("detail_url")

        val itemYear = item.optString("year")
        val isSeries = item.optString("type").equals("series", ignoreCase = true)
        val itemType = if (isSeries) "WEB SERIES" else "MOVIE"
        title.text = itemTitle
        meta.text = if (itemYear.isNotBlank()) "$itemYear | $itemType" else itemType
        description.text = item.optString("description").ifBlank { "Description not available." }

        val primaryImage = item.optString("backdrop")
            .ifBlank { item.optString("poster") }
            .ifBlank { item.optString("backdrop_original") }
            .ifBlank { item.optString("poster_original") }
        val fallbackImage = item.optString("poster")
            .ifBlank { item.optString("poster_original") }
            .ifBlank { item.optString("backdrop") }
        loadPoster(primaryImage, fallbackImage)

        val trailerEmbed = item.optString("trailer_embed_url")
        val trailerUrl = item.optString("trailer_url")
        val trailerTarget = when {
            trailerEmbed.isNotBlank() -> trailerEmbed
            trailerUrl.isNotBlank() -> trailerUrl
            item.optString("trailer_key").isNotBlank() -> "https://www.youtube.com/embed/${item.optString("trailer_key")}?autoplay=1&playsinline=1&rel=0"
            else -> ""
        }
        trailerButton.visibility = if (trailerTarget.isNotBlank()) View.VISIBLE else View.GONE
        trailerButton.setOnClickListener {
            if (trailerTarget.isNotBlank()) {
                openTrailer(trailerTarget)
            }
        }

        bindCast(castRows)

        if (isSeries) {
            tvMovieLinksTitle.visibility = View.GONE
            movieLinksContainer.visibility = View.GONE
            tvSeasonLinksTitle.visibility = View.VISIBLE
            seasonLinksContainer.visibility = View.VISIBLE
            bindSeasonLinks(seasonLinks)
        } else {
            tvSeasonLinksTitle.visibility = View.GONE
            seasonLinksContainer.visibility = View.GONE
            tvMovieLinksTitle.visibility = View.VISIBLE
            movieLinksContainer.visibility = View.VISIBLE
            if (isUserLoggedIn) {
                bindMovieLinks(movieLinks)
            } else {
                showLoginAccessCard(
                    movieLinksContainer,
                    "Login to access Watch, Download, Watch Together and Telegram options."
                )
            }
        }
    }

    private fun loadPoster(primary: String, fallback: String) {
        val primaryResolved = resolveImageUrl(primary)
        val fallbackResolved = resolveImageUrl(fallback)
        poster.load(primaryResolved) {
            crossfade(true)
            placeholder(android.R.drawable.ic_menu_gallery)
            error(android.R.drawable.ic_menu_report_image)
            listener(onError = { _, _ ->
                if (fallbackResolved.isNotBlank() && fallbackResolved != primaryResolved) {
                    poster.load(fallbackResolved) {
                        crossfade(true)
                        placeholder(android.R.drawable.ic_menu_gallery)
                        error(android.R.drawable.ic_menu_report_image)
                    }
                }
            })
        }
    }

    private fun parseCastRows(array: JSONArray?): List<CastEntry> {
        if (array == null) return emptyList()
        val rows = mutableListOf<CastEntry>()
        for (i in 0 until array.length()) {
            val row = array.optJSONObject(i) ?: continue
            rows.add(
                CastEntry(
                    name = row.optString("name"),
                    role = row.optString("role"),
                    image = row.optString("image"),
                    castPath = row.optString("cast_path"),
                )
            )
        }
        return rows
    }

    private fun bindCast(rows: List<CastEntry>) {
        castContainer.removeAllViews()
        if (rows.isEmpty()) {
            val noData = TextView(this).apply {
                text = "Cast details not available."
                setTextColor(0xFF9CA3AF.toInt())
                textSize = 12f
            }
            castContainer.addView(noData)
            return
        }
        rows.take(12).forEach { cast ->
            val view = layoutInflater.inflate(R.layout.item_cast_card, castContainer, false)
            val img = view.findViewById<ImageView>(R.id.imgCast)
            val name = view.findViewById<TextView>(R.id.tvCastName)
            val role = view.findViewById<TextView>(R.id.tvCastRole)
            img.load(resolveImageUrl(cast.image)) {
                crossfade(true)
                placeholder(android.R.drawable.sym_def_app_icon)
                error(android.R.drawable.sym_def_app_icon)
            }
            name.text = cast.name.ifBlank { "Unknown" }
            role.text = cast.role.ifBlank { "View profile" }
            view.setOnClickListener {
                if (cast.castPath.isNotBlank()) {
                    openInAppWeb(cast.castPath, cast.name)
                }
            }
            castContainer.addView(view)
        }
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
            openButton.isEnabled = row.episodeCount > 0 || row.previewUrl.isNotBlank()
            openButton.setOnClickListener {
                if (row.episodes.isNotEmpty()) {
                    openSeasonEpisodes(row)
                } else {
                    val stream = row.previewStreamUrl.ifBlank { row.previewUrl }
                    openPlayer(stream, "$contentTitle S${row.season}")
                }
            }
            seasonLinksContainer.addView(view)
        }
    }

    private fun showLoginAccessCard(container: LinearLayout, message: String) {
        container.removeAllViews()
        val view = LayoutInflater.from(this).inflate(R.layout.item_login_access_row, container, false)
        view.findViewById<TextView>(R.id.tvAccessMessage).text = message
        view.findViewById<Button>(R.id.btnLoginAccess).setOnClickListener {
            openLogin()
        }
        container.addView(view)
    }

    private fun parseMovieLinks(array: JSONArray?): List<MovieLink> {
        if (array == null) return emptyList()
        val rows = mutableListOf<MovieLink>()
        for (i in 0 until array.length()) {
            val row = array.optJSONObject(i) ?: continue
            rows.add(parseMovieLinkRow(row))
        }
        return rows
    }

    private fun parseMovieLinkRow(row: JSONObject): MovieLink {
        return MovieLink(
            label = row.optString("label"),
            streamUrl = row.optString("stream_url"),
            viewUrl = row.optString("view_url"),
            downloadUrl = row.optString("download_url"),
            telegramUrl = row.optString("telegram_url"),
            telegramStartUrl = row.optString("telegram_start_url"),
            telegramDeepLink = row.optString("telegram_deep_link"),
            watchTogetherUrl = row.optString("watch_together_url"),
        )
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
                    episodes = parseEpisodeRows(row.optJSONArray("episodes")),
                )
            )
        }
        return rows
    }

    private fun parseEpisodeRows(array: JSONArray?): List<EpisodeRow> {
        if (array == null) return emptyList()
        val rows = mutableListOf<EpisodeRow>()
        for (i in 0 until array.length()) {
            val row = array.optJSONObject(i) ?: continue
            val qualities = mutableListOf<MovieLink>()
            val qualityRows = row.optJSONArray("qualities")
            if (qualityRows != null) {
                for (q in 0 until qualityRows.length()) {
                    val qRow = qualityRows.optJSONObject(q) ?: continue
                    qualities.add(parseMovieLinkRow(qRow))
                }
            }
            rows.add(
                EpisodeRow(
                    episode = row.optInt("episode"),
                    title = row.optString("title"),
                    qualities = qualities,
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

    private fun openSeasonEpisodes(season: SeasonLink) {
        val intent = Intent(this, SeasonEpisodesActivity::class.java).apply {
            putExtra(SeasonEpisodesActivity.EXTRA_CONTENT_TITLE, contentTitle)
            putExtra(SeasonEpisodesActivity.EXTRA_SEASON_NUMBER, season.season)
            putExtra(SeasonEpisodesActivity.EXTRA_IS_LOGGED_IN, isUserLoggedIn)
            putExtra(SeasonEpisodesActivity.EXTRA_EPISODES_JSON, episodeRowsToJson(season.episodes).toString())
        }
        startActivity(intent)
    }

    private fun episodeRowsToJson(rows: List<EpisodeRow>): JSONArray {
        val array = JSONArray()
        rows.forEach { episode ->
            val row = JSONObject()
            row.put("episode", episode.episode)
            row.put("title", episode.title)
            val qualityArray = JSONArray()
            episode.qualities.forEach { link ->
                qualityArray.put(JSONObject().apply {
                    put("label", link.label)
                    put("stream_url", link.streamUrl)
                    put("view_url", link.viewUrl)
                    put("download_url", link.downloadUrl)
                    put("telegram_url", link.telegramUrl)
                    put("telegram_start_url", link.telegramStartUrl)
                    put("telegram_deep_link", link.telegramDeepLink)
                    put("watch_together_url", link.watchTogetherUrl)
                })
            }
            row.put("qualities", qualityArray)
            array.put(row)
        }
        return array
    }

    private fun openTrailer(target: String) {
        val url = absoluteUrl(target)
        if (url.isBlank()) {
            Toast.makeText(this, "Trailer is not available.", Toast.LENGTH_SHORT).show()
            return
        }
        if (url.contains("youtube.com/embed/") || url.contains("youtu.be") || url.contains("youtube.com/watch")) {
            startActivity(Intent(this, TrailerActivity::class.java).apply {
                putExtra(TrailerActivity.EXTRA_URL, url)
                putExtra(TrailerActivity.EXTRA_TITLE, "$contentTitle Trailer")
            })
            return
        }
        openPlayer(url, "$contentTitle Trailer")
    }

    private fun shareContentLink() {
        val target = when {
            detailUrl.isNotBlank() -> detailUrl
            detailPath.isNotBlank() -> absoluteUrl(detailPath)
            fallbackSlug.isNotBlank() -> absoluteUrl("/content/details/$fallbackSlug")
            else -> ""
        }
        if (target.isBlank()) {
            Toast.makeText(this, "Share link is not available.", Toast.LENGTH_SHORT).show()
            return
        }
        val payload = "${contentTitle.ifBlank { "MysticMovies" }}\n$target"
        val intent = Intent(Intent.ACTION_SEND).apply {
            type = "text/plain"
            putExtra(Intent.EXTRA_SUBJECT, contentTitle.ifBlank { "MysticMovies" })
            putExtra(Intent.EXTRA_TEXT, payload)
        }
        startActivity(Intent.createChooser(intent, "Share content"))
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

    private fun openLogin() {
        loginRequested = true
        startActivity(Intent(this, LoginActivity::class.java).apply {
            putExtra("target_url", absoluteUrl("/login"))
            putExtra("title_text", "Login")
        })
    }

    private fun openRequestContent() {
        val returnTo = detailPath.ifBlank {
            if (fallbackSlug.isNotBlank()) "/content/details/$fallbackSlug" else "/content"
        }
        val target = "/request-content?return_to=${Uri.encode(returnTo)}&prefill_title=${Uri.encode(contentTitle)}"
        openInAppWeb(target, "Request Content")
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
