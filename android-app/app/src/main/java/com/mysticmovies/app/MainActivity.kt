package com.mysticmovies.app

import android.content.Intent
import android.graphics.Color
import android.os.Bundle
import android.provider.Settings
import android.view.View
import android.view.inputmethod.EditorInfo
import android.widget.EditText
import android.widget.ImageView
import android.widget.ProgressBar
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import androidx.lifecycle.lifecycleScope
import androidx.recyclerview.widget.GridLayoutManager
import androidx.recyclerview.widget.RecyclerView
import androidx.swiperefreshlayout.widget.SwipeRefreshLayout
import coil.load
import com.google.android.material.button.MaterialButton
import com.google.android.material.dialog.MaterialAlertDialogBuilder
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONArray
import org.json.JSONObject
import java.net.SocketTimeoutException

private data class CatalogResponse(
    val cards: List<CatalogCard>,
    val hero: HeroSlide?
)

private data class CatalogFetchResult(
    val response: CatalogResponse? = null,
    val error: String = ""
)

private data class HeroSlide(
    val title: String,
    val subtitle: String,
    val image: String,
    val contentKey: String
)

class MainActivity : AppCompatActivity() {
    private val client = createApiHttpClient()

    private lateinit var tvTopbar: TextView
    private lateinit var tvHeaderTitle: TextView
    private lateinit var imgHeaderLogo: ImageView
    private lateinit var btnHeaderLogin: MaterialButton
    private lateinit var tvAnnouncement: TextView
    private lateinit var tvFooterText: TextView
    private lateinit var btnFooterHome: MaterialButton
    private lateinit var btnFooterDownloads: MaterialButton

    private lateinit var swipeRefreshLayout: SwipeRefreshLayout
    private lateinit var progressBar: ProgressBar
    private lateinit var emptyView: TextView
    private lateinit var heroImage: ImageView
    private lateinit var heroTitle: TextView
    private lateinit var heroSubtitle: TextView
    private lateinit var searchInput: EditText
    private lateinit var searchButton: MaterialButton
    private lateinit var filterAll: MaterialButton
    private lateinit var filterMovies: MaterialButton
    private lateinit var filterSeries: MaterialButton
    private lateinit var catalogRecycler: RecyclerView

    private lateinit var adapter: CatalogAdapter

    private var currentFilter = "all"
    private var currentQuery = ""
    private var loading = false
    private var updatePromptShown = false

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        bindViews()
        bindActions()
        applyRuntimeUi()
        applyFilterStyle()

        adapter = CatalogAdapter { card ->
            val key = card.slug.ifBlank { card.id }
            if (key.isNotBlank()) {
                openContentDetail(key)
            }
        }
        catalogRecycler.layoutManager = GridLayoutManager(this, 2)
        catalogRecycler.adapter = adapter

        loadBootstrap()
        loadCatalog()
    }

    private fun bindViews() {
        tvTopbar = findViewById(R.id.tvTopbar)
        tvHeaderTitle = findViewById(R.id.tvHeaderTitle)
        imgHeaderLogo = findViewById(R.id.imgHeaderLogo)
        btnHeaderLogin = findViewById(R.id.btnHeaderLogin)
        tvAnnouncement = findViewById(R.id.tvAnnouncement)
        tvFooterText = findViewById(R.id.tvFooterText)
        btnFooterHome = findViewById(R.id.btnFooterHome)
        btnFooterDownloads = findViewById(R.id.btnFooterDownloads)

        swipeRefreshLayout = findViewById(R.id.swipeRefreshLayout)
        progressBar = findViewById(R.id.progressBar)
        emptyView = findViewById(R.id.tvEmpty)
        heroImage = findViewById(R.id.imgHero)
        heroTitle = findViewById(R.id.tvHeroTitle)
        heroSubtitle = findViewById(R.id.tvHeroSubtitle)
        searchInput = findViewById(R.id.etSearch)
        searchButton = findViewById(R.id.btnSearch)
        filterAll = findViewById(R.id.btnFilterAll)
        filterMovies = findViewById(R.id.btnFilterMovies)
        filterSeries = findViewById(R.id.btnFilterSeries)
        catalogRecycler = findViewById(R.id.rvCatalog)
    }

    private fun bindActions() {
        swipeRefreshLayout.setOnRefreshListener {
            loadCatalog(showLoader = false)
        }

        searchButton.setOnClickListener {
            currentQuery = searchInput.text?.toString()?.trim().orEmpty()
            loadCatalog()
        }

        searchInput.setOnEditorActionListener { _, actionId, _ ->
            if (actionId == EditorInfo.IME_ACTION_SEARCH) {
                currentQuery = searchInput.text?.toString()?.trim().orEmpty()
                loadCatalog()
                true
            } else {
                false
            }
        }

        filterAll.setOnClickListener { updateFilter("all") }
        filterMovies.setOnClickListener { updateFilter("movies") }
        filterSeries.setOnClickListener { updateFilter("series") }

        btnFooterHome.setOnClickListener {
            catalogRecycler.smoothScrollToPosition(0)
        }
        btnFooterDownloads.setOnClickListener {
            startActivity(Intent(this, DownloadsActivity::class.java))
        }
        btnHeaderLogin.setOnClickListener {
            val intent = Intent(this, LoginActivity::class.java)
            startActivity(intent)
        }
    }

    private fun updateFilter(filter: String) {
        if (currentFilter == filter) return
        currentFilter = filter
        applyFilterStyle()
        loadCatalog()
    }

    private fun applyFilterStyle() {
        styleFilterButton(filterAll, currentFilter == "all")
        styleFilterButton(filterMovies, currentFilter == "movies")
        styleFilterButton(filterSeries, currentFilter == "series")
    }

    private fun styleFilterButton(button: MaterialButton, selected: Boolean) {
        if (selected) {
            button.setBackgroundColor(Color.parseColor("#FACC15"))
            button.setTextColor(Color.parseColor("#111111"))
        } else {
            button.setBackgroundColor(Color.parseColor("#171717"))
            button.setTextColor(Color.parseColor("#F9FAFB"))
        }
    }

    private fun applyRuntimeUi() {
        val ui = AppRuntimeState.ui
        tvTopbar.text = ui.topbarText.ifBlank { "Welcome to Mystic Movies" }
        tvHeaderTitle.text = ui.siteName.ifBlank { "mysticmovies" }
        tvFooterText.text = ui.footerText.ifBlank { "MysticMovies" }
        title = ui.siteName.ifBlank { "MysticMovies" }

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

        if (AppRuntimeState.maintenanceMode) {
            emptyView.visibility = View.VISIBLE
            emptyView.text = AppRuntimeState.maintenanceMessage.ifBlank { "App is under maintenance. Please try again later." }
        }
    }

    private fun loadBootstrap() {
        lifecycleScope.launch {
            val result = withContext(Dispatchers.IO) { performBootstrap() }
            if (result) {
                applyRuntimeUi()
                maybeShowUpdatePrompt()
                if (AppRuntimeState.keepaliveOnLaunch) {
                    withContext(Dispatchers.IO) { sendPing() }
                }
            }
        }
    }

    private fun performBootstrap(): Boolean {
        val androidId = Settings.Secure.getString(contentResolver, Settings.Secure.ANDROID_ID).orEmpty().ifBlank { "android-device" }
        val payload = JSONObject()
            .put("device_id", androidId)
            .put("platform", "android")
            .put("app_version", BuildConfig.VERSION_NAME)
            .put("build_number", BuildConfig.VERSION_CODE)
        val mediaType = "application/json; charset=utf-8".toMediaType()

        for (baseUrl in apiBaseCandidates()) {
            try {
                val hsReq = Request.Builder()
                    .url("$baseUrl/app-api/handshake")
                    .post(payload.toString().toRequestBody(mediaType))
                    .build()
                client.newCall(hsReq).execute().use { hsRes ->
                    if (!hsRes.isSuccessful) return@use
                    val hsBody = hsRes.body?.string().orEmpty()
                    val hsRoot = JSONObject(hsBody)
                    if (!hsRoot.optBoolean("ok")) return@use
                    val token = hsRoot.optString("handshake_token").trim()
                    if (token.isBlank()) return@use

                    val bsReq = Request.Builder()
                        .url("$baseUrl/app-api/bootstrap")
                        .header("X-App-Handshake", token)
                        .get()
                        .build()
                    client.newCall(bsReq).execute().use { bsRes ->
                        if (!bsRes.isSuccessful) return@use
                        val bsBody = bsRes.body?.string().orEmpty()
                        val bsRoot = JSONObject(bsBody)
                        if (!bsRoot.optBoolean("ok")) return@use

                        AppRuntimeState.handshakeToken = token
                        AppRuntimeState.applyBootstrap(baseUrl, bsRoot)
                    }
                }
                if (AppRuntimeState.handshakeToken.isNotBlank()) {
                    return true
                }
            } catch (_: Exception) {
                // Try next base URL candidate.
            }
        }
        return false
    }

    private fun sendPing() {
        val token = AppRuntimeState.handshakeToken.trim()
        if (token.isBlank()) return
        val baseUrl = AppRuntimeState.apiBaseUrl.trimEnd('/')
        val mediaType = "application/json; charset=utf-8".toMediaType()
        val req = Request.Builder()
            .url("$baseUrl/app-api/ping")
            .header("X-App-Handshake", token)
            .post("{}".toRequestBody(mediaType))
            .build()
        try {
            client.newCall(req).execute().use { _ -> }
        } catch (_: Exception) {
            // Ignore ping failure.
        }
    }

    private fun maybeShowUpdatePrompt() {
        if (updatePromptShown) return
        val update = AppRuntimeState.update
        val showPrompt = update.forceRequired || update.recommend
        if (!showPrompt) return
        updatePromptShown = true

        val message = buildString {
            append(update.body.ifBlank { "A new app version is available." })
            if (update.latestVersion.isNotBlank()) {
                append("\n\nLatest: ")
                append(update.latestVersion)
            }
            if (update.releaseNotes.isNotBlank()) {
                append("\n\n")
                append(update.releaseNotes)
            }
        }

        val builder = MaterialAlertDialogBuilder(this)
            .setTitle(update.title.ifBlank { "Update Available" })
            .setMessage(message)
            .setPositiveButton("Update") { _, _ ->
                val url = absoluteUrl(update.apkDownloadUrl)
                if (url.isNotBlank()) {
                    startActivity(Intent(this, LoginActivity::class.java).apply {
                        putExtra("target_url", url)
                        putExtra("title_text", "App Update")
                    })
                }
                if (update.forceRequired) {
                    finishAffinity()
                }
            }

        if (!update.forceRequired) {
            builder.setNegativeButton("Later", null)
        }

        val dialog = builder.create()
        dialog.setCancelable(!update.forceRequired)
        dialog.setCanceledOnTouchOutside(!update.forceRequired)
        dialog.show()
    }

    private fun loadCatalog(showLoader: Boolean = true) {
        if (loading || AppRuntimeState.maintenanceMode) return
        loading = true

        if (showLoader) {
            progressBar.visibility = View.VISIBLE
        } else {
            swipeRefreshLayout.isRefreshing = true
        }
        emptyView.visibility = View.GONE

        lifecycleScope.launch {
            val result = withContext(Dispatchers.IO) {
                fetchCatalog(currentFilter, currentQuery)
            }

            loading = false
            progressBar.visibility = View.GONE
            swipeRefreshLayout.isRefreshing = false

            if (result.response == null) {
                adapter.submitItems(emptyList())
                emptyView.visibility = View.VISIBLE
                emptyView.text = if (result.error.isNotBlank()) {
                    "Unable to load content.\n${result.error}"
                } else {
                    "Unable to load content. Please try again."
                }
                return@launch
            }

            adapter.submitItems(result.response.cards)
            bindHero(result.response.hero)

            if (result.response.cards.isEmpty()) {
                emptyView.visibility = View.VISIBLE
                emptyView.text = "No content found."
            } else {
                emptyView.visibility = View.GONE
            }
        }
    }

    private fun bindHero(hero: HeroSlide?) {
        if (hero == null || hero.image.isBlank()) {
            val splash = AppRuntimeState.splashImageUrl
            if (splash.isNotBlank()) {
                heroImage.load(splash) {
                    crossfade(true)
                    error(android.R.drawable.ic_menu_report_image)
                    placeholder(android.R.drawable.ic_menu_report_image)
                }
            } else {
                heroImage.setImageResource(android.R.drawable.ic_menu_report_image)
            }
            heroTitle.text = "MysticMovies"
            heroSubtitle.text = "Latest uploads"
            heroImage.setOnClickListener(null)
            return
        }
        heroImage.load(hero.image) {
            crossfade(true)
            placeholder(android.R.drawable.ic_menu_report_image)
            error(android.R.drawable.ic_menu_report_image)
        }
        heroTitle.text = hero.title.ifBlank { "MysticMovies" }
        heroSubtitle.text = hero.subtitle.ifBlank { "Latest uploads" }
        if (hero.contentKey.isNotBlank()) {
            heroImage.setOnClickListener { openContentDetail(hero.contentKey) }
        } else {
            heroImage.setOnClickListener(null)
        }
    }

    private fun fetchCatalog(filter: String, query: String): CatalogFetchResult {
        var lastError = ""
        for (baseUrl in apiBaseCandidates()) {
            for (attempt in 1..2) {
                val result = fetchCatalogFromBase(baseUrl, filter, query)
                if (result.response != null) {
                    AppRuntimeState.apiBaseUrl = baseUrl.trimEnd('/')
                    return result
                }
                lastError = result.error
                if (lastError.contains("404")) break
            }
        }
        return CatalogFetchResult(null, lastError.ifBlank { "Network error." })
    }

    private fun fetchCatalogFromBase(baseUrl: String, filter: String, query: String): CatalogFetchResult {
        return try {
            val httpUrl = "${baseUrl.trimEnd('/')}/app-api/catalog".toHttpUrlOrNull()
                ?: return CatalogFetchResult(error = "Invalid API URL.")
            val reqUrl = httpUrl.newBuilder()
                .addQueryParameter("filter", filter)
                .addQueryParameter("sort", "release_new")
                .addQueryParameter("page", "1")
                .addQueryParameter("per_page", "24")
                .apply {
                    if (query.isNotBlank()) addQueryParameter("q", query)
                }
                .build()

            val req = Request.Builder().url(reqUrl).get().build()
            client.newCall(req).execute().use { res ->
                val body = res.body?.string().orEmpty()
                if (!res.isSuccessful) {
                    val msg = when (res.code) {
                        404 -> "API not deployed yet (404)."
                        401 -> "Unauthorized from API (401)."
                        in 500..599 -> "Server error (${res.code})."
                        else -> "HTTP ${res.code}."
                    }
                    return CatalogFetchResult(error = msg)
                }
                val root = JSONObject(body)
                if (!root.optBoolean("ok")) {
                    return CatalogFetchResult(
                        error = root.optString("error").ifBlank { "Invalid API response." }
                    )
                }

                val items = root.optJSONArray("items") ?: JSONArray()
                val cards = mutableListOf<CatalogCard>()
                for (i in 0 until items.length()) {
                    val row = items.optJSONObject(i) ?: continue
                    cards.add(
                        CatalogCard(
                            id = row.optString("id"),
                            slug = row.optString("slug"),
                            title = row.optString("title"),
                            year = row.optString("year"),
                            type = row.optString("type"),
                            poster = row.optString("poster"),
                            backdrop = row.optString("backdrop"),
                            qualityRow = readStringArray(row.optJSONArray("quality_row")),
                            seasonText = row.optString("season_text"),
                        )
                    )
                }

                val hero = parseHero(root.optJSONArray("slider"))
                CatalogFetchResult(CatalogResponse(cards = cards, hero = hero))
            }
        } catch (_: SocketTimeoutException) {
            CatalogFetchResult(error = "Request timeout. Server may be waking up.")
        } catch (e: Exception) {
            CatalogFetchResult(error = e.message ?: "Request failed.")
        }
    }

    private fun parseHero(sliderRows: JSONArray?): HeroSlide? {
        if (sliderRows == null || sliderRows.length() == 0) return null
        val row = sliderRows.optJSONObject(0) ?: return null
        val detailPath = row.optString("detail_path")
        val contentKey = extractContentKeyFromPath(detailPath)
        return HeroSlide(
            title = row.optString("title"),
            subtitle = row.optString("subtitle"),
            image = row.optString("image"),
            contentKey = contentKey
        )
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

    private fun openContentDetail(contentKey: String) {
        val intent = Intent(this, ContentDetailActivity::class.java)
        intent.putExtra("content_key", contentKey)
        startActivity(intent)
    }
}
