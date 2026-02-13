package com.mysticmovies.app

import android.content.Intent
import android.os.Bundle
import android.provider.Settings
import android.view.View
import android.widget.ImageView
import android.widget.LinearLayout
import android.widget.ProgressBar
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import androidx.lifecycle.lifecycleScope
import androidx.recyclerview.widget.LinearLayoutManager
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
    val hero: HeroSlide?,
    val sections: List<HomeSectionData>,
)

private data class CatalogFetchResult(
    val response: CatalogResponse? = null,
    val error: String = "",
)

private data class HeroSlide(
    val title: String,
    val subtitle: String,
    val image: String,
    val contentKey: String,
)

private data class HomeSectionData(
    val key: String,
    val title: String,
    val layout: String,
    val cards: List<CatalogCard>,
    val castCards: List<CastProfileCard>,
)

class MainActivity : AppCompatActivity() {
    private val client = createApiHttpClient()

    private lateinit var tvTopbar: TextView
    private lateinit var tvHeaderTitle: TextView
    private lateinit var imgHeaderLogo: ImageView
    private lateinit var btnHeaderLogin: MaterialButton
    private lateinit var btnActionRequest: MaterialButton
    private lateinit var btnActionSearch: MaterialButton
    private lateinit var btnActionDownloads: MaterialButton
    private lateinit var btnActionProfile: MaterialButton
    private lateinit var tvAnnouncement: TextView
    private lateinit var tvFooterText: TextView

    private lateinit var btnNavSearch: MaterialButton
    private lateinit var btnNavHome: MaterialButton
    private lateinit var btnNavDownloads: MaterialButton
    private lateinit var btnNavProfile: MaterialButton

    private lateinit var progressBar: ProgressBar
    private lateinit var loadingIcon: ImageView
    private lateinit var emptyView: TextView
    private lateinit var heroImage: ImageView
    private lateinit var heroTitle: TextView
    private lateinit var heroSubtitle: TextView
    private lateinit var sectionsContainer: LinearLayout

    private var loading = false
    private var updatePromptShown = false

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        bindViews()
        bindActions()
        applyRuntimeUi()

        loadBootstrap()
        loadHome()
    }

    override fun onResume() {
        super.onResume()
        applyRuntimeUi()
    }

    private fun bindViews() {
        tvTopbar = findViewById(R.id.tvTopbar)
        tvHeaderTitle = findViewById(R.id.tvHeaderTitle)
        imgHeaderLogo = findViewById(R.id.imgHeaderLogo)
        btnHeaderLogin = findViewById(R.id.btnHeaderLogin)
        btnActionRequest = findViewById(R.id.btnActionRequest)
        btnActionSearch = findViewById(R.id.btnActionSearch)
        btnActionDownloads = findViewById(R.id.btnActionDownloads)
        btnActionProfile = findViewById(R.id.btnActionProfile)
        tvAnnouncement = findViewById(R.id.tvAnnouncement)
        tvFooterText = findViewById(R.id.tvFooterText)

        btnNavSearch = findViewById(R.id.btnNavSearch)
        btnNavHome = findViewById(R.id.btnNavHome)
        btnNavDownloads = findViewById(R.id.btnNavDownloads)
        btnNavProfile = findViewById(R.id.btnNavProfile)

        progressBar = findViewById(R.id.progressBar)
        loadingIcon = findViewById(R.id.imgLoadingIcon)
        emptyView = findViewById(R.id.tvEmpty)
        heroImage = findViewById(R.id.imgHero)
        heroTitle = findViewById(R.id.tvHeroTitle)
        heroSubtitle = findViewById(R.id.tvHeroSubtitle)
        sectionsContainer = findViewById(R.id.sectionsContainer)
    }

    private fun bindActions() {
        btnHeaderLogin.setOnClickListener {
            openInAppWeb("/login?return_url=%2Fcontent", "Login")
        }

        btnActionSearch.setOnClickListener { openSearch() }
        btnActionRequest.setOnClickListener { openRequestContent("/content") }
        btnActionDownloads.setOnClickListener { openDownloads() }
        btnActionProfile.setOnClickListener { openProfile() }

        btnNavSearch.setOnClickListener { openSearch() }
        btnNavHome.setOnClickListener {
            sectionsContainer.requestFocus()
            sectionsContainer.scrollTo(0, 0)
        }
        btnNavDownloads.setOnClickListener { openDownloads() }
        btnNavProfile.setOnClickListener { openProfile() }
    }

    private fun applyRuntimeUi() {
        val ui = AppRuntimeState.ui
        tvTopbar.text = ui.topbarText.ifBlank { "Welcome to Mystic Movies" }
        tvHeaderTitle.text = ui.siteName.ifBlank { "mysticmovies" }
        tvFooterText.text = ui.footerText.ifBlank { "MysticMovies" }
        title = ui.siteName.ifBlank { "MysticMovies" }

        if (ui.logoUrl.isNotBlank()) {
            imgHeaderLogo.visibility = View.VISIBLE
            imgHeaderLogo.load(resolveImageUrl(ui.logoUrl)) {
                crossfade(true)
                error(android.R.drawable.sym_def_app_icon)
                placeholder(android.R.drawable.sym_def_app_icon)
            }
        } else {
            imgHeaderLogo.visibility = View.GONE
        }

        val loadingIconUrl = AppRuntimeState.loadingIconUrl
        if (loadingIconUrl.isNotBlank()) {
            loadingIcon.load(resolveImageUrl(loadingIconUrl)) {
                crossfade(true)
                error(android.R.drawable.ic_popup_sync)
                placeholder(android.R.drawable.ic_popup_sync)
            }
        } else {
            loadingIcon.setImageResource(android.R.drawable.ic_popup_sync)
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
                client.newCall(hsReq).execute().use hsUse@{ hsRes ->
                    if (!hsRes.isSuccessful) return@hsUse
                    val hsBody = hsRes.body?.string().orEmpty()
                    val hsRoot = JSONObject(hsBody)
                    if (!hsRoot.optBoolean("ok")) return@hsUse
                    val token = hsRoot.optString("handshake_token").trim()
                    if (token.isBlank()) return@hsUse

                    val bsReq = Request.Builder()
                        .url("$baseUrl/app-api/bootstrap")
                        .header("X-App-Handshake", token)
                        .get()
                        .build()
                    client.newCall(bsReq).execute().use bsUse@{ bsRes ->
                        if (!bsRes.isSuccessful) return@bsUse
                        val bsBody = bsRes.body?.string().orEmpty()
                        val bsRoot = JSONObject(bsBody)
                        if (!bsRoot.optBoolean("ok")) return@bsUse

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
                    openInAppWeb(url, "App Update")
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

    private fun loadHome(showLoader: Boolean = true) {
        if (loading || AppRuntimeState.maintenanceMode) return
        loading = true

        if (showLoader) {
            progressBar.visibility = View.VISIBLE
            loadingIcon.visibility = View.VISIBLE
        }
        emptyView.visibility = View.GONE

        lifecycleScope.launch {
            val result = withContext(Dispatchers.IO) { fetchHomeCatalog() }
            loading = false
            progressBar.visibility = View.GONE
            loadingIcon.visibility = View.GONE

            if (result.response == null) {
                emptyView.visibility = View.VISIBLE
                emptyView.text = if (result.error.isNotBlank()) {
                    "Unable to load content.\n${result.error}"
                } else {
                    "Unable to load content. Please try again."
                }
                bindSections(emptyList())
                bindHero(null)
                return@launch
            }

            bindHero(result.response.hero)
            bindSections(result.response.sections.ifEmpty {
                listOf(
                    HomeSectionData(
                        key = "fallback",
                        title = "Latest Releases",
                        layout = "poster_row",
                        cards = result.response.cards,
                        castCards = emptyList(),
                    )
                )
            })

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
                heroImage.load(resolveImageUrl(splash)) {
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
        heroImage.load(resolveImageUrl(hero.image)) {
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

    private fun bindSections(sections: List<HomeSectionData>) {
        sectionsContainer.removeAllViews()
        if (sections.isEmpty()) return
        sections.forEach { section ->
            if (section.layout == "cast_row" && section.castCards.isEmpty()) return@forEach
            if (section.layout != "cast_row" && section.cards.isEmpty()) return@forEach

            val view = layoutInflater.inflate(R.layout.item_home_section, sectionsContainer, false)
            val title = view.findViewById<TextView>(R.id.tvSectionTitle)
            val recycler = view.findViewById<androidx.recyclerview.widget.RecyclerView>(R.id.rvSection)
            title.text = section.title.ifBlank { "Section" }
            recycler.layoutManager = LinearLayoutManager(this, LinearLayoutManager.HORIZONTAL, false)
            if (section.layout == "cast_row") {
                recycler.adapter = CastStripAdapter(section.castCards) { cast ->
                    if (cast.castPath.isNotBlank()) {
                        openInAppWeb(cast.castPath, cast.name)
                    }
                }
            } else {
                recycler.adapter = HomeStripAdapter(section.cards) { card ->
                    val key = card.slug.ifBlank { card.id }
                    if (key.isNotBlank()) {
                        openContentDetail(key)
                    }
                }
            }
            sectionsContainer.addView(view)
        }
    }

    private fun fetchHomeCatalog(): CatalogFetchResult {
        var lastError = ""
        for (baseUrl in apiBaseCandidates()) {
            val result = fetchHomeCatalogFromBase(baseUrl)
            if (result.response != null) {
                AppRuntimeState.apiBaseUrl = baseUrl.trimEnd('/')
                return result
            }
            lastError = result.error
            if (lastError.contains("404")) break
        }
        return CatalogFetchResult(null, lastError.ifBlank { "Network error." })
    }

    private fun fetchHomeCatalogFromBase(baseUrl: String): CatalogFetchResult {
        return try {
            val httpUrl = "${baseUrl.trimEnd('/')}/app-api/catalog".toHttpUrlOrNull()
                ?: return CatalogFetchResult(error = "Invalid API URL.")
            val reqUrl = httpUrl.newBuilder()
                .addQueryParameter("filter", "all")
                .addQueryParameter("sort", "release_new")
                .addQueryParameter("page", "1")
                .addQueryParameter("per_page", "48")
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
                    return CatalogFetchResult(error = root.optString("error").ifBlank { "Invalid API response." })
                }

                val cards = parseCardArray(root.optJSONArray("items"))
                val hero = parseHero(root.optJSONArray("slider"))
                val sections = parseSections(root.optJSONArray("home_sections"), cards)
                CatalogFetchResult(CatalogResponse(cards = cards, hero = hero, sections = sections))
            }
        } catch (_: SocketTimeoutException) {
            CatalogFetchResult(error = "Request timeout. Server may be waking up.")
        } catch (e: Exception) {
            CatalogFetchResult(error = e.message ?: "Request failed.")
        }
    }

    private fun parseCardArray(array: JSONArray?): List<CatalogCard> {
        if (array == null) return emptyList()
        val out = mutableListOf<CatalogCard>()
        for (i in 0 until array.length()) {
            val row = array.optJSONObject(i) ?: continue
            out.add(parseCardRow(row))
        }
        return out
    }

    private fun parseCardRow(row: JSONObject): CatalogCard {
        return CatalogCard(
            id = row.optString("id"),
            slug = row.optString("slug"),
            title = row.optString("title"),
            year = row.optString("year"),
            type = row.optString("type"),
            poster = row.optString("poster").ifBlank { row.optString("poster_original") },
            backdrop = row.optString("backdrop").ifBlank { row.optString("backdrop_original") },
            qualityRow = readStringArray(row.optJSONArray("quality_row")),
            seasonText = row.optString("season_text"),
        )
    }

    private fun parseSections(sectionRows: JSONArray?, fallbackCards: List<CatalogCard>): List<HomeSectionData> {
        if (sectionRows == null) {
            return if (fallbackCards.isEmpty()) emptyList() else listOf(
                HomeSectionData(
                    key = "latest",
                    title = "Latest Releases",
                    layout = "poster_row",
                    cards = fallbackCards,
                    castCards = emptyList(),
                )
            )
        }

        val out = mutableListOf<HomeSectionData>()
        for (i in 0 until sectionRows.length()) {
            val section = sectionRows.optJSONObject(i) ?: continue
            val key = section.optString("key")
            val title = section.optString("title")
            val layout = section.optString("layout").ifBlank { "poster_row" }
            val items = section.optJSONArray("items") ?: JSONArray()

            if (layout == "cast_row") {
                val castItems = mutableListOf<CastProfileCard>()
                for (j in 0 until items.length()) {
                    val row = items.optJSONObject(j) ?: continue
                    castItems.add(
                        CastProfileCard(
                            name = row.optString("name"),
                            role = row.optString("role"),
                            image = row.optString("image"),
                            castPath = row.optString("cast_path"),
                        )
                    )
                }
                out.add(
                    HomeSectionData(
                        key = key,
                        title = title,
                        layout = layout,
                        cards = emptyList(),
                        castCards = castItems,
                    )
                )
            } else {
                val cards = mutableListOf<CatalogCard>()
                for (j in 0 until items.length()) {
                    val row = items.optJSONObject(j) ?: continue
                    cards.add(parseCardRow(row))
                }
                out.add(
                    HomeSectionData(
                        key = key,
                        title = title,
                        layout = layout,
                        cards = cards,
                        castCards = emptyList(),
                    )
                )
            }
        }
        return out
    }

    private fun parseHero(sliderRows: JSONArray?): HeroSlide? {
        if (sliderRows == null || sliderRows.length() == 0) return null
        val row = sliderRows.optJSONObject(0) ?: return null
        val detailPath = row.optString("detail_path")
        val contentKey = extractContentKeyFromPath(detailPath)
        return HeroSlide(
            title = row.optString("title"),
            subtitle = row.optString("subtitle"),
            image = row.optString("image").ifBlank { row.optString("image_original") },
            contentKey = contentKey,
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

    private fun openSearch() {
        startActivity(Intent(this, SearchActivity::class.java))
    }

    private fun openDownloads() {
        startActivity(Intent(this, DownloadsActivity::class.java))
    }

    private fun openProfile() {
        startActivity(Intent(this, ProfileActivity::class.java))
    }

    private fun openRequestContent(returnPath: String, prefill: String = "") {
        val encodedReturn = android.net.Uri.encode(returnPath.ifBlank { "/content" })
        val target = buildString {
            append("/request-content?return_to=")
            append(encodedReturn)
            if (prefill.isNotBlank()) {
                append("&prefill_title=")
                append(android.net.Uri.encode(prefill))
            }
        }
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

    private fun openContentDetail(contentKey: String) {
        val intent = Intent(this, ContentDetailActivity::class.java)
        intent.putExtra("content_key", contentKey)
        startActivity(intent)
    }
}
