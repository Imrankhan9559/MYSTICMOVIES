package com.mysticmovies.app

import android.content.Intent
import android.os.Bundle
import android.view.View
import android.widget.Button
import android.widget.ImageView
import android.widget.ProgressBar
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import androidx.lifecycle.lifecycleScope
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import coil.load
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import okhttp3.Request
import org.json.JSONArray
import org.json.JSONObject

class ProfileActivity : AppCompatActivity() {
    private val client = createApiHttpClient()

    private lateinit var tvTopbar: TextView
    private lateinit var imgHeaderLogo: ImageView
    private lateinit var tvHeaderTitle: TextView
    private lateinit var tvUser: TextView
    private lateinit var tvWatchlistCount: TextView
    private lateinit var tvContinueCount: TextView
    private lateinit var tvHistoryCount: TextView
    private lateinit var progressBar: ProgressBar
    private lateinit var tvEmpty: TextView
    private lateinit var rvWatchlist: RecyclerView
    private lateinit var rvContinue: RecyclerView
    private lateinit var rvHistory: RecyclerView

    private lateinit var watchlistAdapter: ProfileStripAdapter
    private lateinit var continueAdapter: ProfileStripAdapter
    private lateinit var historyAdapter: ProfileStripAdapter
    private var loginRequested = false
    private var profileLoaded = false

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_profile)

        tvTopbar = findViewById(R.id.tvTopbar)
        imgHeaderLogo = findViewById(R.id.imgHeaderLogo)
        tvHeaderTitle = findViewById(R.id.tvHeaderTitle)
        tvUser = findViewById(R.id.tvUser)
        tvWatchlistCount = findViewById(R.id.tvWatchlistCount)
        tvContinueCount = findViewById(R.id.tvContinueCount)
        tvHistoryCount = findViewById(R.id.tvHistoryCount)
        progressBar = findViewById(R.id.progressBar)
        tvEmpty = findViewById(R.id.tvEmpty)
        rvWatchlist = findViewById(R.id.rvWatchlist)
        rvContinue = findViewById(R.id.rvContinue)
        rvHistory = findViewById(R.id.rvHistory)

        findViewById<Button>(R.id.btnBack).setOnClickListener { finish() }
        findViewById<Button>(R.id.btnHome).setOnClickListener {
            startActivity(Intent(this, MainActivity::class.java).apply { addFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP) })
        }
        findViewById<Button>(R.id.btnDownloads).setOnClickListener {
            startActivity(Intent(this, DownloadsActivity::class.java))
        }
        findViewById<Button>(R.id.btnSearch).setOnClickListener {
            startActivity(Intent(this, SearchActivity::class.java))
        }

        watchlistAdapter = ProfileStripAdapter { entry -> openEntry(entry) }
        continueAdapter = ProfileStripAdapter { entry -> openEntry(entry) }
        historyAdapter = ProfileStripAdapter { entry -> openEntry(entry) }
        rvWatchlist.layoutManager = LinearLayoutManager(this, LinearLayoutManager.HORIZONTAL, false)
        rvContinue.layoutManager = LinearLayoutManager(this, LinearLayoutManager.HORIZONTAL, false)
        rvHistory.layoutManager = LinearLayoutManager(this, LinearLayoutManager.HORIZONTAL, false)
        rvWatchlist.adapter = watchlistAdapter
        rvContinue.adapter = continueAdapter
        rvHistory.adapter = historyAdapter

        applyRuntimeUi()
        ensureLoginAndLoadProfile(autoOpenLogin = true)
    }

    override fun onResume() {
        super.onResume()
        if (loginRequested && !profileLoaded) {
            ensureLoginAndLoadProfile(autoOpenLogin = false)
        }
    }

    private fun applyRuntimeUi() {
        val ui = AppRuntimeState.ui
        tvTopbar.text = ui.topbarText.ifBlank { "Welcome to Mystic Movies" }
        tvHeaderTitle.text = "Profile"
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
    }

    private fun loadProfile() {
        progressBar.visibility = View.VISIBLE
        tvEmpty.visibility = View.GONE
        lifecycleScope.launch {
            val root = withContext(Dispatchers.IO) { fetchProfile() }
            progressBar.visibility = View.GONE
            if (root == null) {
                tvEmpty.visibility = View.VISIBLE
                tvEmpty.text = "Login required to access profile."
                tvEmpty.setOnClickListener { openLogin() }
                watchlistAdapter.submit(emptyList())
                continueAdapter.submit(emptyList())
                historyAdapter.submit(emptyList())
                return@launch
            }
            bindProfile(root)
            profileLoaded = true
        }
    }

    private fun ensureLoginAndLoadProfile(autoOpenLogin: Boolean) {
        lifecycleScope.launch {
            val loggedIn = withContext(Dispatchers.IO) { fetchSessionLoggedIn() }
            if (loggedIn) {
                loginRequested = false
                loadProfile()
                return@launch
            }

            tvEmpty.visibility = View.VISIBLE
            tvEmpty.text = "Login required to access profile."
            if (autoOpenLogin) {
                loginRequested = true
                openLogin()
            } else {
                finish()
            }
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

    private fun fetchProfile(): JSONObject? {
        for (base in apiBaseCandidates()) {
            try {
                val url = "${base.trimEnd('/')}/app-api/profile".toHttpUrlOrNull() ?: continue
                val req = Request.Builder().url(url).get().build()
                client.newCall(req).execute().use { res ->
                    if (res.code == 401) return null
                    if (!res.isSuccessful) return@use
                    val root = JSONObject(res.body?.string().orEmpty())
                    if (!root.optBoolean("ok")) return@use
                    AppRuntimeState.apiBaseUrl = base.trimEnd('/')
                    return root
                }
            } catch (_: Exception) {
                // try next.
            }
        }
        return null
    }

    private fun bindProfile(root: JSONObject) {
        val user = root.optJSONObject("user") ?: JSONObject()
        val name = user.optString("name").ifBlank { "Mystic User" }
        val phone = user.optString("phone")
        tvUser.text = if (phone.isNotBlank()) "$name ($phone)" else name

        val watchlist = parseProfileEntries(root.optJSONArray("watchlist"))
        val continueRows = parseProfileEntries(root.optJSONArray("continue_watching"))
        val historyRows = parseProfileEntries(root.optJSONArray("watch_history"))

        watchlistAdapter.submit(watchlist)
        continueAdapter.submit(continueRows)
        historyAdapter.submit(historyRows)

        val counts = root.optJSONObject("counts") ?: JSONObject()
        tvWatchlistCount.text = "Watchlist: ${counts.optInt("watchlist", watchlist.size)}"
        tvContinueCount.text = "Continue: ${counts.optInt("continue_watching", continueRows.size)}"
        tvHistoryCount.text = "History: ${counts.optInt("watch_history", historyRows.size)}"

        val empty = watchlist.isEmpty() && continueRows.isEmpty() && historyRows.isEmpty()
        tvEmpty.visibility = if (empty) View.VISIBLE else View.GONE
        if (empty) {
            tvEmpty.text = "No watch activity yet."
        }
    }

    private fun parseProfileEntries(array: JSONArray?): List<ProfileEntry> {
        if (array == null) return emptyList()
        val rows = mutableListOf<ProfileEntry>()
        for (i in 0 until array.length()) {
            val row = array.optJSONObject(i) ?: continue
            val card = CatalogCard(
                id = row.optString("id"),
                slug = row.optString("slug"),
                title = row.optString("title"),
                year = row.optString("year"),
                type = row.optString("type"),
                poster = row.optString("poster").ifBlank { row.optString("poster_original") },
                backdrop = row.optString("backdrop").ifBlank { row.optString("backdrop_original") },
                qualityRow = emptyList(),
                seasonText = row.optString("season_text"),
            )
            rows.add(
                ProfileEntry(
                    card = card,
                    displayTitle = row.optString("display_title"),
                    streamUrl = row.optString("stream_url"),
                )
            )
        }
        return rows
    }

    private fun openEntry(entry: ProfileEntry) {
        if (entry.streamUrl.isNotBlank()) {
            val target = absoluteUrl(entry.streamUrl)
            if (target.isNotBlank()) {
                val intent = Intent(this, PlayerActivity::class.java).apply {
                    putExtra(PlayerActivity.EXTRA_STREAM_URL, target)
                    putExtra(PlayerActivity.EXTRA_TITLE, entry.displayTitle.ifBlank { entry.card.title })
                }
                startActivity(intent)
                return
            }
        }
        val key = entry.card.slug.ifBlank { entry.card.id }
        if (key.isBlank()) return
        startActivity(Intent(this, ContentDetailActivity::class.java).apply {
            putExtra("content_key", key)
        })
    }

    private fun openLogin() {
        startActivity(Intent(this, LoginActivity::class.java).apply {
            putExtra("target_url", absoluteUrl("/login"))
            putExtra("title_text", "Login")
        })
    }
}
