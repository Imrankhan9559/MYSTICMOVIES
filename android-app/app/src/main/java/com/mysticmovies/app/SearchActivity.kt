package com.mysticmovies.app

import android.content.Intent
import android.os.Bundle
import android.text.Editable
import android.text.TextWatcher
import android.view.View
import android.view.inputmethod.EditorInfo
import android.widget.Button
import android.widget.EditText
import android.widget.ProgressBar
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import androidx.lifecycle.lifecycleScope
import androidx.recyclerview.widget.GridLayoutManager
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import okhttp3.Request
import org.json.JSONArray
import org.json.JSONObject

class SearchActivity : AppCompatActivity() {
    private val client = createApiHttpClient()

    private lateinit var etSearch: EditText
    private lateinit var btnSearch: Button
    private lateinit var btnBack: Button
    private lateinit var tvTrending: TextView
    private lateinit var tvLatestTitle: TextView
    private lateinit var tvResultsTitle: TextView
    private lateinit var rvSuggestions: RecyclerView
    private lateinit var rvLatest: RecyclerView
    private lateinit var rvResults: RecyclerView
    private lateinit var progressBar: ProgressBar
    private lateinit var tvEmpty: TextView

    private lateinit var suggestionAdapter: SearchSuggestionAdapter
    private lateinit var resultAdapter: CatalogAdapter

    private var debounceJob: Job? = null
    private var currentQuery: String = ""

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_search)

        etSearch = findViewById(R.id.etSearch)
        btnSearch = findViewById(R.id.btnSearch)
        btnBack = findViewById(R.id.btnBack)
        tvTrending = findViewById(R.id.tvTrending)
        tvLatestTitle = findViewById(R.id.tvLatestTitle)
        tvResultsTitle = findViewById(R.id.tvResultsTitle)
        rvSuggestions = findViewById(R.id.rvSuggestions)
        rvLatest = findViewById(R.id.rvLatest)
        rvResults = findViewById(R.id.rvResults)
        progressBar = findViewById(R.id.progressBar)
        tvEmpty = findViewById(R.id.tvEmpty)

        suggestionAdapter = SearchSuggestionAdapter { card ->
            val key = card.slug.ifBlank { card.id }
            if (key.isNotBlank()) {
                openContentDetail(key)
            }
        }
        rvSuggestions.layoutManager = LinearLayoutManager(this)
        rvSuggestions.adapter = suggestionAdapter

        resultAdapter = CatalogAdapter { card ->
            val key = card.slug.ifBlank { card.id }
            if (key.isNotBlank()) {
                openContentDetail(key)
            }
        }
        rvResults.layoutManager = GridLayoutManager(this, 2)
        rvResults.adapter = resultAdapter
        rvResults.visibility = View.GONE
        tvResultsTitle.visibility = View.GONE

        rvLatest.layoutManager = LinearLayoutManager(this, LinearLayoutManager.HORIZONTAL, false)
        rvLatest.adapter = HomeStripAdapter(emptyList()) { card ->
            val key = card.slug.ifBlank { card.id }
            if (key.isNotBlank()) {
                openContentDetail(key)
            }
        }

        btnBack.setOnClickListener { finish() }
        btnSearch.setOnClickListener {
            currentQuery = etSearch.text?.toString()?.trim().orEmpty()
            searchCatalog(currentQuery)
        }
        etSearch.setOnEditorActionListener { _, actionId, _ ->
            if (actionId == EditorInfo.IME_ACTION_SEARCH) {
                currentQuery = etSearch.text?.toString()?.trim().orEmpty()
                searchCatalog(currentQuery)
                true
            } else {
                false
            }
        }
        etSearch.addTextChangedListener(object : TextWatcher {
            override fun beforeTextChanged(s: CharSequence?, start: Int, count: Int, after: Int) = Unit
            override fun onTextChanged(s: CharSequence?, start: Int, before: Int, count: Int) = Unit
            override fun afterTextChanged(s: Editable?) {
                val query = s?.toString()?.trim().orEmpty()
                debounceJob?.cancel()
                if (query.length < 2) {
                    rvSuggestions.visibility = View.GONE
                    suggestionAdapter.submit(emptyList())
                    if (query.isBlank()) {
                        rvResults.visibility = View.GONE
                        tvResultsTitle.visibility = View.GONE
                        resultAdapter.submitItems(emptyList())
                        tvEmpty.visibility = View.VISIBLE
                        tvEmpty.text = "Search to discover content"
                    }
                    return
                }
                debounceJob = lifecycleScope.launch {
                    delay(250)
                    loadSuggestions(query)
                }
            }
        })

        lifecycleScope.launch {
            loadSuggestions("movie")
            loadLatestReleases()
        }
    }

    private fun loadSuggestions(query: String) {
        lifecycleScope.launch {
            val result = withContext(Dispatchers.IO) { fetchSuggestions(query) }
            if (!result.first) {
                rvSuggestions.visibility = View.GONE
                return@launch
            }
            suggestionAdapter.submit(result.second)
            rvSuggestions.visibility = if (result.second.isEmpty()) View.GONE else View.VISIBLE
            val tags = result.third
            if (tags.isNotEmpty()) {
                tvTrending.visibility = View.VISIBLE
                tvTrending.text = "Trending: ${tags.joinToString(" • ")}"
            } else {
                tvTrending.visibility = View.GONE
            }
        }
    }

    private fun loadLatestReleases() {
        lifecycleScope.launch {
            val result = withContext(Dispatchers.IO) { fetchCatalog("", limit = 15) }
            if (!result.first || result.second.isEmpty()) {
                tvLatestTitle.visibility = View.GONE
                rvLatest.visibility = View.GONE
                return@launch
            }
            rvLatest.adapter = HomeStripAdapter(result.second.take(15)) { card ->
                val key = card.slug.ifBlank { card.id }
                if (key.isNotBlank()) {
                    openContentDetail(key)
                }
            }
            tvLatestTitle.visibility = View.VISIBLE
            rvLatest.visibility = View.VISIBLE
        }
    }

    private fun fetchSuggestions(query: String): Triple<Boolean, List<CatalogCard>, List<String>> {
        for (base in apiBaseCandidates()) {
            try {
                val url = "${base.trimEnd('/')}/app-api/search/suggestions".toHttpUrlOrNull() ?: continue
                val reqUrl = url.newBuilder()
                    .addQueryParameter("q", query)
                    .addQueryParameter("limit", "10")
                    .build()
                val req = Request.Builder().url(reqUrl).get().build()
                client.newCall(req).execute().use { res ->
                    if (!res.isSuccessful) return@use
                    val root = JSONObject(res.body?.string().orEmpty())
                    if (!root.optBoolean("ok")) return@use
                    AppRuntimeState.apiBaseUrl = base.trimEnd('/')
                    val suggestions = parseCardArray(root.optJSONArray("items"))
                    val trending = readStringArray(root.optJSONArray("trending"))
                    return Triple(true, suggestions, trending)
                }
            } catch (_: Exception) {
                // Try next base URL.
            }
        }
        return Triple(false, emptyList(), emptyList())
    }

    private fun searchCatalog(query: String) {
        if (query.isBlank()) {
            rvResults.visibility = View.GONE
            tvResultsTitle.visibility = View.GONE
            resultAdapter.submitItems(emptyList())
            tvEmpty.visibility = View.VISIBLE
            tvEmpty.text = "Search to discover content"
            return
        }

        progressBar.visibility = View.VISIBLE
        tvEmpty.visibility = View.GONE
        lifecycleScope.launch {
            val result = withContext(Dispatchers.IO) { fetchCatalog(query, limit = 36) }
            progressBar.visibility = View.GONE
            rvResults.visibility = View.VISIBLE
            tvResultsTitle.visibility = View.VISIBLE
            if (!result.first) {
                resultAdapter.submitItems(emptyList())
                tvEmpty.visibility = View.VISIBLE
                tvEmpty.text = "Unable to load content."
                return@launch
            }
            resultAdapter.submitItems(result.second)
            tvEmpty.visibility = if (result.second.isEmpty()) View.VISIBLE else View.GONE
            tvEmpty.text = if (result.second.isEmpty()) "No results found." else ""
        }
    }

    private fun fetchCatalog(query: String, limit: Int): Pair<Boolean, List<CatalogCard>> {
        for (base in apiBaseCandidates()) {
            try {
                val url = "${base.trimEnd('/')}/app-api/catalog".toHttpUrlOrNull() ?: continue
                val reqUrl = url.newBuilder()
                    .addQueryParameter("filter", "all")
                    .addQueryParameter("sort", "release_new")
                    .addQueryParameter("page", "1")
                    .addQueryParameter("per_page", limit.toString())
                    .addQueryParameter("q", query)
                    .build()
                val req = Request.Builder().url(reqUrl).get().build()
                client.newCall(req).execute().use { res ->
                    if (!res.isSuccessful) return@use
                    val root = JSONObject(res.body?.string().orEmpty())
                    if (!root.optBoolean("ok")) return@use
                    AppRuntimeState.apiBaseUrl = base.trimEnd('/')
                    return Pair(true, parseCardArray(root.optJSONArray("items")))
                }
            } catch (_: Exception) {
                // Try next.
            }
        }
        return Pair(false, emptyList())
    }

    private fun parseCardArray(array: JSONArray?): List<CatalogCard> {
        if (array == null) return emptyList()
        val rows = mutableListOf<CatalogCard>()
        for (i in 0 until array.length()) {
            val row = array.optJSONObject(i) ?: continue
            rows.add(
                CatalogCard(
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

    private fun openContentDetail(contentKey: String) {
        val intent = Intent(this, ContentDetailActivity::class.java)
        intent.putExtra("content_key", contentKey)
        startActivity(intent)
    }
}
