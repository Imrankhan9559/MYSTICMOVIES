package com.mysticmovies.app

import android.content.Intent
import android.graphics.Color
import android.os.Bundle
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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import okhttp3.OkHttpClient
import okhttp3.Request
import org.json.JSONArray
import org.json.JSONObject
import java.util.concurrent.TimeUnit

private data class CatalogResponse(
    val cards: List<CatalogCard>,
    val hero: HeroSlide?
)

private data class HeroSlide(
    val title: String,
    val subtitle: String,
    val image: String,
    val contentKey: String
)

class MainActivity : AppCompatActivity() {
    private val client = OkHttpClient.Builder()
        .connectTimeout(20, TimeUnit.SECONDS)
        .readTimeout(30, TimeUnit.SECONDS)
        .writeTimeout(20, TimeUnit.SECONDS)
        .build()

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

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

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

        adapter = CatalogAdapter { card ->
            val key = card.slug.ifBlank { card.id }
            if (key.isNotBlank()) {
                openContentDetail(key)
            }
        }
        catalogRecycler.layoutManager = GridLayoutManager(this, 2)
        catalogRecycler.adapter = adapter

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
        applyFilterStyle()

        loadCatalog()
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

    private fun loadCatalog(showLoader: Boolean = true) {
        if (loading) return
        loading = true

        if (showLoader) {
            progressBar.visibility = View.VISIBLE
        } else {
            swipeRefreshLayout.isRefreshing = true
        }
        emptyView.visibility = View.GONE

        lifecycleScope.launch {
            val response = withContext(Dispatchers.IO) {
                fetchCatalog(currentFilter, currentQuery)
            }

            loading = false
            progressBar.visibility = View.GONE
            swipeRefreshLayout.isRefreshing = false

            if (response == null) {
                adapter.submitItems(emptyList())
                emptyView.visibility = View.VISIBLE
                emptyView.text = "Unable to load content. Please try again."
                return@launch
            }

            adapter.submitItems(response.cards)
            bindHero(response.hero)

            if (response.cards.isEmpty()) {
                emptyView.visibility = View.VISIBLE
                emptyView.text = "No content found."
            } else {
                emptyView.visibility = View.GONE
            }
        }
    }

    private fun bindHero(hero: HeroSlide?) {
        if (hero == null || hero.image.isBlank()) {
            heroImage.setImageResource(android.R.drawable.ic_menu_report_image)
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

    private fun fetchCatalog(filter: String, query: String): CatalogResponse? {
        return try {
            val base = "${BuildConfig.API_BASE_URL.trimEnd('/')}/app-api/catalog".toHttpUrlOrNull()
                ?: return null
            val url = base.newBuilder()
                .addQueryParameter("filter", filter)
                .addQueryParameter("sort", "release_new")
                .addQueryParameter("page", "1")
                .addQueryParameter("per_page", "24")
                .apply {
                    if (query.isNotBlank()) addQueryParameter("q", query)
                }
                .build()

            val req = Request.Builder().url(url).get().build()
            client.newCall(req).execute().use { res ->
                if (!res.isSuccessful) return null
                val body = res.body?.string().orEmpty()
                val root = JSONObject(body)
                if (!root.optBoolean("ok")) return null

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
                CatalogResponse(cards = cards, hero = hero)
            }
        } catch (_: Exception) {
            null
        }
    }

    private fun parseHero(sliderRows: JSONArray?): HeroSlide? {
        if (sliderRows == null || sliderRows.length() == 0) return null
        val row = sliderRows.optJSONObject(0) ?: return null
        val detailPath = row.optString("detail_path")
        val contentKey = detailPath.substringAfterLast("/", "")
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
