package com.mysticmovies.app

import android.content.Intent
import android.net.Uri
import android.os.Bundle
import android.os.Environment
import android.view.View
import android.widget.Button
import android.widget.ImageView
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import coil.load

class DownloadsActivity : AppCompatActivity() {
    private lateinit var tvTopbar: TextView
    private lateinit var imgHeaderLogo: ImageView
    private lateinit var tvHeaderTitle: TextView
    private lateinit var rvDownloads: RecyclerView
    private lateinit var tvEmpty: TextView
    private lateinit var tvPath: TextView
    private lateinit var adapter: DownloadsAdapter

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_downloads)

        tvTopbar = findViewById(R.id.tvTopbar)
        imgHeaderLogo = findViewById(R.id.imgHeaderLogo)
        tvHeaderTitle = findViewById(R.id.tvHeaderTitle)
        rvDownloads = findViewById(R.id.rvDownloads)
        tvEmpty = findViewById(R.id.tvEmpty)
        tvPath = findViewById(R.id.tvPath)
        findViewById<Button>(R.id.btnBack).setOnClickListener { finish() }

        adapter = DownloadsAdapter { file -> openFile(file) }
        rvDownloads.layoutManager = LinearLayoutManager(this)
        rvDownloads.adapter = adapter

        applyRuntimeUi()
        refreshFiles()
    }

    override fun onResume() {
        super.onResume()
        refreshFiles()
    }

    private fun refreshFiles() {
        val dir = getExternalFilesDir(Environment.DIRECTORY_DOWNLOADS)
        tvPath.text = if (dir != null) "Stored in: ${dir.absolutePath}" else "Download folder unavailable."

        if (dir == null || !dir.exists()) {
            adapter.submit(emptyList())
            tvEmpty.visibility = View.VISIBLE
            return
        }

        val rows = dir.listFiles()
            ?.filter { it.isFile }
            ?.sortedByDescending { it.lastModified() }
            ?.map { file ->
                DownloadedFile(
                    name = file.name,
                    uri = Uri.fromFile(file).toString(),
                    sizeBytes = file.length(),
                    modifiedAt = file.lastModified(),
                )
            }
            .orEmpty()

        adapter.submit(rows)
        tvEmpty.visibility = if (rows.isEmpty()) View.VISIBLE else View.GONE
    }

    private fun openFile(file: DownloadedFile) {
        val intent = Intent(this, PlayerActivity::class.java).apply {
            putExtra(PlayerActivity.EXTRA_STREAM_URL, file.uri)
            putExtra(PlayerActivity.EXTRA_TITLE, file.name)
        }
        startActivity(intent)
    }

    private fun applyRuntimeUi() {
        val ui = AppRuntimeState.ui
        tvTopbar.text = ui.topbarText.ifBlank { "Welcome to Mystic Movies" }
        tvHeaderTitle.text = "Downloads"
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
}
