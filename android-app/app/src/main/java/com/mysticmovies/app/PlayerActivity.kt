package com.mysticmovies.app

import android.net.Uri
import android.os.Bundle
import android.view.View
import android.widget.Button
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import androidx.media3.common.MediaItem
import androidx.media3.common.Player
import androidx.media3.exoplayer.ExoPlayer
import androidx.media3.ui.PlayerView
import kotlin.math.max

class PlayerActivity : AppCompatActivity() {
    companion object {
        const val EXTRA_STREAM_URL = "stream_url"
        const val EXTRA_TITLE = "title"
        private const val PREF_PLAYER = "mm_player_prefs"
    }

    private lateinit var playerView: PlayerView
    private lateinit var tvTitle: TextView
    private lateinit var tvStatus: TextView
    private var player: ExoPlayer? = null
    private var streamUrl: String = ""
    private var resumeKey: String = ""

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_player)

        playerView = findViewById(R.id.playerView)
        tvTitle = findViewById(R.id.tvPlayerTitle)
        tvStatus = findViewById(R.id.tvStatus)
        findViewById<Button>(R.id.btnBack).setOnClickListener { finish() }

        streamUrl = intent?.getStringExtra(EXTRA_STREAM_URL).orEmpty().trim()
        val label = intent?.getStringExtra(EXTRA_TITLE).orEmpty().ifBlank { "Player" }
        tvTitle.text = label

        if (streamUrl.isBlank()) {
            tvStatus.visibility = View.VISIBLE
            tvStatus.text = "Invalid stream URL."
            return
        }
        resumeKey = "resume_" + streamUrl.hashCode()
        initPlayer()
    }

    private fun initPlayer() {
        val savedPosition = getSharedPreferences(PREF_PLAYER, MODE_PRIVATE).getLong(resumeKey, 0L)
        val exo = ExoPlayer.Builder(this).build()
        player = exo
        playerView.player = exo
        playerView.keepScreenOn = true
        exo.repeatMode = Player.REPEAT_MODE_OFF

        exo.addListener(object : Player.Listener {
            override fun onPlayerError(error: androidx.media3.common.PlaybackException) {
                tvStatus.visibility = View.VISIBLE
                tvStatus.text = "Playback error: ${error.errorCodeName}"
            }
        })

        val mediaUri = try {
            Uri.parse(streamUrl)
        } catch (_: Exception) {
            null
        }
        if (mediaUri == null) {
            tvStatus.visibility = View.VISIBLE
            tvStatus.text = "Invalid media URI."
            return
        }

        val item = MediaItem.fromUri(mediaUri)
        exo.setMediaItem(item)
        exo.prepare()
        if (savedPosition > 2_000L) {
            exo.seekTo(max(0L, savedPosition))
        }
        exo.playWhenReady = true
    }

    private fun persistPosition() {
        val current = player?.currentPosition ?: 0L
        if (resumeKey.isBlank()) return
        getSharedPreferences(PREF_PLAYER, MODE_PRIVATE)
            .edit()
            .putLong(resumeKey, current)
            .apply()
    }

    override fun onPause() {
        super.onPause()
        persistPosition()
        player?.playWhenReady = false
    }

    override fun onStop() {
        super.onStop()
        persistPosition()
    }

    override fun onDestroy() {
        persistPosition()
        playerView.player = null
        player?.release()
        player = null
        super.onDestroy()
    }
}

