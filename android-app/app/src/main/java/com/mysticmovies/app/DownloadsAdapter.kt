package com.mysticmovies.app

import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.Button
import android.widget.ImageView
import android.widget.TextView
import androidx.recyclerview.widget.RecyclerView
import java.text.DecimalFormat
import java.util.Locale

data class DownloadedFile(
    val name: String,
    val uri: String,
    val sizeBytes: Long,
    val modifiedAt: Long,
)

class DownloadsAdapter(
    private val onPlay: (DownloadedFile) -> Unit
) : RecyclerView.Adapter<DownloadsAdapter.DownloadViewHolder>() {

    private val items = mutableListOf<DownloadedFile>()

    fun submit(rows: List<DownloadedFile>) {
        items.clear()
        items.addAll(rows)
        notifyDataSetChanged()
    }

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): DownloadViewHolder {
        val view = LayoutInflater.from(parent.context).inflate(R.layout.item_download_row, parent, false)
        return DownloadViewHolder(view, onPlay)
    }

    override fun onBindViewHolder(holder: DownloadViewHolder, position: Int) {
        holder.bind(items[position])
    }

    override fun getItemCount(): Int = items.size

    class DownloadViewHolder(
        itemView: View,
        private val onPlay: (DownloadedFile) -> Unit
    ) : RecyclerView.ViewHolder(itemView) {
        private val imgType: ImageView = itemView.findViewById(R.id.imgType)
        private val tvName: TextView = itemView.findViewById(R.id.tvName)
        private val tvMeta: TextView = itemView.findViewById(R.id.tvMeta)
        private val btnPlay: Button = itemView.findViewById(R.id.btnPlay)

        fun bind(file: DownloadedFile) {
            tvName.text = file.name
            val size = formatSize(file.sizeBytes)
            val date = android.text.format.DateFormat.format("dd MMM yyyy HH:mm", file.modifiedAt).toString()
            tvMeta.text = "$size | $date"
            imgType.setImageResource(android.R.drawable.ic_media_play)

            btnPlay.setOnClickListener { onPlay(file) }
            itemView.setOnClickListener { onPlay(file) }
        }

        private fun formatSize(size: Long): String {
            if (size <= 0L) return "0 B"
            val units = arrayOf("B", "KB", "MB", "GB")
            var value = size.toDouble()
            var unitIndex = 0
            while (value >= 1024 && unitIndex < units.lastIndex) {
                value /= 1024
                unitIndex++
            }
            val format = DecimalFormat(if (value >= 100) "#0" else "#0.0")
            return String.format(Locale.US, "%s %s", format.format(value), units[unitIndex])
        }
    }
}

