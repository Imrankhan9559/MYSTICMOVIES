package com.mysticmovies.app

import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.ImageView
import android.widget.TextView
import androidx.recyclerview.widget.RecyclerView
import coil.load

data class CatalogCard(
    val id: String,
    val slug: String,
    val title: String,
    val year: String,
    val type: String,
    val poster: String,
    val backdrop: String,
    val qualityRow: List<String>,
    val seasonText: String,
)

class CatalogAdapter(
    private val onClick: (CatalogCard) -> Unit
) : RecyclerView.Adapter<CatalogAdapter.CatalogViewHolder>() {

    private val items = mutableListOf<CatalogCard>()

    fun submitItems(rows: List<CatalogCard>) {
        items.clear()
        items.addAll(rows)
        notifyDataSetChanged()
    }

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): CatalogViewHolder {
        val view = LayoutInflater.from(parent.context).inflate(R.layout.item_catalog_card, parent, false)
        return CatalogViewHolder(view, onClick)
    }

    override fun onBindViewHolder(holder: CatalogViewHolder, position: Int) {
        holder.bind(items[position])
    }

    override fun getItemCount(): Int = items.size

    class CatalogViewHolder(
        itemView: View,
        private val onClick: (CatalogCard) -> Unit
    ) : RecyclerView.ViewHolder(itemView) {
        private val poster: ImageView = itemView.findViewById(R.id.imgPoster)
        private val title: TextView = itemView.findViewById(R.id.tvTitle)
        private val meta: TextView = itemView.findViewById(R.id.tvMeta)
        private val badges: TextView = itemView.findViewById(R.id.tvBadges)

        fun bind(card: CatalogCard) {
            title.text = card.title.ifBlank { "Untitled" }
            val typeLabel = if (card.type.equals("series", ignoreCase = true)) "WEB SERIES" else "MOVIE"
            meta.text = if (card.year.isNotBlank()) "${card.year} | $typeLabel" else typeLabel
            badges.text = if (card.type.equals("series", ignoreCase = true)) {
                card.seasonText.ifBlank { "Season info unavailable" }
            } else {
                compactQualityText(card.qualityRow)
            }

            val imageUrl = card.poster.ifBlank { card.backdrop }
            poster.load(imageUrl) {
                crossfade(true)
                placeholder(android.R.drawable.ic_menu_report_image)
                error(android.R.drawable.ic_menu_report_image)
            }

            itemView.setOnClickListener {
                onClick(card)
            }
        }

        private fun compactQualityText(rows: List<String>): String {
            if (rows.isEmpty()) return "HD"
            if (rows.size <= 4) return rows.joinToString(" • ")
            val visible = rows.take(4).joinToString(" • ")
            val more = rows.size - 4
            return "$visible +$more"
        }
    }
}
