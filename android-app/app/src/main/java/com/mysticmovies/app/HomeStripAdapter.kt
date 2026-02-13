package com.mysticmovies.app

import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.ImageView
import android.widget.TextView
import androidx.recyclerview.widget.RecyclerView
import coil.load

class HomeStripAdapter(
    private val items: List<CatalogCard>,
    private val onClick: (CatalogCard) -> Unit
) : RecyclerView.Adapter<HomeStripAdapter.HomeCardViewHolder>() {

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): HomeCardViewHolder {
        val view = LayoutInflater.from(parent.context).inflate(R.layout.item_home_card, parent, false)
        return HomeCardViewHolder(view, onClick)
    }

    override fun onBindViewHolder(holder: HomeCardViewHolder, position: Int) {
        holder.bind(items[position])
    }

    override fun getItemCount(): Int = items.size

    class HomeCardViewHolder(
        itemView: View,
        private val onClick: (CatalogCard) -> Unit
    ) : RecyclerView.ViewHolder(itemView) {
        private val poster: ImageView = itemView.findViewById(R.id.imgPoster)
        private val title: TextView = itemView.findViewById(R.id.tvTitle)
        private val meta: TextView = itemView.findViewById(R.id.tvMeta)

        fun bind(card: CatalogCard) {
            title.text = card.title.ifBlank { "Untitled" }
            val typeText = if (card.type.equals("series", ignoreCase = true)) "SERIES" else "MOVIE"
            meta.text = listOf(card.year, typeText).filter { it.isNotBlank() }.joinToString(" | ")

            val image = card.poster.ifBlank { card.backdrop }
            poster.load(resolveImageUrl(image)) {
                crossfade(true)
                placeholder(android.R.drawable.ic_menu_report_image)
                error(android.R.drawable.ic_menu_report_image)
            }

            itemView.setOnClickListener { onClick(card) }
        }
    }
}
