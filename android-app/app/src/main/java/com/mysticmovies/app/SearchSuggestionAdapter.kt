package com.mysticmovies.app

import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.ImageView
import android.widget.TextView
import androidx.recyclerview.widget.RecyclerView
import coil.load

class SearchSuggestionAdapter(
    private val onClick: (CatalogCard) -> Unit
) : RecyclerView.Adapter<SearchSuggestionAdapter.SuggestionViewHolder>() {
    private val items = mutableListOf<CatalogCard>()

    fun submit(rows: List<CatalogCard>) {
        items.clear()
        items.addAll(rows)
        notifyDataSetChanged()
    }

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): SuggestionViewHolder {
        val view = LayoutInflater.from(parent.context).inflate(R.layout.item_search_suggestion, parent, false)
        return SuggestionViewHolder(view, onClick)
    }

    override fun onBindViewHolder(holder: SuggestionViewHolder, position: Int) {
        holder.bind(items[position])
    }

    override fun getItemCount(): Int = items.size

    class SuggestionViewHolder(
        itemView: View,
        private val onClick: (CatalogCard) -> Unit
    ) : RecyclerView.ViewHolder(itemView) {
        private val poster: ImageView = itemView.findViewById(R.id.imgPoster)
        private val title: TextView = itemView.findViewById(R.id.tvTitle)
        private val meta: TextView = itemView.findViewById(R.id.tvMeta)

        fun bind(item: CatalogCard) {
            title.text = item.title.ifBlank { "Untitled" }
            val typeText = if (item.type.equals("series", ignoreCase = true)) "SERIES" else "MOVIE"
            meta.text = listOf(item.year, typeText).filter { it.isNotBlank() }.joinToString(" | ")
            poster.load(resolveImageUrl(item.poster.ifBlank { item.backdrop })) {
                crossfade(true)
                placeholder(android.R.drawable.ic_menu_report_image)
                error(android.R.drawable.ic_menu_report_image)
            }
            itemView.setOnClickListener { onClick(item) }
        }
    }
}
