package com.mysticmovies.app

import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.ImageView
import android.widget.TextView
import androidx.recyclerview.widget.RecyclerView
import coil.load

data class ProfileEntry(
    val card: CatalogCard,
    val displayTitle: String = "",
    val streamUrl: String = "",
)

class ProfileStripAdapter(
    private val onClick: (ProfileEntry) -> Unit
) : RecyclerView.Adapter<ProfileStripAdapter.ProfileViewHolder>() {
    private val items = mutableListOf<ProfileEntry>()

    fun submit(rows: List<ProfileEntry>) {
        items.clear()
        items.addAll(rows)
        notifyDataSetChanged()
    }

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): ProfileViewHolder {
        val view = LayoutInflater.from(parent.context).inflate(R.layout.item_home_card, parent, false)
        return ProfileViewHolder(view, onClick)
    }

    override fun onBindViewHolder(holder: ProfileViewHolder, position: Int) {
        holder.bind(items[position])
    }

    override fun getItemCount(): Int = items.size

    class ProfileViewHolder(
        itemView: View,
        private val onClick: (ProfileEntry) -> Unit
    ) : RecyclerView.ViewHolder(itemView) {
        private val poster: ImageView = itemView.findViewById(R.id.imgPoster)
        private val title: TextView = itemView.findViewById(R.id.tvTitle)
        private val meta: TextView = itemView.findViewById(R.id.tvMeta)

        fun bind(entry: ProfileEntry) {
            val card = entry.card
            title.text = entry.displayTitle.ifBlank { card.title.ifBlank { "Untitled" } }
            val typeText = if (card.type.equals("series", ignoreCase = true)) "SERIES" else "MOVIE"
            meta.text = listOf(card.year, typeText).filter { it.isNotBlank() }.joinToString(" | ")
            poster.load(resolveImageUrl(card.poster.ifBlank { card.backdrop })) {
                crossfade(true)
                placeholder(android.R.drawable.ic_menu_report_image)
                error(android.R.drawable.ic_menu_report_image)
            }
            itemView.setOnClickListener { onClick(entry) }
        }
    }
}
