package com.mysticmovies.app

import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.ImageView
import android.widget.TextView
import androidx.recyclerview.widget.RecyclerView
import coil.load

data class CastProfileCard(
    val name: String,
    val role: String,
    val image: String,
    val castPath: String,
)

class CastStripAdapter(
    private val items: List<CastProfileCard>,
    private val onClick: (CastProfileCard) -> Unit
) : RecyclerView.Adapter<CastStripAdapter.CastViewHolder>() {

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): CastViewHolder {
        val view = LayoutInflater.from(parent.context).inflate(R.layout.item_cast_card, parent, false)
        return CastViewHolder(view, onClick)
    }

    override fun onBindViewHolder(holder: CastViewHolder, position: Int) {
        holder.bind(items[position])
    }

    override fun getItemCount(): Int = items.size

    class CastViewHolder(
        itemView: View,
        private val onClick: (CastProfileCard) -> Unit
    ) : RecyclerView.ViewHolder(itemView) {
        private val image: ImageView = itemView.findViewById(R.id.imgCast)
        private val name: TextView = itemView.findViewById(R.id.tvCastName)
        private val role: TextView = itemView.findViewById(R.id.tvCastRole)

        fun bind(card: CastProfileCard) {
            name.text = card.name.ifBlank { "Unknown" }
            role.text = card.role.ifBlank { "View profile" }
            image.load(resolveImageUrl(card.image)) {
                crossfade(true)
                placeholder(android.R.drawable.sym_def_app_icon)
                error(android.R.drawable.sym_def_app_icon)
            }
            itemView.setOnClickListener { onClick(card) }
        }
    }
}
