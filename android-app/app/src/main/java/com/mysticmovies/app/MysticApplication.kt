package com.mysticmovies.app

import android.app.Application
import coil.ImageLoader
import coil.ImageLoaderFactory
import coil.util.DebugLogger

class MysticApplication : Application(), ImageLoaderFactory {
    override fun newImageLoader(): ImageLoader {
        return ImageLoader.Builder(this)
            .okHttpClient(createApiHttpClient())
            .crossfade(true)
            .logger(DebugLogger())
            .build()
    }
}
