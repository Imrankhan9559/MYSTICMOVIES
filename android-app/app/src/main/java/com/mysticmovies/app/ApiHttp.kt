package com.mysticmovies.app

import okhttp3.Dns
import okhttp3.OkHttpClient
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import java.net.URI
import java.net.InetAddress
import java.net.UnknownHostException
import java.util.Locale
import java.util.concurrent.TimeUnit

private val hostIpFallbacks: Map<String, List<String>> = mapOf(
    // Render + Cloudflare edge IPs for mysticmovies.onrender.com
    "mysticmovies.onrender.com" to listOf("216.24.57.7", "216.24.57.251")
)

private val appDns = object : Dns {
    override fun lookup(hostname: String): List<InetAddress> {
        val host = hostname.trim().lowercase(Locale.US)
        if (host.isBlank()) throw UnknownHostException(hostname)

        try {
            return Dns.SYSTEM.lookup(host)
        } catch (_: UnknownHostException) {
            val fallbackIps = hostIpFallbacks[host].orEmpty()
            if (fallbackIps.isEmpty()) {
                throw UnknownHostException(host)
            }
            val resolved = fallbackIps.mapNotNull { ip ->
                try {
                    InetAddress.getByName(ip)
                } catch (_: Exception) {
                    null
                }
            }
            if (resolved.isEmpty()) throw UnknownHostException(host)
            return resolved
        }
    }
}

fun createApiHttpClient(): OkHttpClient {
    return OkHttpClient.Builder()
        .dns(appDns)
        .connectTimeout(20, TimeUnit.SECONDS)
        .readTimeout(30, TimeUnit.SECONDS)
        .writeTimeout(20, TimeUnit.SECONDS)
        .build()
}

fun apiBaseCandidates(): List<String> {
    val rows = linkedSetOf(
        AppRuntimeState.apiBaseUrl.trimEnd('/'),
        BuildConfig.API_BASE_URL.trimEnd('/'),
        "https://mysticmovies.onrender.com",
    )
    return rows.filter { it.isNotBlank() }
}

fun absoluteUrl(raw: String): String {
    val cleaned = raw.trim()
    if (cleaned.isBlank()) return ""
    if (cleaned.startsWith("http://") || cleaned.startsWith("https://")) {
        return cleaned
    }
    val base = AppRuntimeState.apiBaseUrl.trimEnd('/')
    val path = if (cleaned.startsWith("/")) cleaned else "/$cleaned"
    return "$base$path"
}

fun parseShareToken(pathOrUrl: String, expectedPrefix: String): String {
    val cleaned = pathOrUrl.trim()
    if (cleaned.isBlank()) return ""
    val path = try {
        if (cleaned.startsWith("http://") || cleaned.startsWith("https://")) {
            URI(cleaned).path.orEmpty()
        } else {
            cleaned.substringBefore("?")
        }
    } catch (_: Exception) {
        cleaned.substringBefore("?")
    }
    val normalized = path.trim()
    val prefix = "/$expectedPrefix/"
    if (!normalized.contains(prefix)) return ""
    return normalized.substringAfter(prefix).substringBefore("/").trim()
}

fun extractContentKeyFromPath(path: String): String {
    val cleaned = path.trim()
    if (cleaned.isBlank()) return ""
    return cleaned.substringAfterLast("/").substringBefore("?").trim()
}

fun appendQuery(url: String, key: String, value: String): String {
    val cleanedUrl = url.trim()
    if (cleanedUrl.isBlank() || key.isBlank() || value.isBlank()) return cleanedUrl
    val parsed = cleanedUrl.toHttpUrlOrNull()
    if (parsed != null) {
        return parsed.newBuilder().addQueryParameter(key, value).build().toString()
    }
    val separator = if (cleanedUrl.contains("?")) "&" else "?"
    return "$cleanedUrl$separator$key=$value"
}
