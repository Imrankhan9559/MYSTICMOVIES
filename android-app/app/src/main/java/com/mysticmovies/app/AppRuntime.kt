package com.mysticmovies.app

import org.json.JSONArray
import org.json.JSONObject

data class UiMenuItem(
    val label: String = "",
    val url: String = "",
    val icon: String = "",
)

data class UiConfig(
    val siteName: String = "mysticmovies",
    val footerText: String = "MysticMovies",
    val topbarText: String = "Welcome to Mystic Movies",
    val logoUrl: String = "",
    val headerMenu: List<UiMenuItem> = emptyList(),
    val footerExploreLinks: List<UiMenuItem> = emptyList(),
    val footerSupportLinks: List<UiMenuItem> = emptyList(),
    val footerAboutText: String = "",
)

data class UpdateConfig(
    val mode: String = "none",
    val forceRequired: Boolean = false,
    val recommend: Boolean = false,
    val latestVersion: String = "",
    val latestBuild: Int = 0,
    val releaseNotes: String = "",
    val title: String = "Update Available",
    val body: String = "A new app version is available.",
    val apkDownloadUrl: String = "",
)

object AppRuntimeState {
    @Volatile
    var apiBaseUrl: String = BuildConfig.API_BASE_URL.trimEnd('/')

    @Volatile
    var handshakeToken: String = ""

    @Volatile
    var appName: String = "MysticMovies"

    @Volatile
    var telegramBotUsername: String = ""

    @Volatile
    var keepaliveOnLaunch: Boolean = true

    @Volatile
    var maintenanceMode: Boolean = false

    @Volatile
    var maintenanceMessage: String = ""

    @Volatile
    var splashImageUrl: String = ""

    @Volatile
    var loadingIconUrl: String = ""

    @Volatile
    var adsMessage: String = ""

    @Volatile
    var ui: UiConfig = UiConfig()

    @Volatile
    var update: UpdateConfig = UpdateConfig()

    @Volatile
    var notifications: List<String> = emptyList()

    fun applyBootstrap(baseUrl: String, root: JSONObject) {
        apiBaseUrl = baseUrl.trimEnd('/')

        val app = root.optJSONObject("app") ?: JSONObject()
        appName = app.optString("name").ifBlank { "MysticMovies" }
        keepaliveOnLaunch = app.optBoolean("keepalive_on_launch", true)
        maintenanceMode = app.optBoolean("maintenance_mode", false)
        maintenanceMessage = app.optString("maintenance_message")
        splashImageUrl = app.optString("splash_image_original")
            .ifBlank { app.optString("splash_image_url") }
        loadingIconUrl = app.optString("loading_icon_original")
            .ifBlank { app.optString("loading_icon_url") }
        adsMessage = app.optString("ads_message")

        val telegram = root.optJSONObject("telegram") ?: JSONObject()
        telegramBotUsername = telegram.optString("bot_username").trim().removePrefix("@")

        val uiRoot = root.optJSONObject("ui") ?: JSONObject()
        ui = UiConfig(
            siteName = uiRoot.optString("site_name").ifBlank { "mysticmovies" },
            footerText = uiRoot.optString("footer_text").ifBlank { "MysticMovies" },
            topbarText = uiRoot.optString("topbar_text").ifBlank { "Welcome to Mystic Movies" },
            logoUrl = uiRoot.optString("logo_original").ifBlank { uiRoot.optString("logo_url") },
            headerMenu = readMenu(uiRoot.optJSONArray("header_menu")),
            footerExploreLinks = readMenu(uiRoot.optJSONArray("footer_explore_links")),
            footerSupportLinks = readMenu(uiRoot.optJSONArray("footer_support_links")),
            footerAboutText = uiRoot.optString("footer_about_text"),
        )

        val updateRoot = root.optJSONObject("update") ?: JSONObject()
        update = UpdateConfig(
            mode = updateRoot.optString("mode").ifBlank { "none" },
            forceRequired = updateRoot.optBoolean("force_required", false),
            recommend = updateRoot.optBoolean("recommend", false),
            latestVersion = updateRoot.optString("latest_version"),
            latestBuild = updateRoot.optInt("latest_build", 0),
            releaseNotes = updateRoot.optString("release_notes"),
            title = updateRoot.optString("update_popup_title").ifBlank { "Update Available" },
            body = updateRoot.optString("update_popup_body").ifBlank { "A new app version is available." },
            apkDownloadUrl = updateRoot.optString("apk_download_url"),
        )

        val notiRows = mutableListOf<String>()
        val notificationsArray = root.optJSONArray("notifications") ?: JSONArray()
        for (i in 0 until notificationsArray.length()) {
            val row = notificationsArray.optJSONObject(i) ?: continue
            val title = row.optString("title").trim()
            val msg = row.optString("message").trim()
            if (title.isBlank() && msg.isBlank()) continue
            if (title.isBlank()) {
                notiRows.add(msg)
            } else if (msg.isBlank()) {
                notiRows.add(title)
            } else {
                notiRows.add("$title: $msg")
            }
        }
        notifications = notiRows
    }

    private fun readMenu(array: JSONArray?): List<UiMenuItem> {
        if (array == null) return emptyList()
        val rows = mutableListOf<UiMenuItem>()
        for (i in 0 until array.length()) {
            val row = array.optJSONObject(i) ?: continue
            rows.add(
                UiMenuItem(
                    label = row.optString("label"),
                    url = row.optString("url"),
                    icon = row.optString("icon"),
                )
            )
        }
        return rows
    }
}
