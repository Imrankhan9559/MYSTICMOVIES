# MysticMovies Android App (Starter Project)

This folder is a ready Android Studio project.

Current app behavior:

- Native home feed (not full WebView wrapper)
- Header/footer + topbar text loaded from `/app-api/bootstrap` (admin-managed)
- Native content cards + filters + search
- Native detail screen with in-app watch/download/telegram actions
- In-app player (Media3 ExoPlayer) for stream/local files
- In-app downloads screen (files stored under app external downloads dir)
- In-app web shell for login/trailer/watch-together pages
- Data source: backend JSON APIs (`/app-api/catalog`, `/app-api/content/{key}`)

## Open in Android Studio

1. Open Android Studio.
2. Click `Open` and select `android-app/`.
3. Let Gradle sync.
4. Run `app` on emulator/device.

## Build APK Locally

```bash
./gradlew assembleDebug
```

On Windows PowerShell:

```powershell
.\gradlew.bat assembleDebug
```

If Windows/OneDrive file lock error appears (Unable to delete directory / AccessDeniedException), run:

```powershell
.\fix-build-lock.ps1
```

Project now includes:
- automatic read-only cleanup before Kotlin/dex/clean tasks (`clearReadOnlyBuildDir`)
- non-incremental Kotlin compile settings to reduce snapshot lock issues on OneDrive
- retry + daemon kill logic in `fix-build-lock.ps1`

## Release Signing (Optional)

1. Copy `android-templates/key.properties.example` to `android-app/key.properties`.
2. Put keystore at `android-app/release.keystore`.
3. Update `key.properties` values.
4. Build release:

```bash
./gradlew assembleRelease
```

## API Base URL

Configured in `android-app/app/build.gradle`:

- `BuildConfig.API_BASE_URL = "https://mysticmovies.onrender.com"`

Change it if your backend URL changes.

## Java / Gradle Note

If Android Studio/Gradle says `JAVA_HOME is not set`, set `JAVA_HOME` to your JDK path (JDK 17+ recommended), then restart terminal/Android Studio.
