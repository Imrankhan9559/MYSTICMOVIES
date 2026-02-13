# MysticMovies Android App (Starter Project)

This folder is a ready Android Studio project.

Current app behavior:

- Native home feed (not WebView wrapper)
- Native content cards + filters + search
- Native detail screen with watch/download/telegram actions
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

Project now includes automatic read-only cleanup before dex/clean tasks (`clearReadOnlyBuildDir`), which reduces this error on OneDrive paths.

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
