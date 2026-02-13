# Android Auto APK Build (Android Studio + GitHub Actions)

This repo now includes CI workflow:

- `.github/workflows/android-apk-build.yml`

It builds APK automatically when code is pushed to `main` (under `android-app/**`) or when manually triggered.

## 1. Android Studio Project Location

A starter Android Studio project is already included at:

- `android-app/`

You can open this directly in Android Studio, or replace it later with your own project while keeping the same folder path.

## 2. Configure Signing (Release APK)

Use one of these templates:

- `android-templates/app-build.gradle.groovy.snippet`
- `android-templates/app-build.gradle.kts.snippet`

Create local file:

- `android-app/key.properties` (from `android-templates/key.properties.example`)

Place keystore:

- `android-app/release.keystore`

Never commit real keystore or real `key.properties`.

## 3. GitHub Secrets

Add these secrets in your repo settings:

- `ANDROID_KEYSTORE_BASE64` (base64 content of `.keystore`)
- `ANDROID_KEYSTORE_PASSWORD`
- `ANDROID_KEY_ALIAS`
- `ANDROID_KEY_PASSWORD`

PowerShell example to generate base64:

```powershell
[Convert]::ToBase64String([IO.File]::ReadAllBytes("C:\path\release.keystore"))
```

## 4. How CI Build Works

Workflow behavior:

1. Validates Android project exists at `android-app`.
2. Sets up Java + Android SDK.
3. If signing secrets exist:
   - writes `android-app/release.keystore`
   - writes `android-app/key.properties`
   - runs `assembleRelease`
4. If secrets missing:
   - runs `assembleDebug`
5. Uploads built APK as GitHub artifact.

Artifact names:

- `mysticmovies-release-apk`
- `mysticmovies-debug-apk`

## 5. Manual Trigger

Use:

- GitHub -> Actions -> `Android APK Build` -> `Run workflow`

## 6. Publish in Your Admin Panel

After downloading APK artifact:

1. Upload APK into your app storage uploader.
2. Open `/app-management`.
3. Select latest APK in `Latest APK File`.
4. Set version/build/update mode and save or publish release.

The app will receive update policy from:

- `GET /app-api/bootstrap`
