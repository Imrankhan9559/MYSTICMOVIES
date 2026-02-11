<?php
// =======================================================================
// FILE: proxy_actor.php
// PURPOSE: Final Fix - Path Cleaning & Fallback Redirect
// =======================================================================

// 1. CONFIGURATION
$tmdbApiKey = '840e5bb01c4264285a6217863c3cc306'; 
$fallbackImage = '/image/01.jpg'; // <--- VERIFY THIS FILE EXISTS!

// 2. HELPER: DNS Resolver
function getSecureIP($domain) {
    $providers = [
        "https://dns.google/resolve?name={$domain}&type=A",
        "https://cloudflare-dns.com/dns-query?name={$domain}&type=A&ct=application/dns-json"
    ];
    foreach ($providers as $url) {
        $opts = ["http" => ["method" => "GET", "header" => "Accept: application/dns-json\r\n", "timeout" => 2]];
        $context = stream_context_create($opts);
        $json = @file_get_contents($url, false, $context);
        if ($json) {
            $data = json_decode($json, true);
            if (isset($data['Answer'])) {
                foreach ($data['Answer'] as $record) {
                    if (isset($record['type']) && $record['type'] === 1) return $record['data'];
                }
            }
        }
    }
    return gethostbyname($domain);
}

// 3. HELPER: Secure Fetcher
function fetchSecure($ip, $host, $path) {
    if (!$ip) return false;
    $url = "https://{$host}{$path}"; // Use Hostname for SSL
    
    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, $url);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
    
    // DNS Mapping
    if ($ip && $ip !== $host) {
        curl_setopt($ch, CURLOPT_RESOLVE, ["{$host}:443:{$ip}"]);
    }

    curl_setopt($ch, CURLOPT_HTTPHEADER, ["User-Agent: MysticMovies/1.0"]);
    curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 4); 
    curl_setopt($ch, CURLOPT_TIMEOUT, 6); 
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
    
    $result = curl_exec($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);
    
    if ($httpCode == 200 && $result) return $result;
    return false;
}

// 4. MAIN LOGIC
if (!isset($_GET['action'])) die();

// SEARCH
if ($_GET['action'] === 'search' && !empty($_GET['name'])) {
    header('Content-Type: application/json');
    header("Access-Control-Allow-Origin: *");
    $host = "api.themoviedb.org";
    $ip = getSecureIP($host);
    $path = "/3/search/person?api_key={$tmdbApiKey}&query=" . urlencode($_GET['name']);
    $data = fetchSecure($ip, $host, $path);
    echo $data ? $data : json_encode(['results' => []]);
    exit;
}

// IMAGE
if ($_GET['action'] === 'image' && !empty($_GET['path'])) {
    // FIX: Clean the path to prevent double slashes (e.g., //image.jpg)
    $cleanPath = '/' . ltrim($_GET['path'], '/');
    
    $host = "image.tmdb.org";
    $ip = getSecureIP($host);
    $tmdbPath = "/t/p/w200" . $cleanPath;
    
    $imageData = fetchSecure($ip, $host, $tmdbPath);
    
    if ($imageData) {
        header('Content-Type: image/jpeg');
        header('Cache-Control: public, max-age=86400');
        echo $imageData;
    } else {
        // Redirect to fallback
        header("Location: " . $fallbackImage);
    }
    exit;
}
?>