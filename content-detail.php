<?php
/**
 * ======================================================================================
 * MYSTIC MOVIES | CONTENT DETAIL PAGE | FINAL REVISION
 * ======================================================================================
 */

session_start();

// --------------------------------------------------------------------------------------
// 1. SYSTEM CONFIGURATION & ERROR REPORTING
// --------------------------------------------------------------------------------------
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);
date_default_timezone_set('Asia/Kolkata');

// --------------------------------------------------------------------------------------
// 2. DATABASE CONNECTION
// --------------------------------------------------------------------------------------
if (file_exists('site_connection.php')) {
    include_once 'site_connection.php';
} else {
    // Graceful fallback for testing
    $conn = null; 
}

// --------------------------------------------------------------------------------------
// 3. FETCH CONTENT DATA
// --------------------------------------------------------------------------------------
if (!isset($_GET['slg']) || empty($_GET['slg'])) {
    header("Location: /index");
    exit;
}

$slg = mysqli_real_escape_string($conn, $_GET['slg']);

// Fetch Product
$sql_select = "SELECT * FROM `product` WHERE `slug`='$slg' LIMIT 1";
$data = mysqli_query($conn, $sql_select);

if (!$data || mysqli_num_rows($data) === 0) {
    http_response_code(404);
    echo "<div style='background:#000; color:#fff; height:100vh; display:flex; flex-direction:column; justify-content:center; align-items:center; font-family:sans-serif;'>
            <h1 style='color:#d4af37; font-size:3rem;'>404</h1>
            <p>Content not found.</p>
            <a href='/' style='color:#fff; text-decoration:underline;'>Return Home</a>
          </div>";
    exit;
}

$product = mysqli_fetch_assoc($data);

// --------------------------------------------------------------------------------------
// 4. DATA PROCESSING (IMAGES & HASHTAGS)
// --------------------------------------------------------------------------------------

// Clean Images Array
$images = array_filter([
    $product['image1'] ?? null, 
    $product['image2'] ?? null, 
    $product['image3'] ?? null
]);
// Re-index array so keys are 0,1,2...
$images = array_values($images);
$image_count = count($images);

// Clean Hashtags
$rawTags = $product['hashtag'] ?? '';
$rawTags = str_replace(',', ' ', $rawTags); // Replace commas with spaces just in case
$tagArray = array_filter(explode(' ', $rawTags));

// Prepare SEO Keywords
$cleanKeywords = [];
foreach ($tagArray as $t) {
    $cleanKeywords[] = ltrim($t, '#');
}
$seo_keywords = implode(', ', $cleanKeywords);

// Determine Schema Type
$schemaType = (stripos($product['category'], 'series') !== false) ? 'TVSeries' : 'Movie';

// --------------------------------------------------------------------------------------
// 5. USER AUTHENTICATION
// --------------------------------------------------------------------------------------
$current_url = "https://" . $_SERVER['HTTP_HOST'] . $_SERVER['REQUEST_URI'];
$user_is_logged_in = isset($_SESSION['user_id']);
$user_is_premium = false;
$username = 'Guest';
$user_email = '';

if ($user_is_logged_in) {
    $user_id = $_SESSION['user_id'];
    $user_query = mysqli_query($conn, "SELECT username, email, is_premium, premium_expires_at FROM users WHERE id = '$user_id'");
    
    if ($user_query && $user_data = mysqli_fetch_assoc($user_query)) {
        $username = $user_data['username'];
        $user_email = $user_data['email'];
        
        if ($user_data['is_premium'] == 1) {
            if ($user_data['premium_expires_at'] && strtotime($user_data['premium_expires_at']) > time()) {
                $user_is_premium = true;
            } else {
                mysqli_query($conn, "UPDATE users SET is_premium = 0 WHERE id = '$user_id'");
                $user_is_premium = false;
            }
        }
    }
}

// --------------------------------------------------------------------------------------
// 6. PERMISSION LOGIC
// --------------------------------------------------------------------------------------
$access_granted = false;
$product_user_type = isset($product['user_type']) ? ucfirst(strtolower($product['user_type'])) : 'Normal'; 
$lock_reason = ''; 

switch ($product_user_type) {
    case 'Normal': 
        $access_granted = true; 
        break;
    case 'Login': 
        if ($user_is_logged_in) {
            $access_granted = true;
        } else {
            $access_granted = false;
            $lock_reason = 'login';
        }
        break;
    case 'Premium': 
        if ($user_is_premium) {
            $access_granted = true;
        } else {
            $access_granted = false;
            $lock_reason = 'premium';
        }
        break;
    default:
        $access_granted = true;
}

// --------------------------------------------------------------------------------------
// 7. SETTINGS & DOWNLOAD LOGIC
// --------------------------------------------------------------------------------------
$gen_set = ['live_watch_enabled' => 1, 'tg_tutorial_link' => ''];
$check_table = mysqli_query($conn, "SHOW TABLES LIKE 'general_settings'");

if ($check_table && mysqli_num_rows($check_table) > 0) {
    $gen_set_query = mysqli_query($conn, "SELECT * FROM `general_settings` WHERE id=1");
    if ($gen_set_query && mysqli_num_rows($gen_set_query) > 0) {
        $gen_set = mysqli_fetch_assoc($gen_set_query);
    }
}

$global_live = isset($gen_set['live_watch_enabled']) ? (int)$gen_set['live_watch_enabled'] : 0;
$product_live = isset($product['is_live']) ? (int)$product['is_live'] : 0;
$show_direct_downloads = ($global_live === 1 && $product_live === 1);

// Size Validation
$size_val = isset($product['size']) ? trim($product['size']) : '';
$contains_not_fetch = (stripos($size_val, 'not fetch') !== false); 
$is_size_valid = ($size_val !== '' && !$contains_not_fetch);

// Request URL
$request_query = http_build_query([
    'content_name' => $product['name'],
    'content_link' => $current_url,
    'user_name' => $username,
    'user_email' => $user_email
]);

// Breadcrumbs & Related
$cat_slug = strtolower($product['category']);
$bread_link = (strpos($cat_slug, 'series') !== false) ? "/content/f/series" : "/content/f/movies";
$cat_esc = mysqli_real_escape_string($conn, $product['category']);
$related_data = mysqli_query($conn, "SELECT * FROM `product` WHERE `category`='$cat_esc' AND `slug`!='$slg' ORDER BY release_date DESC LIMIT 8");
?>

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title><?php echo htmlspecialchars($product['name']); ?> | Mystic Movies</title>
    
    <meta name="description" content="<?= htmlspecialchars(strip_tags($product['description'])) ?>">
    <meta name="keywords" content="<?= htmlspecialchars($seo_keywords) ?>">

    <base href="/">
    
    <script type="application/ld+json">
    {
      "@context": "https://schema.org",
      "@type": "<?= $schemaType ?>",
      "name": "<?= htmlspecialchars($product['name']) ?>",
      "image": [
        <?php foreach ($images as $i => $img): ?>
        "https://<?= $_SERVER['HTTP_HOST'] ?>/admin/image/<?= $img ?>"<?= ($i < $image_count-1) ? ',' : '' ?>
        <?php endforeach; ?>
      ],
      "datePublished": "<?= $product['release_date'] ?>",
      "description": "<?= htmlspecialchars(strip_tags($product['description'])) ?>",
      "keywords": "<?= htmlspecialchars($seo_keywords) ?>",
      "aggregateRating": {
        "@type": "AggregateRating",
        "ratingValue": "<?= $product['rating'] ?>",
        "bestRating": "10",
        "ratingCount": "1000"
      }
    }
    </script>

    <link rel="stylesheet" href="/vendor/bootstrap/css/bootstrap.min.css">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@300;400;500;600;700;800&family=Orbitron:wght@400;500;600;700&display=swap" rel="stylesheet">
    
    <style>
        :root {
            --bg-black: #000000;
            --gold: #d4af37;
            --gold-glow: rgba(212, 175, 55, 0.4);
            --dark-bg: #0a0a0a;
        }
        
        body {
            background-color: #000000 !important;
            color: #fff !important;
            font-family: 'Montserrat', sans-serif;
            overflow-x: hidden;
            margin: 0;
            /* Added Top Padding for Fixed Buttons */
            padding-top: 150px;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 0 15px;
        }
        
        /* ===== NEW BREADCRUMB BUTTONS ===== */
        .bread-crumb-container {
            margin-bottom: 30px;
        }

        .bread-crumb {
            display: flex;
            align-items: center;
            flex-wrap: wrap;
            gap: 10px;
            padding: 0;
            background: transparent;
            border: none;
        }

        .bc-btn {
            background: #111;
            border: 1px solid #333;
            padding: 10px 25px;
            border-radius: 50px;
            color: #aaa;
            text-decoration: none;
            font-size: 13px;
            font-weight: 600;
            text-transform: uppercase;
            transition: all 0.3s ease;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .bc-btn:hover {
            border-color: var(--gold);
            color: #fff;
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(212, 175, 55, 0.1);
            text-decoration: none;
        }

        .bc-btn.active {
            background: linear-gradient(45deg, var(--gold), #f1c40f);
            color: #000 !important;
            border-color: var(--gold);
            font-weight: 800;
            box-shadow: 0 0 15px var(--gold-glow);
        }

        .bc-sep {
            color: #444;
            font-size: 12px;
        }
        
        /* Movie Heading */
        .movie-heading {
            font-family: 'Orbitron', sans-serif;
            font-size: 2.5rem;
            color: var(--gold);
            text-align: center;
            margin: 30px 0;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        /* Detail Badges */
        .detail-badge {
            display: inline-block;
            background: #151515;
            border: 1px solid #333;
            color: #ccc;
            padding: 8px 15px;
            margin: 5px;
            border-radius: 20px;
            font-size: 0.9rem;
        }
        
        /* Main Content Row */
        .content-row {
            display: flex;
            flex-wrap: wrap;
            margin: 0 -15px;
        }
        
        /* Image Slider Section (LEFT COLUMN) */
        .image-slider-section {
            flex: 0 0 40%;
            max-width: 40%;
            padding: 0 15px;
        }
        
        @media (max-width: 992px) {
            .image-slider-section {
                flex: 0 0 100%;
                max-width: 100%;
                margin-bottom: 30px;
            }
        }
        
        /* Slider Container */
        .slider-container {
            position: relative;
            width: 100%;
            background: #000;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 5px 15px rgba(0,0,0,0.5);
        }
        
        .slider-wrapper {
            position: relative;
            width: 100%;
            height: 500px;
            overflow: hidden;
        }
        
        .slide {
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            opacity: 0;
            transition: opacity 0.5s ease-in-out;
            z-index: 1;
            pointer-events: none;
        }
        
        .slide.active {
            opacity: 1;
            z-index: 2;
            pointer-events: auto;
        }
        
        .slide img {
            width: 100%;
            height: 100%;
            object-fit: cover;
        }
        
        .slider-btn {
            position: absolute;
            top: 50%;
            transform: translateY(-50%);
            width: 50px;
            height: 50px;
            background: rgba(0,0,0,0.7);
            color: var(--gold);
            border: 2px solid var(--gold);
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            cursor: pointer;
            z-index: 10;
            transition: all 0.3s;
            font-size: 1.2rem;
            user-select: none;
        }
        
        .slider-btn:hover {
            background: var(--gold);
            color: #000;
        }
        
        .slider-btn.prev {
            left: 15px;
        }
        
        .slider-btn.next {
            right: 15px;
        }
        
        .slide-counter {
            position: absolute;
            bottom: 15px;
            right: 15px;
            background: rgba(0,0,0,0.7);
            color: #fff;
            padding: 5px 10px;
            border-radius: 20px;
            font-size: 0.9rem;
            z-index: 10;
        }
        
        .expand-btn {
            position: absolute;
            top: 15px;
            right: 15px;
            width: 40px;
            height: 40px;
            background: rgba(0,0,0,0.7);
            color: #fff;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            text-decoration: none;
            z-index: 10;
            border: 1px solid #444;
            transition: all 0.3s;
        }
        
        .expand-btn:hover {
            background: var(--gold);
            color: #000;
        }
        
        /* Content Details Section (RIGHT COLUMN) */
        .content-details-section {
            flex: 0 0 60%;
            max-width: 60%;
            padding: 0 15px;
        }
        
        @media (max-width: 992px) {
            .content-details-section {
                flex: 0 0 100%;
                max-width: 100%;
            }
        }
        
        .content-info {
            background: #111;
            border-radius: 10px;
            padding: 25px;
            border: 1px solid #333;
            margin-bottom: 20px;
        }
        
        .info-item {
            margin-bottom: 15px;
            padding-bottom: 15px;
            border-bottom: 1px solid #222;
        }
        
        .info-item:last-child {
            border-bottom: none;
            margin-bottom: 0;
            padding-bottom: 0;
        }
        
        .info-label {
            color: var(--gold);
            font-weight: 600;
            margin-bottom: 5px;
        }
        
        .info-value {
            color: #ccc;
            line-height: 1.6;
        }

        /* --- ADDED: Styles for Actors (FIXED BLACK CIRCLE) --- */
        .cast-scroller {
            display: flex; gap: 15px; overflow-x: auto; padding-bottom: 10px;
        }
        .cast-scroller::-webkit-scrollbar { height: 6px; }
        .cast-scroller::-webkit-scrollbar-track { background: #111; }
        .cast-scroller::-webkit-scrollbar-thumb { background: #333; border-radius: 3px; }
        
        .actor-card { flex: 0 0 90px; text-align: center; }
        .actor-card a { text-decoration: none; }
        
        .actor-img-box {
            width: 90px; height: 90px; border-radius: 50%; overflow: hidden;
            margin-bottom: 8px; border: 2px solid #333; transition: all 0.3s;
            position: relative; 
            
            /* FIXED: Use Dark Grey Background AND the Image URL as background */
            /* If the top image fails, this background image shows through immediately */
            background-color: #222;
            background-image: url('/image/01.jpg'); 
            background-size: cover;
            background-position: center;
            background-repeat: no-repeat;
        }
        .actor-card:hover .actor-img-box { border-color: var(--gold); transform: scale(1.05); }
        .actor-img-box img { width: 100%; height: 100%; object-fit: cover; }
        
        .actor-name { color: #ddd; font-size: 0.75rem; line-height: 1.2; font-weight: 500; }
        /* -------------------------------- */
        
        /* Trailer Section (Moved under slider) */
        .trailer-section {
            margin: 30px 0;
        }
        
        .trailer-box {
            position: relative;
            width: 100%;
            padding-bottom: 56.25%; /* 16:9 Aspect Ratio */
            height: 0;
            overflow: hidden;
            border-radius: 10px;
            background: #000;
            border: 1px solid #333;
        }
        
        .trailer-box iframe {
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            border: none;
        }
        
        /* Download Section (Moved under details) */
        .download-section {
            margin: 20px 0;
        }
        
        .status-box {
            background: #111;
            border-radius: 10px;
            padding: 30px;
            text-align: center;
            border: 1px solid #333;
            margin-bottom: 20px;
        }
        
        .status-box.request-mode {
            border-color: #00aaff;
            background: linear-gradient(135deg, #05141a, #000);
        }
        
        .status-box.locked-login {
            border-color: #ff4757;
        }
        
        .status-box.locked-premium {
            border-color: var(--gold);
        }
        
        .status-box.server-down {
            border-color: #ff9f43;
        }
        
        .status-icon {
            font-size: 3rem;
            margin-bottom: 20px;
            display: block;
        }
        
        .status-title {
            font-size: 1.5rem;
            font-weight: 600;
            margin-bottom: 10px;
        }
        
        .status-desc {
            color: #ccc;
            margin-bottom: 20px;
            line-height: 1.6;
        }
        
        .btn-download {
            display: block;
            width: 100%;
            padding: 15px;
            background: linear-gradient(45deg, var(--gold), #f1c40f);
            color: #000 !important;
            border: none;
            border-radius: 50px;
            font-weight: 700;
            text-align: center;
            text-decoration: none;
            margin-bottom: 15px;
            transition: all 0.3s;
            font-size: 1.1rem;
        }
        
        .btn-download:hover {
            transform: translateY(-3px);
            box-shadow: 0 5px 15px rgba(212, 175, 55, 0.3);
            color: #000 !important;
        }
        
        .btn-telegram {
            background: #0088cc !important;
            color: #fff !important;
        }
        
        .btn-tutorial {
            background: #333 !important;
            color: #fff !important;
            border: 1px solid #555 !important;
        }
        
        /* Tabs Section */
        .tabs-section {
            margin: 40px 0;
        }
        
        .tabs-header {
            display: flex;
            border-bottom: 2px solid #333;
            margin-bottom: 20px;
        }
        
        .tab-btn {
            flex: 1;
            background: transparent;
            border: none;
            color: #888;
            padding: 15px 20px;
            font-family: 'Orbitron', sans-serif;
            font-weight: 600;
            cursor: pointer;
            border-bottom: 3px solid transparent;
            transition: all 0.3s;
            font-size: 1rem;
        }
        
        .tab-btn:hover {
            color: #fff;
            background: rgba(255,255,255,0.05);
        }
        
        .tab-btn.active {
            color: var(--gold);
            border-bottom-color: var(--gold);
            background: rgba(212, 175, 55, 0.05);
        }
        
        .tab-content {
            display: none;
            padding: 25px;
            background: #111;
            border-radius: 10px;
            border: 1px solid #333;
            animation: fadeIn 0.5s ease;
        }
        
        .tab-content.active {
            display: block;
        }
        
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }
        
        .description-text {
            color: #ccc;
            line-height: 1.8;
            font-size: 1rem;
        }
        
        .hashtags-container {
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            margin-top: 15px;
        }
        
        .hashtag-pill {
            display: inline-block;
            background: #1a1a1a;
            color: var(--gold);
            padding: 8px 16px;
            border-radius: 20px;
            text-decoration: none;
            border: 1px solid #333;
            transition: all 0.3s;
            font-size: 0.9rem;
        }
        
        .hashtag-pill:hover {
            background: var(--gold);
            color: #000;
            transform: translateY(-2px);
        }
        
        /* Related Content */
        .related-section {
            margin: 50px 0;
            padding-top: 30px;
            border-top: 1px solid #333;
        }
        
        .section-title {
            font-family: 'Orbitron', sans-serif;
            color: var(--gold);
            font-size: 1.8rem;
            margin-bottom: 30px;
            text-align: center;
        }
        
        .related-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
            gap: 20px;
        }
        
        .related-item {
            background: #111;
            border-radius: 10px;
            overflow: hidden;
            border: 1px solid #333;
            transition: all 0.3s;
        }
        
        .related-item:hover {
            transform: translateY(-5px);
            box-shadow: 0 5px 15px rgba(0,0,0,0.3);
            border-color: var(--gold);
        }
        
        .related-link {
            display: block;
            text-decoration: none;
            color: inherit;
        }
        
        .related-image {
            width: 100%;
            height: 250px;
            object-fit: cover;
        }
        
        .related-info {
            padding: 15px;
        }
        
        .related-title {
            color: #fff;
            font-weight: 600;
            margin-bottom: 10px;
            font-size: 1rem;
            line-height: 1.4;
        }
        
        .related-meta {
            display: flex;
            justify-content: space-between;
            color: #888;
            font-size: 0.9rem;
        }
        
        /* Responsive */
        @media (max-width: 768px) {
            .movie-heading {
                font-size: 1.8rem;
            }
            
            .slider-wrapper {
                /* FIXED: INCREASE HEIGHT FOR MOBILE PORTRAIT */
                height: 500px !important; 
            }
            
            .related-grid {
                grid-template-columns: repeat(auto-fill, minmax(150px, 1fr));
            }
            
            .bc-sep { display: none; } 
            .bread-crumb { justify-content: center; }
        }
        
        @media (max-width: 576px) {
            .slider-wrapper {
                height: 480px;
            }
            
            .slider-btn {
                width: 40px;
                height: 40px;
                font-size: 1rem;
            }
            
            .tab-btn {
                padding: 12px 10px;
                font-size: 0.9rem;
            }
            
            .related-grid {
                grid-template-columns: repeat(2, 1fr);
            }
        }
    </style>
</head>

<body>
    <?php if(file_exists('animated-header.php')) include_once 'animated-header.php'; ?>

    <div class="container">
        
        <div class="bread-crumb-container">
            <div class="bread-crumb">
                <a href="/home" class="bc-btn"><i class="fas fa-home"></i> Home</a>
                <i class="fas fa-angle-right bc-sep"></i>
                <a href="/content" class="bc-btn"><i class="fa-solid fa-border-all"></i> Content</a>
                <i class="fas fa-angle-right bc-sep"></i>
                <a href="<?php echo $bread_link; ?>" class="bc-btn"><i class="fas fa-film"></i> <?php echo $product['category']; ?></a>
                <i class="fas fa-angle-right bc-sep"></i>
                <span class="bc-btn active"><?php echo $product['name']; ?></span>
            </div>
        </div>

        <h1 class="movie-heading"><?php echo $product['name']; ?></h1>

        <div class="text-center" style="margin-bottom: 30px;">
            <span class="detail-badge"><i class="fas fa-calendar"></i> <?php echo $product['release_date']; ?></span>
            <?php if($is_size_valid): ?>
            <span class="detail-badge"><i class="fas fa-hdd"></i> <?php echo $product['size']; ?></span>
            <?php else: ?>
            <span class="detail-badge" style="color:#ffcc00;"><i class="fas fa-exclamation-circle"></i> Not Fetched</span>
            <?php endif; ?>
            <span class="detail-badge"><i class="fas fa-star"></i> <?php echo $product['rating']; ?>/10</span>
        </div>

        <div class="content-row">
            <div class="image-slider-section">
                <div class="slider-container">
                    <div class="slider-wrapper" id="sliderWrapper">
                        <?php if($image_count > 0): ?>
                            <?php foreach($images as $key => $img): ?>
                                <div class="slide <?php echo ($key === 0) ? 'active' : ''; ?>" data-index="<?php echo $key; ?>">
                                    <img src="/admin/image/<?php echo $img; ?>" 
                                         onerror="this.src='/images/placeholder.jpg'" 
                                         alt="<?php echo $product['name']; ?> - Image <?php echo $key + 1; ?>">
                                    <a href="/admin/image/<?php echo $img; ?>" class="expand-btn" target="_blank">
                                        <i class="fas fa-expand"></i>
                                    </a>
                                </div>
                            <?php endforeach; ?>
                        <?php else: ?>
                            <div class="slide active">
                                <img src="/images/placeholder.jpg" alt="No Image Available">
                            </div>
                        <?php endif; ?>

                        <?php if($image_count > 1): ?>
                            <div class="slider-btn prev" id="prevBtn">
                                <i class="fas fa-chevron-left"></i>
                            </div>
                            <div class="slider-btn next" id="nextBtn">
                                <i class="fas fa-chevron-right"></i>
                            </div>
                            <div class="slide-counter" id="slideCounter">
                                1 / <?php echo $image_count; ?>
                            </div>
                        <?php endif; ?>
                    </div>
                </div>
                
                <?php if($image_count > 1): ?>
                <div style="text-align: center; margin-top: 15px; color: #666; font-size: 14px; margin-bottom: 25px;">
                    <i class="fas fa-hand-point-up"></i> Click arrows or swipe to navigate
                </div>
                <?php endif; ?>

                <?php if(!empty($product['trailer'])): ?>
                    <div class="trailer-section" style="margin-top: 30px;">
                        <h3 style="color:var(--gold); margin-bottom: 15px;">
                            <i class="fas fa-play-circle"></i> Official Trailer
                        </h3>
                        <div class="trailer-box">
                            <?php 
                            $t_url = $product['trailer'];
                            if (strpos($t_url, 'youtube.com') !== false || strpos($t_url, 'youtu.be') !== false) {
                                if (strpos($t_url, 'embed') === false) {
                                    if (strpos($t_url, 'youtu.be') !== false) {
                                        $video_id = substr(parse_url($t_url, PHP_URL_PATH), 1);
                                    } else {
                                        parse_str(parse_url($t_url, PHP_URL_QUERY), $params);
                                        $video_id = $params['v'] ?? '';
                                    }
                                    $t_url = "https://www.youtube.com/embed/{$video_id}";
                                }
                            }
                            ?>
                            <iframe src="<?php echo $t_url; ?>" 
                                    allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" 
                                    allowfullscreen>
                            </iframe>
                        </div>
                    </div>
                <?php endif; ?>
            </div>

            <div class="content-details-section">
                <div class="content-info">
                    <div class="info-item">
                        <div class="info-label">Director</div>
                        <div id="director-container" class="cast-scroller" data-list="<?php echo htmlspecialchars($product['director']); ?>">
                            <span style="color:#666; font-style:italic;">Loading...</span>
                        </div>
                    </div>
                    <div class="info-item">
                        <div class="info-label">Cast</div>
                        <div id="cast-container" class="cast-scroller" data-list="<?php echo htmlspecialchars($product['cast']); ?>">
                            <span style="color:#666; font-style:italic;">Loading...</span>
                        </div>
                    </div>
                    <div class="info-item">
                        <div class="info-label">Tagline</div>
                        <div class="info-value" style="font-style: italic; color: var(--gold);">"<?php echo $product['one_line_title']; ?>"</div>
                    </div>
                </div>

                <div class="download-section">
                    <?php if (!$is_size_valid): ?>
                        <div class="status-box request-mode">
                            <i class="fas fa-cloud-upload-alt status-icon" style="color:#00aaff;"></i>
                            <div class="status-title" style="color:#00aaff;">File Not Uploaded Yet</div>
                            <div class="status-desc">
                                Admin will upload within 12 hours<br>
                                <small>(Maximum waiting time till now is 2 hours)</small>
                            </div>
                            <div style="margin: 20px 0; color: #00aaff;">
                                <i class="fas fa-chevron-down fa-bounce"></i>
                            </div>
                            <a href="/request-link.php?<?php echo $request_query; ?>" class="btn-download" style="background: linear-gradient(45deg, #00aaff, #0077cc); color:#fff!important;">
                                <i class="fas fa-bell"></i> Request Link Now
                            </a>
                        </div>
                    <?php else: ?>
                        <?php if (!$access_granted): ?>
                            <?php if ($lock_reason == 'login'): ?>
                                <div class="status-box locked-login" style="padding: 40px 30px;">
                                    <div style="margin-bottom: 25px;">
                                        <span style="background: rgba(255, 71, 87, 0.1); color: #ff4757; padding: 20px; border-radius: 50%; display: inline-block; width: 90px; height: 90px; line-height: 50px; box-shadow: 0 0 20px rgba(255, 71, 87, 0.2);">
                                            <i class="fas fa-user-lock" style="font-size: 35px;"></i>
                                        </span>
                                    </div>
                                    <div class="status-title" style="color:#ff4757; font-size: 1.8rem; margin-bottom: 15px;">Login Required</div>
                                    <div class="status-desc" style="color:#ccc; font-size: 15px; margin-bottom: 30px; line-height: 1.6; max-width: 400px; margin-left: auto; margin-right: auto;">
                                        You need to be logged in to access this content.<br>
                                        For the fastest experience, use <strong>Google Login</strong>.
                                    </div>
                                    <a href="/login?return_url=<?= urlencode($current_url) ?>&method=google" class="btn-download" style="background: #fff; color: #333 !important; font-weight: 800; border: none; box-shadow: 0 5px 15px rgba(255,255,255,0.15); display: flex; align-items: center; justify-content: center; gap: 12px; font-size: 16px; padding: 18px;">
                                        <img src="https://www.google.com/favicon.ico" width="20" alt="G"> Continue with Google
                                    </a>
                                    <a href="/login?return_url=<?= urlencode($current_url) ?>" class="btn-download" style="background: transparent; border: 2px solid #333; color: #aaa !important; font-weight: 600; margin-top: 15px; display: flex; align-items: center; justify-content: center; gap: 10px;">
                                        <i class="fas fa-envelope"></i> Email Login
                                    </a>
                                </div>
                            <?php else: ?>
                                <div class="status-box locked-premium">
                                    <i class="fas fa-crown status-icon" style="color:var(--gold);"></i>
                                    <div class="status-title" style="color:var(--gold);">Premium Content</div>
                                    <div class="status-desc">This content is exclusive to Premium members</div>
                                    <a href="/premium" class="btn-download">
                                        <i class="fas fa-gem"></i> Upgrade to Premium
                                    </a>
                                </div>
                            <?php endif; ?>
                        <?php else: ?>
                            <a href="/telegram/<?= $product['slug'] ?>" class="btn-download btn-telegram">
                                <i class="fab fa-telegram"></i> Get File from Telegram
                            </a>
                            <?php if ($show_direct_downloads): ?>
                                <a href="/direct-download/<?= $product['slug'] ?>" class="btn-download">
                                    <i class="fas fa-bolt"></i> Direct Download
                                </a>
                                <a href="/watch-download/<?= $product['slug'] ?>" class="btn-download" style="background:#222; color:#fff!important;">
                                    <i class="fas fa-play"></i> Watch & download
                                </a>
                            <?php else: ?>
                                <div class="status-box server-down">
                                    <i class="fas fa-server status-icon" style="color:#ff9f43;"></i>
                                    <div class="status-title" style="color:#ff9f43;">Server Maintenance</div>
                                    <div class="status-desc">Please use Telegram for download</div>
                                    <?php if(!empty($gen_set['tg_tutorial_link'])): ?>
                                    <a href="<?= $gen_set['tg_tutorial_link'] ?>" target="_blank" class="btn-download btn-tutorial">
                                        <i class="fas fa-play-circle"></i> Watch Tutorial
                                    </a>
                                    <?php endif; ?>
                                </div>
                            <?php endif; ?>
                        <?php endif; ?>
                        <div style="text-align: center; margin-top: 20px;">
                            <a href="/request-link.php?<?php echo $request_query; ?>" 
                               style="color: #666; text-decoration: none; font-size: 14px;">
                                <i class="fas fa-flag"></i> Report Issue / Request 4K
                            </a>
                        </div>
                    <?php endif; ?>
                </div>
            </div>
        </div>

        <div class="tabs-section">
            <div class="tabs-header">
                <button class="tab-btn active" data-tab="description" onclick="window.switchTab('description')">
                    <i class="fas fa-file-alt"></i> Description
                </button>
                <button class="tab-btn" data-tab="hashtags" onclick="window.switchTab('hashtags')">
                    <i class="fas fa-hashtag"></i> Hashtags
                </button>
            </div>
            
            <div id="description" class="tab-content active">
                <div class="description-text">
                    <?php 
                    if(!empty($product['description'])) {
                        echo nl2br(htmlspecialchars($product['description']));
                    } else {
                        echo '<p style="color:#666; text-align:center;">No description available for this content.</p>';
                    }
                    ?>
                </div>
            </div>
            
            <div id="hashtags" class="tab-content">
                <?php if(!empty($tagArray)): ?>
                    <div class="hashtags-container">
                        <?php foreach($tagArray as $tag): ?>
                            <?php 
                                $tag = trim($tag);
                                if(empty($tag)) continue;
                                $tag_link = ltrim($tag, '#');
                            ?>
                            <a href="/content?search_content=<?= urlencode($tag_link) ?>" class="hashtag-pill">
                                <?= htmlspecialchars($tag) ?>
                            </a>
                        <?php endforeach; ?>
                    </div>
                <?php else: ?>
                    <p style="color:#666; text-align:center;">No hashtags available for this content.</p>
                <?php endif; ?>
            </div>
        </div>

        <div class="related-section">
            <h3 class="section-title">You May Also Like</h3>
            
            <div class="related-grid">
                <?php if($related_data && mysqli_num_rows($related_data) > 0): ?>
                    <?php while($rel = mysqli_fetch_assoc($related_data)): ?>
                        <div class="related-item">
                            <a href="/content-detail/<?php echo $rel['slug']; ?>" class="related-link">
                                <img src="/admin/image/<?php echo $rel['image1']; ?>" 
                                     onerror="this.src='/images/placeholder.jpg'" 
                                     alt="<?php echo htmlspecialchars($rel['name']); ?>"
                                     class="related-image">
                                <div class="related-info">
                                    <div class="related-title"><?php echo $rel['name']; ?></div>
                                    <div class="related-meta">
                                        <span><i class="fas fa-star"></i> <?php echo $rel['rating']; ?>/10</span>
                                        <span><i class="fas fa-calendar"></i> <?php echo $rel['release_date']; ?></span>
                                    </div>
                                </div>
                            </a>
                        </div>
                    <?php endwhile; ?>
                <?php else: ?>
                    <div style="grid-column: 1 / -1; text-align: center; padding: 40px; color: #666;">
                        <i class="fas fa-film" style="font-size: 3rem; margin-bottom: 20px;"></i>
                        <p>No related content found.</p>
                    </div>
                <?php endif; ?>
            </div>
        </div>
    </div>

    <?php if(file_exists('footer.php')) include_once 'footer.php'; ?>

    <script>
        // =====================
        // TAB SYSTEM
        // =====================
        window.switchTab = function(tabName) {
            // Hide all contents
            document.querySelectorAll('.tab-content').forEach(el => {
                el.style.display = 'none';
                el.classList.remove('active');
            });
            
            // Deactivate all buttons
            document.querySelectorAll('.tab-btn').forEach(el => {
                el.classList.remove('active');
            });
            
            // Show target
            var activeTab = document.getElementById(tabName);
            if (activeTab) {
                activeTab.style.display = 'block';
                // Small delay for CSS fade effect if needed
                setTimeout(() => activeTab.classList.add('active'), 10);
            }
            
            // Activate button
            var activeBtn = document.querySelector(`.tab-btn[data-tab="${tabName}"]`);
            if (activeBtn) activeBtn.classList.add('active');
        };

        // =====================
        // IMAGE SLIDER (Full Features: Swipe + Autoplay)
        // =====================
        document.addEventListener('DOMContentLoaded', function() {
            const sliderWrapper = document.getElementById('sliderWrapper');
            const slides = document.querySelectorAll('.slide');
            const prevBtn = document.getElementById('prevBtn');
            const nextBtn = document.getElementById('nextBtn');
            const slideCounter = document.getElementById('slideCounter');
            
            let currentSlide = 0;
            const totalSlides = slides.length;
            let slideInterval;

            // 1. Show Slide Function
            function showSlide(index) {
                if (index < 0) index = totalSlides - 1;
                if (index >= totalSlides) index = 0;
                
                currentSlide = index;

                slides.forEach(slide => {
                    slide.classList.remove('active');
                    slide.style.opacity = '0';
                    slide.style.zIndex = '1';
                    slide.style.pointerEvents = 'none';
                });

                slides[currentSlide].classList.add('active');
                slides[currentSlide].style.opacity = '1';
                slides[currentSlide].style.zIndex = '2';
                slides[currentSlide].style.pointerEvents = 'auto';

                if (slideCounter) {
                    slideCounter.textContent = (currentSlide + 1) + ' / ' + totalSlides;
                }
            }

            // 2. Navigation Functions
            function nextSlide() {
                showSlide(currentSlide + 1);
            }
            
            function prevSlide() {
                showSlide(currentSlide - 1);
            }

            // Only initialize if we have slides
            if (totalSlides > 0) {
                showSlide(0);

                if (nextBtn) {
                    nextBtn.addEventListener('click', function(e) {
                        e.preventDefault();
                        e.stopPropagation();
                        nextSlide();
                        resetInterval();
                    });
                }

                if (prevBtn) {
                    prevBtn.addEventListener('click', function(e) {
                        e.preventDefault();
                        e.stopPropagation();
                        prevSlide();
                        resetInterval();
                    });
                }

                // Keyboard
                document.addEventListener('keydown', function(e) {
                    if (e.key === 'ArrowLeft') { prevSlide(); resetInterval(); }
                    if (e.key === 'ArrowRight') { nextSlide(); resetInterval(); }
                });

                // 3. Auto Play Logic
                function startInterval() {
                    if (totalSlides > 1) {
                        slideInterval = setInterval(nextSlide, 5000); // 5 seconds
                    }
                }

                function resetInterval() {
                    clearInterval(slideInterval);
                    startInterval();
                }

                if (sliderWrapper) {
                    sliderWrapper.addEventListener('mouseenter', () => clearInterval(slideInterval));
                    sliderWrapper.addEventListener('mouseleave', startInterval);
                }

                // Start autoplay
                startInterval();

                // 4. Touch / Swipe Logic
                let touchStartX = 0;
                let touchEndX = 0;

                if (sliderWrapper) {
                    sliderWrapper.addEventListener('touchstart', (e) => {
                        touchStartX = e.changedTouches[0].screenX;
                    }, { passive: true });

                    sliderWrapper.addEventListener('touchend', (e) => {
                        touchEndX = e.changedTouches[0].screenX;
                        handleSwipe();
                        resetInterval();
                    }, { passive: true });
                }

                function handleSwipe() {
                    const swipeThreshold = 50;
                    const diff = touchStartX - touchEndX;

                    if (Math.abs(diff) > swipeThreshold) {
                        if (diff > 0) nextSlide();
                        else prevSlide();
                    }
                }
            }

            // Lightbox
            document.querySelectorAll('.expand-btn').forEach(btn => {
                btn.addEventListener('click', function(e) {
                    e.preventDefault();
                    const imgSrc = this.getAttribute('href');
                    openImageInLightbox(imgSrc);
                });
            });
            
            // Hashtag animation
            document.querySelectorAll('.hashtag-pill').forEach(pill => {
                pill.addEventListener('click', function(e) {
                    this.style.transform = 'scale(0.95)';
                    setTimeout(() => { this.style.transform = ''; }, 200);
                });
            });

            // ============================================
            // ADDED: DYNAMIC ACTOR LOADER (With Aggressive Fallback)
            // ============================================
            function loadPeople(containerId) {
                const container = document.getElementById(containerId);
                if(!container) return;
                
                const rawList = container.getAttribute('data-list');
                if(!rawList) { container.innerHTML = '-'; return; }

                const names = rawList.split(',').map(s => s.trim()).filter(s => s.length > 0);
                container.innerHTML = ''; // Clear loading text

                names.forEach(name => {
                    // Use 'search' action via proxy
                    fetch(`proxy_actor.php?action=search&name=${encodeURIComponent(name)}`)
                        .then(res => res.json())
                        .then(data => {
                            // Default is the fallback image
                            let imgSrc = '/image/01.jpg'; 
                            
                            // If proxy found a path, try to use it
                            if (data.results && data.results.length > 0 && data.results[0].profile_path) {
                                const path = data.results[0].profile_path; 
                                imgSrc = `actor&director/image&path${path}`;
                            }

                            const googleLink = `https://www.google.com/search?q=${encodeURIComponent(name)}`;

                            const card = document.createElement('div');
                            card.className = 'actor-card';
                            // Note the added 'onerror' event handler directly in the HTML
                            // FIX: Hide the img tag on error so the CSS background shows through
                            card.innerHTML = `
                                <a href="${googleLink}" target="_blank">
                                    <div class="actor-img-box">
                                        <img src="${imgSrc}" alt="${name}" 
                                             onerror="this.style.display='none'"> 
                                    </div>
                                    <div class="actor-name">${name}</div>
                                </a>
                            `;
                            container.appendChild(card);
                        })
                        .catch(err => {
                            // On total fetch failure, force fallback
                            console.error("Actor fetch error:", err);
                            const card = document.createElement('div');
                            card.className = 'actor-card';
                            card.innerHTML = `
                                <a href="https://www.google.com/search?q=${encodeURIComponent(name)}" target="_blank">
                                    <div class="actor-img-box">
                                        </div>
                                    <div class="actor-name">${name}</div>
                                </a>
                            `;
                            container.appendChild(card);
                        });
                });
            }

            loadPeople('director-container');
            loadPeople('cast-container');
        });
        
        // =====================
        // UTILITY FUNCTIONS
        // =====================
        function openImageInLightbox(imgSrc) {
            const lightbox = document.createElement('div');
            lightbox.style.cssText = `
                position: fixed; top: 0; left: 0; width: 100%; height: 100%;
                background: rgba(0,0,0,0.95); display: flex; justify-content: center;
                align-items: center; z-index: 9999; cursor: pointer;
            `;
            
            const img = document.createElement('img');
            img.src = imgSrc;
            img.style.cssText = `max-width: 90%; max-height: 90%; object-fit: contain;`;
            
            lightbox.appendChild(img);
            document.body.appendChild(lightbox);
            
            lightbox.addEventListener('click', () => document.body.removeChild(lightbox));
        }
    </script>
</body>
</html>