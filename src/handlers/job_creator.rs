use axum::response::Html;

const GEMINI_GEM_URL: &str =
    "https://gemini.google.com/gem/1hdu25_T_4pcvjZnj-wuGzN2Q-HC1Q7D4?usp=sharing";

/// 求人作成タブ - Gemini Gem（求人原稿改善AI）への導線
pub async fn tab_job_creator() -> Html<String> {
    Html(format!(
        r#"
<div class="p-6">
    <h2 class="text-xl font-bold text-gray-100 mb-4">✏️ 求人原稿の改善</h2>

    <div class="bg-gray-800 rounded-lg p-4 mb-4">
        <p class="text-gray-300 mb-2">
            <svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M12 18v-5.25m0 0a6.01 6.01 0 001.5-.189m-1.5.189a6.01 6.01 0 01-1.5-.189m3.75 7.478a12.06 12.06 0 01-4.5 0m3.75 2.383a14.406 14.406 0 01-3 0M14.25 18v-.192c0-.983.658-1.823 1.508-2.316a7.5 7.5 0 10-7.517 0c.85.493 1.509 1.333 1.509 2.316V18'/></svg>  このツールは <strong>Gemini AI</strong> を使って求人原稿を分析・改善します。
        </p>
        <p class="text-gray-400 text-sm mb-3">
            求人原稿のテキストを貼り付けると、改善提案や書き直し案を受け取れます。
        </p>
        <a href="{url}" target="_blank" rel="noopener noreferrer"
           class="inline-flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-500 text-white rounded-lg transition-colors text-sm font-medium">
            🔗 別ウィンドウで開く
        </a>
    </div>

    <div id="iframe-container" class="bg-gray-900 rounded-lg overflow-hidden border border-gray-700">
        <iframe id="gemini-iframe"
                src="{url}"
                style="width: 100%; height: 80vh; border: none;"
                sandbox="allow-scripts allow-same-origin allow-popups allow-forms"
                referrerpolicy="no-referrer"
                loading="lazy">
        </iframe>
        <div id="iframe-fallback" style="display: none;" class="p-8 text-center">
            <p class="text-gray-400 text-lg mb-4">
                ⚠️ iframe での表示がブロックされています
            </p>
            <p class="text-gray-500 mb-6">
                Gemini はセキュリティ上の理由で iframe 埋め込みを制限している場合があります。<br>
                下のボタンから別ウィンドウで開いてご利用ください。
            </p>
            <a href="{url}" target="_blank" rel="noopener noreferrer"
               class="inline-flex items-center gap-2 px-6 py-3 bg-blue-600 hover:bg-blue-500 text-white rounded-lg transition-colors font-medium">
                🔗 Gemini AI を開く
            </a>
        </div>
    </div>
</div>

<script>
(function() {{
    var iframe = document.getElementById('gemini-iframe');
    var fallback = document.getElementById('iframe-fallback');
    if (!iframe || !fallback) return;

    // iframe 読み込みエラー検出
    iframe.addEventListener('error', function() {{
        iframe.style.display = 'none';
        fallback.style.display = 'block';
    }});

    // タイムアウトによるフォールバック（3秒）
    setTimeout(function() {{
        try {{
            var doc = iframe.contentDocument || iframe.contentWindow.document;
            if (!doc || !doc.body || doc.body.innerHTML === '') {{
                iframe.style.display = 'none';
                fallback.style.display = 'block';
            }}
        }} catch (e) {{
            // クロスオリジンエラー = コンテンツ読み込み済み（正常）
        }}
    }}, 3000);
}})();
</script>
"#,
        url = GEMINI_GEM_URL
    ))
}
