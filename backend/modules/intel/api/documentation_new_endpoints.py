"""
API Documentation - New Endpoints
==================================
Activities, Intel Feed, Projects Extended, Unlocks Calendar
"""

from .documentation_registry import ApiEndpoint, ApiParameter, ApiResponse, HttpMethod

# ═══════════════════════════════════════════════════════════════
# CRYPTO ACTIVITIES API
# ═══════════════════════════════════════════════════════════════

ACTIVITIES_DOCUMENTATION = [
    ApiEndpoint(
        endpoint_id="activities_list",
        path="/api/activities",
        method=HttpMethod.GET,
        title_en="List Crypto Activities",
        title_ru="Список криптоактивностей",
        description_en="Get all crypto activities with filters. Includes airdrops, campaigns, testnets, listings, launches.",
        description_ru="Получить все криптоактивности с фильтрами. Включает airdrop'ы, кампании, тестнеты, листинги, запуски.",
        category="activities",
        tags=["activities", "airdrops", "campaigns", "testnets"],
        parameters=[
            ApiParameter(name="category", type="string", required=False, description_en="Filter by category: launch, campaign, exchange, ecosystem, community, development", description_ru="Фильтр по категории"),
            ApiParameter(name="type", type="string", required=False, description_en="Filter by type: airdrop, launchpool, testnet, points_program, listing", description_ru="Фильтр по типу"),
            ApiParameter(name="status", type="string", required=False, description_en="Filter: upcoming, active, ended", description_ru="Статус: upcoming, active, ended"),
            ApiParameter(name="project", type="string", required=False, description_en="Filter by project ID/name", description_ru="Фильтр по проекту"),
            ApiParameter(name="chain", type="string", required=False, description_en="Filter by blockchain", description_ru="Фильтр по блокчейну"),
            ApiParameter(name="page", type="integer", required=False, default=1, description_en="Page number", description_ru="Номер страницы"),
            ApiParameter(name="limit", type="integer", required=False, default=20, description_en="Items per page", description_ru="Элементов на странице"),
        ],
        responses=[
            ApiResponse(status_code=200, description_en="List of activities", description_ru="Список активностей",
                       example={"ts": 1234567890, "total": 10, "items": [{"id": "...", "title": "LayerZero Airdrop", "type": "airdrop", "score": 85}]})
        ]
    ),
    
    ApiEndpoint(
        endpoint_id="activities_active",
        path="/api/activities/active",
        method=HttpMethod.GET,
        title_en="Active Activities",
        title_ru="Активные активности",
        description_en="Get currently active activities (started, not ended yet).",
        description_ru="Получить текущие активные активности (начавшиеся, не завершённые).",
        category="activities",
        tags=["activities", "active"],
        parameters=[
            ApiParameter(name="category", type="string", required=False, description_en="Filter by category", description_ru="Фильтр по категории"),
            ApiParameter(name="limit", type="integer", required=False, default=50, description_en="Limit results", description_ru="Лимит результатов"),
        ],
        responses=[
            ApiResponse(status_code=200, description_en="Active activities", description_ru="Активные активности")
        ]
    ),
    
    ApiEndpoint(
        endpoint_id="activities_upcoming",
        path="/api/activities/upcoming",
        method=HttpMethod.GET,
        title_en="Upcoming Activities",
        title_ru="Предстоящие активности",
        description_en="Get upcoming activities (not yet started).",
        description_ru="Получить предстоящие активности (ещё не начавшиеся).",
        category="activities",
        tags=["activities", "upcoming"],
        parameters=[
            ApiParameter(name="days", type="integer", required=False, default=30, description_en="Days ahead (1-90)", description_ru="Дней вперёд"),
            ApiParameter(name="category", type="string", required=False, description_en="Filter by category", description_ru="Фильтр по категории"),
            ApiParameter(name="limit", type="integer", required=False, default=50, description_en="Limit results", description_ru="Лимит результатов"),
        ],
        responses=[
            ApiResponse(status_code=200, description_en="Upcoming activities", description_ru="Предстоящие активности")
        ]
    ),
    
    ApiEndpoint(
        endpoint_id="activities_trending",
        path="/api/activities/trending",
        method=HttpMethod.GET,
        title_en="Trending Activities",
        title_ru="Трендовые активности",
        description_en="Get trending activities by score and engagement.",
        description_ru="Получить трендовые активности по рейтингу и вовлечённости.",
        category="activities",
        tags=["activities", "trending"],
        parameters=[
            ApiParameter(name="period", type="string", required=False, default="week", description_en="Period: day, week, month", description_ru="Период: day, week, month"),
            ApiParameter(name="limit", type="integer", required=False, default=20, description_en="Limit results", description_ru="Лимит результатов"),
        ],
        responses=[
            ApiResponse(status_code=200, description_en="Trending activities ranked by score", description_ru="Трендовые активности по рейтингу")
        ]
    ),
    
    ApiEndpoint(
        endpoint_id="activities_campaigns",
        path="/api/activities/campaigns",
        method=HttpMethod.GET,
        title_en="Active Campaigns",
        title_ru="Активные кампании",
        description_en="Get active campaigns: airdrops, points programs, quests, testnets.",
        description_ru="Получить активные кампании: airdrop'ы, программы поинтов, квесты, тестнеты.",
        category="activities",
        tags=["campaigns", "airdrops", "points", "quests"],
        parameters=[
            ApiParameter(name="type", type="string", required=False, description_en="Filter: airdrop, points_program, quest, testnet", description_ru="Тип кампании"),
            ApiParameter(name="status", type="string", required=False, default="active", description_en="Status: active, upcoming, all", description_ru="Статус"),
            ApiParameter(name="chain", type="string", required=False, description_en="Filter by blockchain", description_ru="Фильтр по блокчейну"),
            ApiParameter(name="difficulty", type="string", required=False, description_en="Filter: easy, medium, hard", description_ru="Сложность"),
            ApiParameter(name="limit", type="integer", required=False, default=50, description_en="Limit results", description_ru="Лимит"),
        ],
        responses=[
            ApiResponse(status_code=200, description_en="Active campaigns with rewards and difficulty", description_ru="Активные кампании с наградами и сложностью")
        ]
    ),
    
    ApiEndpoint(
        endpoint_id="activities_project",
        path="/api/activities/project/{project_id}",
        method=HttpMethod.GET,
        title_en="Project Activities",
        title_ru="Активности проекта",
        description_en="Get all activities for a specific project.",
        description_ru="Получить все активности для конкретного проекта.",
        category="activities",
        tags=["activities", "project"],
        parameters=[
            ApiParameter(name="project_id", type="string", location="path", required=True, description_en="Project ID or slug", description_ru="ID или slug проекта"),
            ApiParameter(name="status", type="string", required=False, description_en="Filter: active, upcoming, ended", description_ru="Статус"),
            ApiParameter(name="limit", type="integer", required=False, default=20, description_en="Limit results", description_ru="Лимит"),
        ],
        responses=[
            ApiResponse(status_code=200, description_en="Project activities", description_ru="Активности проекта")
        ]
    ),
    
    ApiEndpoint(
        endpoint_id="activities_detail",
        path="/api/activities/{activity_id}",
        method=HttpMethod.GET,
        title_en="Activity Detail",
        title_ru="Детали активности",
        description_en="Get detailed information about a specific activity.",
        description_ru="Получить детальную информацию об активности.",
        category="activities",
        tags=["activities", "detail"],
        parameters=[
            ApiParameter(name="activity_id", type="string", location="path", required=True, description_en="Activity ID", description_ru="ID активности"),
        ],
        responses=[
            ApiResponse(status_code=200, description_en="Activity details", description_ru="Детали активности"),
            ApiResponse(status_code=404, description_en="Activity not found", description_ru="Активность не найдена")
        ]
    ),
]


# ═══════════════════════════════════════════════════════════════
# INTEL FEED API
# ═══════════════════════════════════════════════════════════════

INTEL_FEED_DOCUMENTATION = [
    ApiEndpoint(
        endpoint_id="intel_feed_main",
        path="/api/intel-feed",
        method=HttpMethod.GET,
        title_en="Intel Feed - Unified Event Stream",
        title_ru="Intel Feed - Единый поток событий",
        description_en="Get unified intel feed combining funding rounds, activities, unlocks, news, listings, and launches.",
        description_ru="Получить единый поток событий: раунды финансирования, активности, анлоки, новости, листинги, запуски.",
        category="feed",
        tags=["feed", "intel", "unified", "funding", "activities", "unlocks"],
        parameters=[
            ApiParameter(name="types", type="string", required=False, description_en="Filter types (comma-sep): funding,activity,unlock,news,listing,launch", description_ru="Типы событий"),
            ApiParameter(name="project", type="string", required=False, description_en="Filter by project", description_ru="Фильтр по проекту"),
            ApiParameter(name="investor", type="string", required=False, description_en="Filter by investor", description_ru="Фильтр по инвестору"),
            ApiParameter(name="importance", type="string", required=False, description_en="Filter: high, medium, low", description_ru="Важность"),
            ApiParameter(name="page", type="integer", required=False, default=1, description_en="Page number", description_ru="Номер страницы"),
            ApiParameter(name="limit", type="integer", required=False, default=30, description_en="Items per page", description_ru="Элементов на странице"),
        ],
        responses=[
            ApiResponse(status_code=200, description_en="Unified intel feed with all event types", description_ru="Единый поток событий всех типов",
                       example={"ts": 1234567890, "total": 50, "types_available": ["funding", "activity", "unlock"], "items": []})
        ]
    ),
    
    ApiEndpoint(
        endpoint_id="intel_feed_trending",
        path="/api/intel-feed/trending",
        method=HttpMethod.GET,
        title_en="Trending Intel Events",
        title_ru="Трендовые события",
        description_en="Get trending intel events by score and recency.",
        description_ru="Получить трендовые события по рейтингу и актуальности.",
        category="feed",
        tags=["feed", "trending"],
        parameters=[
            ApiParameter(name="period", type="string", required=False, default="day", description_en="Period: day, week, month", description_ru="Период"),
            ApiParameter(name="limit", type="integer", required=False, default=20, description_en="Limit results", description_ru="Лимит"),
        ],
        responses=[
            ApiResponse(status_code=200, description_en="Trending events", description_ru="Трендовые события")
        ]
    ),
]


# ═══════════════════════════════════════════════════════════════
# PROJECTS EXTENDED API
# ═══════════════════════════════════════════════════════════════

PROJECTS_EXTENDED_DOCUMENTATION = [
    ApiEndpoint(
        endpoint_id="project_about",
        path="/api/projects/{project_id}/about",
        method=HttpMethod.GET,
        title_en="Project About",
        title_ru="О проекте",
        description_en="Get project about information: description, technology, consensus, whitepaper.",
        description_ru="Получить информацию о проекте: описание, технология, консенсус, whitepaper.",
        category="projects",
        tags=["projects", "about", "profile"],
        parameters=[
            ApiParameter(name="project_id", type="string", location="path", required=True, description_en="Project ID, slug, or symbol", description_ru="ID, slug или символ проекта"),
        ],
        responses=[
            ApiResponse(status_code=200, description_en="Project about information", description_ru="Информация о проекте"),
            ApiResponse(status_code=404, description_en="Project not found", description_ru="Проект не найден")
        ]
    ),
    
    ApiEndpoint(
        endpoint_id="project_links",
        path="/api/projects/{project_id}/links",
        method=HttpMethod.GET,
        title_en="Project Official Links",
        title_ru="Официальные ссылки проекта",
        description_en="Get project official links: website, Twitter, Discord, GitHub, Telegram, etc.",
        description_ru="Получить официальные ссылки проекта: сайт, Twitter, Discord, GitHub, Telegram и т.д.",
        category="projects",
        tags=["projects", "links", "social"],
        parameters=[
            ApiParameter(name="project_id", type="string", location="path", required=True, description_en="Project ID, slug, or symbol", description_ru="ID, slug или символ проекта"),
        ],
        responses=[
            ApiResponse(status_code=200, description_en="Project official links", description_ru="Официальные ссылки проекта",
                       example={"links": {"website": "https://...", "twitter": "https://twitter.com/...", "discord": "https://discord.gg/..."}})
        ]
    ),
    
    ApiEndpoint(
        endpoint_id="project_explorers",
        path="/api/projects/{project_id}/explorers",
        method=HttpMethod.GET,
        title_en="Project Blockchain Explorers",
        title_ru="Обозреватели блокчейна проекта",
        description_en="Get blockchain explorers for project: Etherscan, Solscan, etc.",
        description_ru="Получить обозреватели блокчейна проекта: Etherscan, Solscan и т.д.",
        category="projects",
        tags=["projects", "explorers", "blockchain"],
        parameters=[
            ApiParameter(name="project_id", type="string", location="path", required=True, description_en="Project ID, slug, or symbol", description_ru="ID, slug или символ проекта"),
        ],
        responses=[
            ApiResponse(status_code=200, description_en="Blockchain explorers list", description_ru="Список обозревателей блокчейна",
                       example={"explorers": [{"name": "Etherscan", "chain": "Ethereum", "url": "https://etherscan.io"}]})
        ]
    ),
    
    ApiEndpoint(
        endpoint_id="project_bridges",
        path="/api/projects/{project_id}/bridges",
        method=HttpMethod.GET,
        title_en="Project Cross-Chain Bridges",
        title_ru="Мосты проекта",
        description_en="Get cross-chain bridges for project token.",
        description_ru="Получить кросс-чейн мосты для токена проекта.",
        category="projects",
        tags=["projects", "bridges", "cross-chain"],
        parameters=[
            ApiParameter(name="project_id", type="string", location="path", required=True, description_en="Project ID, slug, or symbol", description_ru="ID, slug или символ проекта"),
        ],
        responses=[
            ApiResponse(status_code=200, description_en="Cross-chain bridges list", description_ru="Список мостов",
                       example={"bridges": [{"bridge_name": "Arbitrum Bridge", "from_chain": "Ethereum", "to_chain": "Arbitrum"}]})
        ]
    ),
    
    ApiEndpoint(
        endpoint_id="project_activities_v2",
        path="/api/projects/{project_id}/activities",
        method=HttpMethod.GET,
        title_en="Project Activities",
        title_ru="Активности проекта",
        description_en="Get all activities (airdrops, campaigns, etc.) for a project.",
        description_ru="Получить все активности (airdrop'ы, кампании) для проекта.",
        category="projects",
        tags=["projects", "activities"],
        parameters=[
            ApiParameter(name="project_id", type="string", location="path", required=True, description_en="Project ID", description_ru="ID проекта"),
            ApiParameter(name="status", type="string", required=False, description_en="Filter: active, upcoming, ended", description_ru="Статус"),
            ApiParameter(name="type", type="string", required=False, description_en="Activity type filter", description_ru="Тип активности"),
            ApiParameter(name="limit", type="integer", required=False, default=20, description_en="Limit results", description_ru="Лимит"),
        ],
        responses=[
            ApiResponse(status_code=200, description_en="Project activities", description_ru="Активности проекта")
        ]
    ),
    
    ApiEndpoint(
        endpoint_id="project_unlocks_v2",
        path="/api/projects/{project_id}/unlocks",
        method=HttpMethod.GET,
        title_en="Project Token Unlocks",
        title_ru="Анлоки токенов проекта",
        description_en="Get all token unlocks for a project.",
        description_ru="Получить все анлоки токенов для проекта.",
        category="projects",
        tags=["projects", "unlocks", "tokenomics"],
        parameters=[
            ApiParameter(name="project_id", type="string", location="path", required=True, description_en="Project ID", description_ru="ID проекта"),
            ApiParameter(name="include_past", type="boolean", required=False, default=False, description_en="Include past unlocks", description_ru="Включить прошлые анлоки"),
            ApiParameter(name="limit", type="integer", required=False, default=50, description_en="Limit results", description_ru="Лимит"),
        ],
        responses=[
            ApiResponse(status_code=200, description_en="Token unlock schedule", description_ru="Расписание анлоков токенов")
        ]
    ),
]


# ═══════════════════════════════════════════════════════════════
# UNLOCKS EXTENDED API
# ═══════════════════════════════════════════════════════════════

UNLOCKS_EXTENDED_DOCUMENTATION = [
    ApiEndpoint(
        endpoint_id="unlocks_calendar",
        path="/api/unlocks/calendar",
        method=HttpMethod.GET,
        title_en="Token Unlocks Calendar",
        title_ru="Календарь анлоков",
        description_en="Get token unlocks calendar view grouped by date.",
        description_ru="Получить календарный вид анлоков токенов, сгруппированный по дате.",
        category="unlocks",
        tags=["unlocks", "calendar", "schedule"],
        parameters=[
            ApiParameter(name="year", type="integer", required=False, description_en="Year filter (default: current)", description_ru="Год"),
            ApiParameter(name="month", type="integer", required=False, description_en="Month filter 1-12 (default: current)", description_ru="Месяц 1-12"),
            ApiParameter(name="min_percent", type="number", required=False, description_en="Minimum % of supply", description_ru="Минимальный % supply"),
        ],
        responses=[
            ApiResponse(status_code=200, description_en="Calendar with unlocks grouped by date", description_ru="Календарь с анлоками по датам",
                       example={"year": 2026, "month": 3, "days_with_unlocks": 5, "calendar": [{"date": "2026-03-15", "unlock_count": 2, "unlocks": []}]})
        ]
    ),
]


# ═══════════════════════════════════════════════════════════════
# NEWS INTELLIGENCE HEALTH MONITORING API
# ═══════════════════════════════════════════════════════════════

NEWS_HEALTH_DOCUMENTATION = [
    ApiEndpoint(
        endpoint_id="news-health-sources",
        path="/api/news-intelligence/health/sources",
        method=HttpMethod.GET,
        title_en="Get Sources Health Metrics",
        title_ru="Получить метрики здоровья источников",
        description_en="Get detailed health metrics for all news sources including fetch success rate, validation rate, parser drift detection, and sandbox statistics.",
        description_ru="Получить детальные метрики здоровья всех новостных источников, включая успешность запросов, валидацию, обнаружение дрифта парсера и статистику sandbox.",
        category="news_intelligence",
        tags=["health", "monitoring", "sources", "sandbox"],
        responses=[
            ApiResponse(
                status_code=200,
                description_en="Health metrics for all sources",
                description_ru="Метрики здоровья всех источников",
                example={
                    "ok": True,
                    "sources": [{"source_id": "coindesk", "health_score": 0.98, "status": "active"}],
                    "summary": {"total_sources": 20, "active": 15, "paused": 1}
                }
            )
        ]
    ),
    
    ApiEndpoint(
        endpoint_id="news-health-summary",
        path="/api/news-intelligence/health/summary",
        method=HttpMethod.GET,
        title_en="Get Health Summary",
        title_ru="Получить сводку здоровья",
        description_en="Get summarized health status of the news intelligence system.",
        description_ru="Получить сводную информацию о здоровье системы новостной аналитики.",
        category="news_intelligence",
        tags=["health", "monitoring"],
        responses=[
            ApiResponse(status_code=200, description_en="Health summary", description_ru="Сводка здоровья")
        ]
    ),
    
    ApiEndpoint(
        endpoint_id="news-health-unpause",
        path="/api/news-intelligence/health/unpause/{source_id}",
        method=HttpMethod.POST,
        title_en="Unpause Source",
        title_ru="Возобновить источник",
        description_en="Manually unpause a paused news source.",
        description_ru="Вручную возобновить приостановленный новостной источник.",
        category="news_intelligence",
        tags=["health", "admin"],
        parameters=[
            ApiParameter(name="source_id", type="string", required=True, location="path", description_en="Source ID", description_ru="ID источника")
        ]
    ),
    
    ApiEndpoint(
        endpoint_id="news-breaking",
        path="/api/news-intelligence/breaking",
        method=HttpMethod.GET,
        title_en="Get Breaking News",
        title_ru="Получить срочные новости",
        description_en="Get latest breaking/developing news events with high importance.",
        description_ru="Получить последние срочные новостные события с высокой важностью.",
        category="news_intelligence",
        tags=["news", "breaking"],
        parameters=[
            ApiParameter(name="limit", type="integer", required=False, default=5, description_en="Max events", description_ru="Макс. событий")
        ]
    ),
    
    ApiEndpoint(
        endpoint_id="news-events-by-asset",
        path="/api/news-intelligence/assets/{symbol}",
        method=HttpMethod.GET,
        title_en="Get Events by Asset",
        title_ru="Получить события по активу",
        description_en="Get news events related to a specific cryptocurrency asset.",
        description_ru="Получить новостные события по криптоактиву.",
        category="news_intelligence",
        tags=["news", "assets"],
        parameters=[
            ApiParameter(name="symbol", type="string", required=True, location="path", description_en="Asset symbol (BTC, ETH)", description_ru="Символ актива")
        ]
    ),
    
    ApiEndpoint(
        endpoint_id="news-pipeline-fetch",
        path="/api/news-intelligence/pipeline/fetch",
        method=HttpMethod.POST,
        title_en="Run Fetch Pipeline",
        title_ru="Запустить пайплайн получения",
        description_en="Run fetch stage with sandbox isolation and validation.",
        description_ru="Запустить этап получения с изоляцией sandbox и валидацией.",
        category="news_intelligence",
        tags=["pipeline", "admin"]
    ),
    
    ApiEndpoint(
        endpoint_id="news-pipeline-process",
        path="/api/news-intelligence/pipeline/process",
        method=HttpMethod.POST,
        title_en="Run Process Pipeline",
        title_ru="Запустить пайплайн обработки",
        description_en="Run processing stages (normalize, embed, cluster).",
        description_ru="Запустить этапы обработки (нормализация, эмбеддинг, кластеризация).",
        category="news_intelligence",
        tags=["pipeline", "admin"]
    ),
    
    ApiEndpoint(
        endpoint_id="news-pipeline-synthesize",
        path="/api/news-intelligence/pipeline/synthesize",
        method=HttpMethod.POST,
        title_en="Run Synthesis Pipeline",
        title_ru="Запустить пайплайн синтеза",
        description_en="Run AI synthesis for confirmed events.",
        description_ru="Запустить AI синтез для подтвержденных событий.",
        category="news_intelligence",
        tags=["pipeline", "admin", "ai"]
    ),
    
    ApiEndpoint(
        endpoint_id="news-pipeline-merge",
        path="/api/news-intelligence/pipeline/merge",
        method=HttpMethod.POST,
        title_en="Run Event Merge",
        title_ru="Запустить слияние событий",
        description_en="Merge similar events to reduce duplicates.",
        description_ru="Объединить похожие события.",
        category="news_intelligence",
        tags=["pipeline", "admin"]
    ),
    
    ApiEndpoint(
        endpoint_id="news-scheduler-start",
        path="/api/news-intelligence/scheduler/start",
        method=HttpMethod.POST,
        title_en="Start Scheduler",
        title_ru="Запустить планировщик",
        description_en="Start the background news intelligence scheduler.",
        description_ru="Запустить фоновый планировщик.",
        category="news_intelligence",
        tags=["scheduler", "admin"]
    ),
    
    ApiEndpoint(
        endpoint_id="news-scheduler-stop",
        path="/api/news-intelligence/scheduler/stop",
        method=HttpMethod.POST,
        title_en="Stop Scheduler",
        title_ru="Остановить планировщик",
        description_en="Stop the background scheduler.",
        description_ru="Остановить планировщик.",
        category="news_intelligence",
        tags=["scheduler", "admin"]
    ),
    
    ApiEndpoint(
        endpoint_id="news-scheduler-status",
        path="/api/news-intelligence/scheduler/status",
        method=HttpMethod.GET,
        title_en="Get Scheduler Status",
        title_ru="Статус планировщика",
        description_en="Get scheduler status and last run info.",
        description_ru="Получить статус планировщика.",
        category="news_intelligence",
        tags=["scheduler"]
    ),
    
    ApiEndpoint(
        endpoint_id="news-event-types",
        path="/api/news-intelligence/event-types",
        method=HttpMethod.GET,
        title_en="Get Event Types",
        title_ru="Типы событий",
        description_en="Get list of available news event types.",
        description_ru="Получить список типов событий.",
        category="news_intelligence",
        tags=["news", "metadata"]
    ),
]


# Combined list for easy import
NEW_ENDPOINTS_DOCUMENTATION = (
    ACTIVITIES_DOCUMENTATION +
    INTEL_FEED_DOCUMENTATION +
    PROJECTS_EXTENDED_DOCUMENTATION +
    UNLOCKS_EXTENDED_DOCUMENTATION +
    NEWS_HEALTH_DOCUMENTATION
)
