# Sentiment API — Интеграция

## Базовый URL
```
<YOUR_SERVER_URL>
```

## Авторизация
Все запросы (кроме /health) требуют ключ в заголовке:
```
X-API-Key: <YOUR_API_KEY>
```

---

## Быстрый старт (30 секунд)

### Python — одна функция
```python
import requests

API_URL = "<YOUR_SERVER_URL>"
API_KEY = "<YOUR_API_KEY>"

def analyze_sentiment(text, source="generic"):
    r = requests.post(
        f"{API_URL}/api/v1/sentiment/analyze",
        json={"text": text, "source": source},
        headers={"X-API-Key": API_KEY},
    )
    return r.json()["data"]

# Использование:
result = analyze_sentiment("Bitcoin ETF approved!", "twitter")
print(result["label"])       # "POSITIVE"
print(result["score"])       # 0.72
print(result["meta"]["confidence"])  # "HIGH"
```

### JavaScript/TypeScript — одна функция
```javascript
const API_URL = "<YOUR_SERVER_URL>"
const API_KEY = "<YOUR_API_KEY>"

async function analyzeSentiment(text, source = "generic") {
  const res = await fetch(`${API_URL}/api/v1/sentiment/analyze`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-API-Key": API_KEY,
    },
    body: JSON.stringify({ text, source }),
  })
  const json = await res.json()
  return json.data
}

// Использование:
const result = await analyzeSentiment("Bitcoin ETF approved!", "twitter")
console.log(result.label)       // "POSITIVE"
console.log(result.score)       // 0.72
console.log(result.meta.confidence)  // "HIGH"
```

### curl
```bash
curl -X POST <YOUR_SERVER_URL>/api/v1/sentiment/analyze \
  -H "Content-Type: application/json" \
  -H "X-API-Key: <YOUR_API_KEY>" \
  -d '{"text": "Bitcoin ETF approved!", "source": "twitter"}'
```

---

## Endpoints

| Метод | Путь | Ключ | Описание |
|-------|------|------|----------|
| POST | /api/v1/sentiment/analyze | Да | Анализ одного текста |
| POST | /api/v1/sentiment/batch | Да | Пакетный анализ (до 100) |
| POST | /api/v1/sentiment/normalize | Да | Очистка текста + язык |
| GET | /api/v1/sentiment/health | Нет | Проверка статуса |
| GET | /api/v1/sentiment/capabilities | Нет | Возможности движка |

---

## Формат запроса — analyze

```json
{
  "text": "Bitcoin looks very strong today",
  "source": "twitter"
}
```

source — откуда текст: `twitter`, `news`, `telegram`, `article`, `headline`, `user`

## Формат ответа — analyze

```json
{
  "ok": true,
  "data": {
    "label": "POSITIVE",
    "score": 0.62,
    "source": "twitter",
    "meta": {
      "engineVersion": "2.0.0",
      "confidence": "HIGH",
      "confidenceScore": 0.71,
      "processingTimeMs": 0.3,
      "cached": false
    }
  }
}
```

- `label` — POSITIVE / NEUTRAL / NEGATIVE
- `score` — 0.0 (негатив) ... 1.0 (позитив)
- `confidence` — LOW / MEDIUM / HIGH

---

## Пакетный анализ — batch

```json
POST /api/v1/sentiment/batch

{
  "source": "news",
  "items": [
    {"id": "1", "text": "BTC breakout confirmed"},
    {"id": "2", "text": "Market crash incoming"}
  ]
}
```

---

## Ошибки

| Код | Ошибка | Причина |
|-----|--------|---------|
| 401 | UNAUTHORIZED | Нет ключа |
| 403 | FORBIDDEN | Неверный или отозванный ключ |
| 400 | INVALID_INPUT | Пустой text |
| 429 | RATE_LIMIT_EXCEEDED | Лимит 1000 запросов/мин |

---

## SDK (готовые файлы)

Скачать и положить в проект:

- Python: `GET <YOUR_SERVER_URL>/api/v1/sentiment/sdk/python`
- TypeScript: `GET <YOUR_SERVER_URL>/api/v1/sentiment/sdk/typescript`
