# FOMO Data Sources Registry - Правильная Аранжировка

## Pipeline (Приоритет выполнения)
```
Parser → Public API → Admin API

Если источник падает → skip → следующий источник
Система НЕ зависит ни от одного источника
```

---

## TIER 1 — CORE DATA (sync каждые 10 мин)
Основа всей системы: projects, funding, investors, protocols, ecosystem

| # | ID | Name | Type | API | Parser | priority_score |
|---|-----|------|------|-----|--------|----------------|
| 1 | cryptorank | CryptoRank | funding, ico, unlocks | ✅ | ✅ | 100 |
| 2 | rootdata | RootData | funding, funds, persons | ✅ | ✅ | 95 |
| 3 | defillama | DefiLlama | defi, protocols, tvl | ✅ | - | 90 |
| 4 | dropstab | Dropstab | activities, airdrops | - | ✅ | 85 |

**Используется для:**
- projects
- funding rounds
- investors
- protocol data
- ecosystem

---

## TIER 2 — TOKEN / MARKET DATA (sync каждые 15 мин)
Рыночные данные: prices, marketcap, tokenomics, unlock schedules

| # | ID | Name | Type | API | Parser | priority_score |
|---|-----|------|------|-----|--------|----------------|
| 1 | coingecko | CoinGecko | market, prices | ✅ | ✅ | 80 |
| 2 | coinmarketcap | CoinMarketCap | market, prices | ✅🔑 | ✅ | 75 |
| 3 | tokenunlocks | TokenUnlocks | unlocks | ✅ | ✅ | 70 |

**Используется для:**
- prices
- marketcap
- tokenomics
- unlock schedules

**Правило:** CoinGecko/CoinMarketCap работают:
- API если есть ключ
- Parser если API нет

---

## TIER 3 — ACTIVITIES (sync каждые 30 мин)
Активности: airdrop campaigns, project events, launches

| # | ID | Name | Type | API | Parser | priority_score |
|---|-----|------|------|-----|--------|----------------|
| 1 | dropsearn | DropsEarn | activities | - | ✅ | 60 |
| 2 | icodrops | ICO Drops | ico | - | ✅ | 55 |
| 3 | dappradar | DappRadar | dapps | ✅ | ✅ | 50 |
| 4 | airdropalert | AirdropAlert | airdrops | - | ✅ | 45 |

**Используется для:**
- airdrop campaigns
- project activities
- launches
- ecosystem events

---

## TIER 4 — RESEARCH DATA (sync каждые 3 часа)
Исследования: project descriptions, research, tokenomics

| # | ID | Name | Type | API | Parser | priority_score |
|---|-----|------|------|-----|--------|----------------|
| 1 | messari | Messari | research | ✅🔑 | - | 30 |

**Используется для:**
- project descriptions
- research
- tokenomics data

**Правило:** если API нет → источник просто пропускается (не блокирует систему)

---

## PERSON / TEAM DATA (для графа person ↔ fund ↔ project)

| # | ID | Name | Type | API | Parser | priority_score |
|---|-----|------|------|-----|--------|----------------|
| 1 | rootdata | RootData | teams, founders | ✅ | ✅ | 95 |
| 2 | cryptorank | CryptoRank | teams | ✅ | ✅ | 100 |
| 3 | github | GitHub | developers | ✅ | - | 42 |
| 4 | twitter | Twitter/X | social | ✅🔑 | ✅ | 40 |
| 5 | linkedin | LinkedIn | professional | ✅🔑 | - | 25 |

---

## NEWS SOURCES (отдельная категория, обработка через news_parser)

| Tier | ID | Name | RSS | priority_score |
|------|-----|------|-----|----------------|
| 1 | incrypted | Incrypted | ✅ | 88 |
| 3 | cointelegraph | Cointelegraph | ✅ | 40 |
| 3 | theblock | The Block | ✅ | 38 |
| 3 | coindesk | CoinDesk | ✅ | 35 |

---

## БИРЖЕВЫЕ ДАННЫЕ (отдельная категория)

Используются только для:
- asset → market mapping
- trading pairs
- liquidity sources

### CEX (Централизованные)
Binance, Coinbase, Bybit, OKX, Kraken, KuCoin, Gate.io, Huobi, MEXC, Bitget, Bitfinex, Bitstamp, Crypto.com, Gemini

### DEX (Децентрализованные)
Uniswap, dYdX, HyperLiquid, PancakeSwap, Curve, GMX, Raydium, Jupiter, 1inch, SushiSwap

---

## ИТОГОВАЯ СТАТИСТИКА

| Tier | Interval | Sources | Description |
|------|----------|---------|-------------|
| 1 | 10 min | 4 | Core Data |
| 2 | 15 min | 3 | Token/Market |
| 3 | 30 min | 4 | Activities |
| 4 | 3 hours | 1 | Research |
| - | varies | 4 | News |
| - | varies | 5 | Person/Team |
| - | varies | 24 | Exchanges |

**Всего: 45 источников данных**

---

## КЛЮЧЕВЫЕ ПРАВИЛА

1. **Система НИКОГДА не зависит от одного источника**
2. **Если источник падает → skip → следующий**
3. **Pipeline: Parser → Public API → Admin API** (никогда наоборот)
4. **Приоритет выполнения: Tier1 → Tier2 → Tier3 → Tier4**
5. **API ключи опциональны** - без них система работает через парсеры

---
Updated: 2026-03-08
