"""
FOMO Platform Bootstrap Script
==============================

Complete initialization script for FOMO Crypto Intelligence Terminal.
Preserves existing data while ensuring all required components are initialized.

Features:
- Safe upsert operations (preserves existing data)
- News Intelligence sources initialization
- Core assets registry
- Data providers setup
- API documentation seeding
- Indices creation for performance

Usage:
    python bootstrap.py [--force]
    
    --force: Reset health metrics and re-initialize all sources
"""

import asyncio
import sys
import logging
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# SEED DATA
# ═══════════════════════════════════════════════════════════════

PERSONS_DATA = [
    {"name": "Vitalik Buterin", "slug": "vitalik-buterin", "role": "founder", "projects": ["Ethereum"], "twitter": "VitalikButerin"},
    {"name": "Changpeng Zhao (CZ)", "slug": "cz-binance", "role": "founder", "projects": ["Binance"], "twitter": "caboris"},
    {"name": "Brian Armstrong", "slug": "brian-armstrong", "role": "founder", "projects": ["Coinbase"], "twitter": "brian_armstrong"},
    {"name": "Anatoly Yakovenko", "slug": "anatoly-yakovenko", "role": "founder", "projects": ["Solana"], "twitter": "aaboris"},
    {"name": "Hayden Adams", "slug": "hayden-adams", "role": "founder", "projects": ["Uniswap"], "twitter": "haaboris"},
    {"name": "Andre Cronje", "slug": "andre-cronje", "role": "founder", "projects": ["Yearn Finance", "Fantom"], "twitter": "andrecronje"},
    {"name": "Stani Kulechov", "slug": "stani-kulechov", "role": "founder", "projects": ["Aave"], "twitter": "stanikulechov"},
    {"name": "Sergey Nazarov", "slug": "sergey-nazarov", "role": "founder", "projects": ["Chainlink"], "twitter": "sergeynazarov"},
    {"name": "Gavin Wood", "slug": "gavin-wood", "role": "founder", "projects": ["Polkadot", "Ethereum"], "twitter": "gavofyork"},
    {"name": "Charles Hoskinson", "slug": "charles-hoskinson", "role": "founder", "projects": ["Cardano"], "twitter": "iohk_charles"},
    {"name": "Marc Andreessen", "slug": "marc-andreessen", "role": "investor", "projects": ["a16z"]},
    {"name": "Chris Dixon", "slug": "chris-dixon", "role": "investor", "projects": ["a16z crypto"], "twitter": "cdixon"},
    {"name": "Fred Ehrsam", "slug": "fred-ehrsam", "role": "investor", "projects": ["Paradigm"], "twitter": "FEhrsam"},
    {"name": "Matt Huang", "slug": "matt-huang", "role": "investor", "projects": ["Paradigm"], "twitter": "matthuang"},
    {"name": "Olaf Carlson-Wee", "slug": "olaf-carlson-wee", "role": "investor", "projects": ["Polychain"]},
    {"name": "Naval Ravikant", "slug": "naval-ravikant", "role": "investor", "projects": ["AngelList"], "twitter": "naval"},
    {"name": "Balaji Srinivasan", "slug": "balaji-srinivasan", "role": "investor", "projects": ["a16z"], "twitter": "balajis"},
    {"name": "Dan Morehead", "slug": "dan-morehead", "role": "investor", "projects": ["Pantera Capital"]},
    {"name": "Haseeb Qureshi", "slug": "haseeb-qureshi", "role": "investor", "projects": ["Dragonfly"], "twitter": "hosseeb"},
    {"name": "Arthur Hayes", "slug": "arthur-hayes", "role": "founder", "projects": ["BitMEX"], "twitter": "CryptoHayes"},
    {"name": "Sam Bankman-Fried", "slug": "sbf", "role": "former-founder", "projects": ["FTX"], "status": "inactive"},
    {"name": "Do Kwon", "slug": "do-kwon", "role": "former-founder", "projects": ["Terra"], "status": "inactive"},
    {"name": "Su Zhu", "slug": "su-zhu", "role": "former-founder", "projects": ["3AC"], "status": "inactive"},
]

EXCHANGES_DATA = [
    {"name": "Binance", "slug": "binance", "type": "CEX", "volume_rank": 1, "country": "Global"},
    {"name": "Coinbase", "slug": "coinbase", "type": "CEX", "volume_rank": 2, "country": "USA"},
    {"name": "Bybit", "slug": "bybit", "type": "CEX", "volume_rank": 3, "country": "Dubai"},
    {"name": "OKX", "slug": "okx", "type": "CEX", "volume_rank": 4, "country": "Seychelles"},
    {"name": "Kraken", "slug": "kraken", "type": "CEX", "volume_rank": 5, "country": "USA"},
    {"name": "KuCoin", "slug": "kucoin", "type": "CEX", "volume_rank": 6, "country": "Seychelles"},
    {"name": "Gate.io", "slug": "gate-io", "type": "CEX", "volume_rank": 7, "country": "Cayman"},
    {"name": "Huobi", "slug": "huobi", "type": "CEX", "volume_rank": 8, "country": "Seychelles"},
    {"name": "MEXC", "slug": "mexc", "type": "CEX", "volume_rank": 9, "country": "Singapore"},
    {"name": "Bitget", "slug": "bitget", "type": "CEX", "volume_rank": 10, "country": "Singapore"},
    {"name": "Bitfinex", "slug": "bitfinex", "type": "CEX", "volume_rank": 11, "country": "BVI"},
    {"name": "Bitstamp", "slug": "bitstamp", "type": "CEX", "volume_rank": 12, "country": "Luxembourg"},
    {"name": "Crypto.com", "slug": "crypto-com", "type": "CEX", "volume_rank": 13, "country": "Singapore"},
    {"name": "Gemini", "slug": "gemini", "type": "CEX", "volume_rank": 14, "country": "USA"},
    {"name": "Uniswap", "slug": "uniswap", "type": "DEX", "chain": "Ethereum", "tvl_rank": 1},
    {"name": "dYdX", "slug": "dydx", "type": "DEX", "chain": "Cosmos", "tvl_rank": 2},
    {"name": "HyperLiquid", "slug": "hyperliquid", "type": "DEX", "chain": "Arbitrum", "tvl_rank": 3},
    {"name": "PancakeSwap", "slug": "pancakeswap", "type": "DEX", "chain": "BSC", "tvl_rank": 4},
    {"name": "Curve", "slug": "curve", "type": "DEX", "chain": "Ethereum", "tvl_rank": 5},
    {"name": "GMX", "slug": "gmx", "type": "DEX", "chain": "Arbitrum", "tvl_rank": 6},
    {"name": "Raydium", "slug": "raydium", "type": "DEX", "chain": "Solana", "tvl_rank": 7},
    {"name": "Jupiter", "slug": "jupiter", "type": "DEX", "chain": "Solana", "tvl_rank": 8},
    {"name": "1inch", "slug": "1inch", "type": "DEX", "chain": "Multi-chain", "tvl_rank": 9},
    {"name": "SushiSwap", "slug": "sushiswap", "type": "DEX", "chain": "Multi-chain", "tvl_rank": 10},
]

PROJECTS_DATA = [
    {"name": "Bitcoin", "symbol": "BTC", "slug": "bitcoin", "category": "Currency", "market_cap_rank": 1},
    {"name": "Ethereum", "symbol": "ETH", "slug": "ethereum", "category": "Smart Contracts", "market_cap_rank": 2},
    {"name": "Tether", "symbol": "USDT", "slug": "tether", "category": "Stablecoin", "market_cap_rank": 3},
    {"name": "XRP", "symbol": "XRP", "slug": "ripple", "category": "Payment", "market_cap_rank": 4},
    {"name": "BNB", "symbol": "BNB", "slug": "bnb", "category": "Exchange", "market_cap_rank": 5},
    {"name": "Solana", "symbol": "SOL", "slug": "solana", "category": "Smart Contracts", "market_cap_rank": 6},
    {"name": "USD Coin", "symbol": "USDC", "slug": "usd-coin", "category": "Stablecoin", "market_cap_rank": 7},
    {"name": "Cardano", "symbol": "ADA", "slug": "cardano", "category": "Smart Contracts", "market_cap_rank": 8},
    {"name": "Avalanche", "symbol": "AVAX", "slug": "avalanche", "category": "Smart Contracts", "market_cap_rank": 9},
    {"name": "Dogecoin", "symbol": "DOGE", "slug": "dogecoin", "category": "Meme", "market_cap_rank": 10},
    {"name": "TRON", "symbol": "TRX", "slug": "tron", "category": "Smart Contracts", "market_cap_rank": 11},
    {"name": "Chainlink", "symbol": "LINK", "slug": "chainlink", "category": "Oracle", "market_cap_rank": 12},
    {"name": "Polkadot", "symbol": "DOT", "slug": "polkadot", "category": "Parachain", "market_cap_rank": 13},
    {"name": "Polygon", "symbol": "MATIC", "slug": "polygon", "category": "Layer 2", "market_cap_rank": 14},
    {"name": "Shiba Inu", "symbol": "SHIB", "slug": "shiba-inu", "category": "Meme", "market_cap_rank": 15},
    {"name": "Litecoin", "symbol": "LTC", "slug": "litecoin", "category": "Currency", "market_cap_rank": 16},
    {"name": "Uniswap", "symbol": "UNI", "slug": "uniswap", "category": "DEX", "market_cap_rank": 17},
    {"name": "Cosmos", "symbol": "ATOM", "slug": "cosmos", "category": "Interoperability", "market_cap_rank": 18},
    {"name": "Ethereum Classic", "symbol": "ETC", "slug": "ethereum-classic", "category": "Smart Contracts", "market_cap_rank": 19},
    {"name": "Monero", "symbol": "XMR", "slug": "monero", "category": "Privacy", "market_cap_rank": 20},
    {"name": "Arbitrum", "symbol": "ARB", "slug": "arbitrum", "category": "Layer 2"},
    {"name": "Optimism", "symbol": "OP", "slug": "optimism", "category": "Layer 2"},
    {"name": "Aave", "symbol": "AAVE", "slug": "aave", "category": "DeFi Lending"},
    {"name": "Lido", "symbol": "LDO", "slug": "lido", "category": "Liquid Staking"},
    {"name": "Maker", "symbol": "MKR", "slug": "maker", "category": "DeFi"},
    {"name": "Celestia", "symbol": "TIA", "slug": "celestia", "category": "Modular"},
    {"name": "Sui", "symbol": "SUI", "slug": "sui", "category": "Smart Contracts"},
    {"name": "Aptos", "symbol": "APT", "slug": "aptos", "category": "Smart Contracts"},
    {"name": "Starknet", "symbol": "STRK", "slug": "starknet", "category": "Layer 2"},
    {"name": "LayerZero", "symbol": "ZRO", "slug": "layerzero", "category": "Cross-chain"},
    {"name": "EigenLayer", "symbol": "EIGEN", "slug": "eigenlayer", "category": "Restaking"},
    {"name": "Pyth Network", "symbol": "PYTH", "slug": "pyth", "category": "Oracle"},
    {"name": "Jupiter", "symbol": "JUP", "slug": "jupiter", "category": "DEX Aggregator"},
    {"name": "Render", "symbol": "RNDR", "slug": "render", "category": "AI/GPU"},
    {"name": "Fetch.ai", "symbol": "FET", "slug": "fetch-ai", "category": "AI"},
    {"name": "Injective", "symbol": "INJ", "slug": "injective", "category": "DeFi"},
    {"name": "Sei", "symbol": "SEI", "slug": "sei", "category": "Trading Chain"},
    {"name": "Pendle", "symbol": "PENDLE", "slug": "pendle", "category": "Yield"},
    {"name": "Ethena", "symbol": "ENA", "slug": "ethena", "category": "Synthetic USD"},
    {"name": "Worldcoin", "symbol": "WLD", "slug": "worldcoin", "category": "Identity"},
]

INVESTORS_DATA = [
    {"name": "a16z crypto", "slug": "a16z", "type": "VC", "tier": 1, "aum": "7.6B"},
    {"name": "Paradigm", "slug": "paradigm", "type": "VC", "tier": 1, "aum": "2.5B"},
    {"name": "Polychain Capital", "slug": "polychain", "type": "VC", "tier": 1},
    {"name": "Pantera Capital", "slug": "pantera", "type": "VC", "tier": 1},
    {"name": "Dragonfly", "slug": "dragonfly", "type": "VC", "tier": 1},
    {"name": "Multicoin Capital", "slug": "multicoin", "type": "VC", "tier": 1},
    {"name": "Electric Capital", "slug": "electric", "type": "VC", "tier": 1},
    {"name": "Framework Ventures", "slug": "framework", "type": "VC", "tier": 2},
    {"name": "Hack VC", "slug": "hack-vc", "type": "VC", "tier": 2},
    {"name": "Binance Labs", "slug": "binance-labs", "type": "Corporate VC", "tier": 1},
    {"name": "Coinbase Ventures", "slug": "coinbase-ventures", "type": "Corporate VC", "tier": 1},
    {"name": "Jump Crypto", "slug": "jump-crypto", "type": "Trading/VC", "tier": 1},
    {"name": "Wintermute", "slug": "wintermute", "type": "Trading/VC", "tier": 1},
    {"name": "Galaxy Digital", "slug": "galaxy", "type": "Fund", "tier": 1},
    {"name": "Grayscale", "slug": "grayscale", "type": "Fund", "tier": 1},
]

NEWS_SOURCES_DATA = [
    # ═══════════════════════════════════════════════════════════════
    # TIER A - PRIMARY SOURCES (5 min refresh) - 15 sources
    # ═══════════════════════════════════════════════════════════════
    {"id": "coindesk", "name": "CoinDesk", "domain": "coindesk.com", "tier": "A", "language": "en", "category": "news", "rss_url": "https://www.coindesk.com/arc/outboundfeeds/rss/"},
    {"id": "cointelegraph", "name": "Cointelegraph", "domain": "cointelegraph.com", "tier": "A", "language": "en", "category": "news", "rss_url": "https://cointelegraph.com/rss"},
    {"id": "theblock", "name": "The Block", "domain": "theblock.co", "tier": "A", "language": "en", "category": "news", "rss_url": "https://www.theblock.co/rss.xml"},
    {"id": "decrypt", "name": "Decrypt", "domain": "decrypt.co", "tier": "A", "language": "en", "category": "news", "rss_url": "https://decrypt.co/feed"},
    {"id": "blockworks", "name": "Blockworks", "domain": "blockworks.co", "tier": "A", "language": "en", "category": "news", "rss_url": "https://blockworks.co/feed/"},
    {"id": "bitcoinmagazine", "name": "Bitcoin Magazine", "domain": "bitcoinmagazine.com", "tier": "A", "language": "en", "category": "news", "rss_url": "https://bitcoinmagazine.com/.rss/full/"},
    {"id": "cryptoslate", "name": "CryptoSlate", "domain": "cryptoslate.com", "tier": "A", "language": "en", "category": "news", "rss_url": "https://cryptoslate.com/feed/"},
    {"id": "dailyhodl", "name": "The Daily Hodl", "domain": "dailyhodl.com", "tier": "A", "language": "en", "category": "news", "rss_url": "https://dailyhodl.com/feed/"},
    {"id": "cryptonews_en", "name": "CryptoNews EN", "domain": "cryptonews.com", "tier": "A", "language": "en", "category": "news", "rss_url": "https://cryptonews.com/news/feed/"},
    {"id": "bitcoinist", "name": "Bitcoinist", "domain": "bitcoinist.com", "tier": "A", "language": "en", "category": "news", "rss_url": "https://bitcoinist.com/feed/"},
    {"id": "incrypted", "name": "Incrypted", "domain": "incrypted.com", "tier": "A", "language": "ru", "category": "news", "rss_url": "https://incrypted.com/feed/"},
    {"id": "forklog", "name": "Forklog", "domain": "forklog.com", "tier": "A", "language": "ru", "category": "news", "rss_url": "https://forklog.com/feed/"},
    {"id": "bits_media", "name": "Bits.media", "domain": "bits.media", "tier": "A", "language": "ru", "category": "news", "rss_url": "https://bits.media/rss2/"},
    {"id": "coinspot", "name": "CoinSpot", "domain": "coinspot.io", "tier": "A", "language": "ru", "category": "news", "rss_url": "https://coinspot.io/feed/"},
    {"id": "cryptach", "name": "Cryptach", "domain": "cryptach.org", "tier": "A", "language": "ua", "category": "news", "rss_url": "https://cryptach.org/feed/"},
    
    # ═══════════════════════════════════════════════════════════════
    # TIER B - SECONDARY NEWS (10 min refresh) - 35 sources
    # ═══════════════════════════════════════════════════════════════
    {"id": "beincrypto", "name": "BeInCrypto", "domain": "beincrypto.com", "tier": "B", "language": "en", "category": "news", "rss_url": "https://beincrypto.com/feed/"},
    {"id": "newsbtc", "name": "NewsBTC", "domain": "newsbtc.com", "tier": "B", "language": "en", "category": "news", "rss_url": "https://www.newsbtc.com/feed/"},
    {"id": "cryptopotato", "name": "CryptoPotato", "domain": "cryptopotato.com", "tier": "B", "language": "en", "category": "news", "rss_url": "https://cryptopotato.com/feed/"},
    {"id": "utoday", "name": "U.Today", "domain": "u.today", "tier": "B", "language": "en", "category": "news", "rss_url": "https://u.today/rss"},
    {"id": "coinjournal", "name": "CoinJournal", "domain": "coinjournal.net", "tier": "B", "language": "en", "category": "news", "rss_url": "https://coinjournal.net/feed/"},
    {"id": "coingape", "name": "CoinGape", "domain": "coingape.com", "tier": "B", "language": "en", "category": "news", "rss_url": "https://coingape.com/feed/"},
    {"id": "coinpedia", "name": "Coinpedia", "domain": "coinpedia.org", "tier": "B", "language": "en", "category": "news", "rss_url": "https://coinpedia.org/feed/"},
    {"id": "cryptobriefing", "name": "Crypto Briefing", "domain": "cryptobriefing.com", "tier": "B", "language": "en", "category": "news", "rss_url": "https://cryptobriefing.com/feed/"},
    {"id": "ambcrypto", "name": "AMBCrypto", "domain": "ambcrypto.com", "tier": "B", "language": "en", "category": "news", "rss_url": "https://ambcrypto.com/feed/"},
    {"id": "cryptoglobe", "name": "CryptoGlobe", "domain": "cryptoglobe.com", "tier": "B", "language": "en", "category": "news", "rss_url": "https://www.cryptoglobe.com/latest/feed/"},
    {"id": "zycrypto", "name": "ZyCrypto", "domain": "zycrypto.com", "tier": "B", "language": "en", "category": "news", "rss_url": "https://zycrypto.com/feed/"},
    {"id": "blockonomi", "name": "Blockonomi", "domain": "blockonomi.com", "tier": "B", "language": "en", "category": "news", "rss_url": "https://blockonomi.com/feed/"},
    {"id": "coinspeaker", "name": "Coinspeaker", "domain": "coinspeaker.com", "tier": "B", "language": "en", "category": "news", "rss_url": "https://www.coinspeaker.com/feed/"},
    {"id": "cryptonewsz", "name": "CryptoNewsZ", "domain": "cryptonewsz.com", "tier": "B", "language": "en", "category": "news", "rss_url": "https://www.cryptonewsz.com/feed/"},
    {"id": "nulltx", "name": "NullTX", "domain": "nulltx.com", "tier": "B", "language": "en", "category": "news", "rss_url": "https://nulltx.com/feed/"},
    {"id": "coincodex", "name": "CoinCodex News", "domain": "coincodex.com", "tier": "B", "language": "en", "category": "news", "rss_url": "https://coincodex.com/feed/"},
    {"id": "cryptodaily", "name": "Crypto Daily", "domain": "cryptodaily.co.uk", "tier": "B", "language": "en", "category": "news", "rss_url": "https://cryptodaily.co.uk/feed"},
    {"id": "thecryptobasic", "name": "The Crypto Basic", "domain": "thecryptobasic.com", "tier": "B", "language": "en", "category": "news", "rss_url": "https://thecryptobasic.com/feed/"},
    {"id": "cryptonewsland", "name": "CryptoNewsLand", "domain": "cryptonewsland.com", "tier": "B", "language": "en", "category": "news", "rss_url": "https://cryptonewsland.com/feed/"},
    {"id": "btcecho", "name": "BTC-ECHO", "domain": "btc-echo.de", "tier": "B", "language": "de", "category": "news", "rss_url": "https://www.btc-echo.de/feed/"},
    {"id": "cryptonews_ru", "name": "CryptoNews RU", "domain": "cryptonews.net", "tier": "B", "language": "ru", "category": "news", "rss_url": "https://ru.cryptonews.com/news/feed/"},
    {"id": "2bitcoins", "name": "2Bitcoins", "domain": "2bitcoins.ru", "tier": "B", "language": "ru", "category": "news", "rss_url": "https://2bitcoins.ru/feed/"},
    {"id": "mining_cryptocurrency", "name": "Mining Cryptocurrency", "domain": "mining-cryptocurrency.ru", "tier": "B", "language": "ru", "category": "news", "rss_url": "https://mining-cryptocurrency.ru/feed/"},
    {"id": "coinpost", "name": "CoinPost", "domain": "coinpost.jp", "tier": "B", "language": "jp", "category": "news", "rss_url": "https://coinpost.jp/feed"},
    {"id": "coinjinja", "name": "CoinJinja", "domain": "coinjinja.com", "tier": "B", "language": "jp", "category": "news", "rss_url": "https://coinjinja.com/feed"},
    {"id": "jinse", "name": "Jinse Finance", "domain": "jinse.com", "tier": "B", "language": "zh", "category": "news", "rss_url": "https://www.jinse.com/rss"},
    {"id": "chainnews", "name": "ChainNews", "domain": "chainnews.com", "tier": "B", "language": "zh", "category": "news", "rss_url": "https://www.chainnews.com/rss"},
    {"id": "tokenclub", "name": "TokenClub", "domain": "tokenclub.com", "tier": "B", "language": "zh", "category": "news", "rss_url": "https://www.tokenclub.com/rss"},
    {"id": "blocktempo", "name": "BlockTempo", "domain": "blocktempo.com", "tier": "B", "language": "zh", "category": "news", "rss_url": "https://www.blocktempo.com/feed/"},
    {"id": "techflowpost", "name": "TechFlow", "domain": "techflowpost.com", "tier": "B", "language": "zh", "category": "news", "rss_url": "https://www.techflowpost.com/rss"},
    {"id": "panewslab", "name": "PANews", "domain": "panewslab.com", "tier": "B", "language": "zh", "category": "news", "rss_url": "https://www.panewslab.com/rss"},
    {"id": "odaily", "name": "Odaily", "domain": "odaily.news", "tier": "B", "language": "zh", "category": "news", "rss_url": "https://www.odaily.news/rss"},
    {"id": "foresightnews", "name": "Foresight News", "domain": "foresightnews.pro", "tier": "B", "language": "zh", "category": "news", "rss_url": "https://foresightnews.pro/rss"},
    {"id": "thedefiant", "name": "The Defiant", "domain": "thedefiant.io", "tier": "B", "language": "en", "category": "defi", "rss_url": "https://thedefiant.io/feed"},
    {"id": "defiprime", "name": "DeFi Prime", "domain": "defiprime.com", "tier": "B", "language": "en", "category": "defi", "rss_url": "https://defiprime.com/feed.xml"},
    
    # ═══════════════════════════════════════════════════════════════
    # TIER C - NICHE/RESEARCH (30 min refresh) - 40 sources
    # ═══════════════════════════════════════════════════════════════
    {"id": "messari_research", "name": "Messari Research", "domain": "messari.io", "tier": "C", "language": "en", "category": "research", "rss_url": "https://messari.io/rss/news"},
    {"id": "delphidigital", "name": "Delphi Digital", "domain": "delphidigital.io", "tier": "C", "language": "en", "category": "research", "rss_url": "https://members.delphidigital.io/feed"},
    {"id": "galaxy_research", "name": "Galaxy Research", "domain": "galaxy.com", "tier": "C", "language": "en", "category": "research", "rss_url": "https://www.galaxy.com/research/feed/"},
    {"id": "coinbase_research", "name": "Coinbase Research", "domain": "coinbase.com", "tier": "C", "language": "en", "category": "research", "rss_url": "https://www.coinbase.com/blog/feed"},
    {"id": "binance_research", "name": "Binance Research", "domain": "binance.com", "tier": "C", "language": "en", "category": "research", "rss_url": "https://research.binance.com/feed"},
    {"id": "a16z_crypto", "name": "a16z Crypto", "domain": "a16zcrypto.com", "tier": "C", "language": "en", "category": "research", "rss_url": "https://a16zcrypto.com/feed/"},
    {"id": "paradigm", "name": "Paradigm", "domain": "paradigm.xyz", "tier": "C", "language": "en", "category": "research", "rss_url": "https://www.paradigm.xyz/feed.xml"},
    {"id": "jump_crypto", "name": "Jump Crypto", "domain": "jumpcrypto.com", "tier": "C", "language": "en", "category": "research", "rss_url": "https://jumpcrypto.com/feed/"},
    {"id": "dragonfly", "name": "Dragonfly Research", "domain": "dragonfly.xyz", "tier": "C", "language": "en", "category": "research", "rss_url": "https://medium.com/feed/dragonfly-research"},
    {"id": "polychain", "name": "Polychain Capital", "domain": "polychain.capital", "tier": "C", "language": "en", "category": "research", "rss_url": "https://polychain.capital/blog-feed.xml"},
    {"id": "multicoin", "name": "Multicoin Capital", "domain": "multicoin.capital", "tier": "C", "language": "en", "category": "research", "rss_url": "https://multicoin.capital/feed/"},
    {"id": "pantera", "name": "Pantera Capital", "domain": "panteracapital.com", "tier": "C", "language": "en", "category": "research", "rss_url": "https://panteracapital.com/blockchain-letter/feed/"},
    {"id": "placeholder", "name": "Placeholder VC", "domain": "placeholder.vc", "tier": "C", "language": "en", "category": "research", "rss_url": "https://www.placeholder.vc/blog?format=rss"},
    {"id": "variant", "name": "Variant Fund", "domain": "variant.fund", "tier": "C", "language": "en", "category": "research", "rss_url": "https://variant.fund/feed.xml"},
    {"id": "electric_capital", "name": "Electric Capital", "domain": "electriccapital.com", "tier": "C", "language": "en", "category": "research", "rss_url": "https://www.electriccapital.com/feed"},
    {"id": "framework", "name": "Framework Ventures", "domain": "framework.ventures", "tier": "C", "language": "en", "category": "research", "rss_url": "https://framework.ventures/feed"},
    {"id": "mechanism", "name": "Mechanism Capital", "domain": "mechanism.capital", "tier": "C", "language": "en", "category": "research", "rss_url": "https://www.mechanism.capital/feed"},
    {"id": "bankless", "name": "Bankless", "domain": "bankless.com", "tier": "C", "language": "en", "category": "analysis", "rss_url": "https://www.bankless.com/feed"},
    {"id": "weekinethereumnews", "name": "Week in Ethereum News", "domain": "weekinethereumnews.com", "tier": "C", "language": "en", "category": "analysis", "rss_url": "https://weekinethereumnews.com/feed/"},
    {"id": "ethereum_blog", "name": "Ethereum Foundation Blog", "domain": "blog.ethereum.org", "tier": "C", "language": "en", "category": "official", "rss_url": "https://blog.ethereum.org/feed.xml"},
    {"id": "bitcoin_dev", "name": "Bitcoin Dev", "domain": "bitcoin.org", "tier": "C", "language": "en", "category": "official", "rss_url": "https://bitcoin.org/en/rss/blog.xml"},
    {"id": "solana_blog", "name": "Solana Blog", "domain": "solana.com", "tier": "C", "language": "en", "category": "official", "rss_url": "https://solana.com/news/feed.xml"},
    {"id": "avalanche_blog", "name": "Avalanche Blog", "domain": "avax.network", "tier": "C", "language": "en", "category": "official", "rss_url": "https://medium.com/feed/avalancheavax"},
    {"id": "polygon_blog", "name": "Polygon Blog", "domain": "polygon.technology", "tier": "C", "language": "en", "category": "official", "rss_url": "https://blog.polygon.technology/feed/"},
    {"id": "arbitrum_blog", "name": "Arbitrum Blog", "domain": "arbitrum.io", "tier": "C", "language": "en", "category": "official", "rss_url": "https://medium.com/feed/offchainlabs"},
    {"id": "optimism_blog", "name": "Optimism Blog", "domain": "optimism.io", "tier": "C", "language": "en", "category": "official", "rss_url": "https://optimism.mirror.xyz/feed/atom"},
    {"id": "near_blog", "name": "NEAR Blog", "domain": "near.org", "tier": "C", "language": "en", "category": "official", "rss_url": "https://near.org/blog/feed/"},
    {"id": "cosmos_blog", "name": "Cosmos Blog", "domain": "cosmos.network", "tier": "C", "language": "en", "category": "official", "rss_url": "https://blog.cosmos.network/feed"},
    {"id": "polkadot_blog", "name": "Polkadot Blog", "domain": "polkadot.network", "tier": "C", "language": "en", "category": "official", "rss_url": "https://polkadot.network/blog/feed/"},
    {"id": "chainlink_blog", "name": "Chainlink Blog", "domain": "chain.link", "tier": "C", "language": "en", "category": "official", "rss_url": "https://blog.chain.link/feed/"},
    {"id": "uniswap_blog", "name": "Uniswap Blog", "domain": "uniswap.org", "tier": "C", "language": "en", "category": "official", "rss_url": "https://blog.uniswap.org/rss.xml"},
    {"id": "aave_blog", "name": "Aave Blog", "domain": "aave.com", "tier": "C", "language": "en", "category": "official", "rss_url": "https://aave.mirror.xyz/feed/atom"},
    {"id": "compound_blog", "name": "Compound Blog", "domain": "compound.finance", "tier": "C", "language": "en", "category": "official", "rss_url": "https://medium.com/feed/compound-finance"},
    {"id": "makerdao_blog", "name": "MakerDAO Blog", "domain": "makerdao.com", "tier": "C", "language": "en", "category": "official", "rss_url": "https://blog.makerdao.com/feed/"},
    {"id": "lido_blog", "name": "Lido Blog", "domain": "lido.fi", "tier": "C", "language": "en", "category": "official", "rss_url": "https://blog.lido.fi/rss/"},
    {"id": "eigenlayer_blog", "name": "EigenLayer Blog", "domain": "eigenlayer.xyz", "tier": "C", "language": "en", "category": "official", "rss_url": "https://www.blog.eigenlayer.xyz/rss/"},
    {"id": "layerzero_blog", "name": "LayerZero Blog", "domain": "layerzero.network", "tier": "C", "language": "en", "category": "official", "rss_url": "https://medium.com/feed/layerzero-official"},
    {"id": "wormhole_blog", "name": "Wormhole Blog", "domain": "wormhole.com", "tier": "C", "language": "en", "category": "official", "rss_url": "https://wormhole.com/blog/feed"},
    {"id": "celestia_blog", "name": "Celestia Blog", "domain": "celestia.org", "tier": "C", "language": "en", "category": "official", "rss_url": "https://blog.celestia.org/rss/"},
    {"id": "sui_blog", "name": "Sui Blog", "domain": "sui.io", "tier": "C", "language": "en", "category": "official", "rss_url": "https://blog.sui.io/feed"},
    
    # ═══════════════════════════════════════════════════════════════
    # TIER D - AGGREGATORS & TRACKERS (60 min refresh) - 30 sources
    # ═══════════════════════════════════════════════════════════════
    {"id": "coingecko_news", "name": "CoinGecko News", "domain": "coingecko.com", "tier": "D", "language": "en", "category": "aggregator", "rss_url": "https://www.coingecko.com/en/news/feed"},
    {"id": "coinmarketcap_news", "name": "CoinMarketCap News", "domain": "coinmarketcap.com", "tier": "D", "language": "en", "category": "aggregator", "rss_url": "https://coinmarketcap.com/headlines/feed/"},
    {"id": "cryptopanic", "name": "CryptoPanic", "domain": "cryptopanic.com", "tier": "D", "language": "en", "category": "aggregator", "rss_url": "https://cryptopanic.com/news/rss/"},
    {"id": "rekt_news", "name": "Rekt News", "domain": "rekt.news", "tier": "D", "language": "en", "category": "security", "rss_url": "https://rekt.news/feed.xml"},
    {"id": "slowmist", "name": "SlowMist", "domain": "slowmist.com", "tier": "D", "language": "en", "category": "security", "rss_url": "https://slowmist.medium.com/feed"},
    {"id": "certik", "name": "CertiK Alerts", "domain": "certik.com", "tier": "D", "language": "en", "category": "security", "rss_url": "https://www.certik.com/resources/blog/feed"},
    {"id": "immunefi", "name": "Immunefi", "domain": "immunefi.com", "tier": "D", "language": "en", "category": "security", "rss_url": "https://medium.com/feed/immunefi"},
    {"id": "chainalysis", "name": "Chainalysis Blog", "domain": "chainalysis.com", "tier": "D", "language": "en", "category": "analytics", "rss_url": "https://blog.chainalysis.com/reports/feed/"},
    {"id": "glassnode_insights", "name": "Glassnode Insights", "domain": "glassnode.com", "tier": "D", "language": "en", "category": "analytics", "rss_url": "https://insights.glassnode.com/rss/"},
    {"id": "santiment_insights", "name": "Santiment Insights", "domain": "santiment.net", "tier": "D", "language": "en", "category": "analytics", "rss_url": "https://insights.santiment.net/feed"},
    {"id": "nansen_research", "name": "Nansen Research", "domain": "nansen.ai", "tier": "D", "language": "en", "category": "analytics", "rss_url": "https://www.nansen.ai/feed"},
    {"id": "dune_blog", "name": "Dune Blog", "domain": "dune.com", "tier": "D", "language": "en", "category": "analytics", "rss_url": "https://dune.com/blog/feed.xml"},
    {"id": "defillama_news", "name": "DeFiLlama News", "domain": "defillama.com", "tier": "D", "language": "en", "category": "defi", "rss_url": "https://defillama.com/feed"},
    {"id": "debank_news", "name": "DeBank News", "domain": "debank.com", "tier": "D", "language": "en", "category": "defi", "rss_url": "https://debank.com/feed"},
    {"id": "tokenterminal_research", "name": "Token Terminal", "domain": "tokenterminal.com", "tier": "D", "language": "en", "category": "analytics", "rss_url": "https://tokenterminal.com/blog/feed"},
    {"id": "l2beat", "name": "L2BEAT", "domain": "l2beat.com", "tier": "D", "language": "en", "category": "l2", "rss_url": "https://l2beat.com/feed.xml"},
    {"id": "growthepie", "name": "growthepie", "domain": "growthepie.xyz", "tier": "D", "language": "en", "category": "l2", "rss_url": "https://www.growthepie.xyz/feed"},
    {"id": "artemis", "name": "Artemis", "domain": "artemis.xyz", "tier": "D", "language": "en", "category": "analytics", "rss_url": "https://www.artemis.xyz/feed"},
    {"id": "rootdata_news", "name": "RootData News", "domain": "rootdata.com", "tier": "D", "language": "en", "category": "funding", "rss_url": "https://www.rootdata.com/feed"},
    {"id": "cryptorank_news", "name": "CryptoRank News", "domain": "cryptorank.io", "tier": "D", "language": "en", "category": "funding", "rss_url": "https://cryptorank.io/news/feed"},
    {"id": "coinlist", "name": "CoinList Blog", "domain": "coinlist.co", "tier": "D", "language": "en", "category": "funding", "rss_url": "https://blog.coinlist.co/rss/"},
    {"id": "tokensniffer", "name": "TokenSniffer", "domain": "tokensniffer.com", "tier": "D", "language": "en", "category": "security", "rss_url": "https://tokensniffer.com/feed"},
    {"id": "dexscreener_blog", "name": "DEXScreener Blog", "domain": "dexscreener.com", "tier": "D", "language": "en", "category": "dex", "rss_url": "https://dexscreener.com/blog/feed"},
    {"id": "geckoterminal_blog", "name": "GeckoTerminal Blog", "domain": "geckoterminal.com", "tier": "D", "language": "en", "category": "dex", "rss_url": "https://www.geckoterminal.com/blog/feed"},
    {"id": "dextools_blog", "name": "DEXTools Blog", "domain": "dextools.io", "tier": "D", "language": "en", "category": "dex", "rss_url": "https://www.dextools.io/blog/feed/"},
    {"id": "definedfi", "name": "Defined.fi", "domain": "defined.fi", "tier": "D", "language": "en", "category": "dex", "rss_url": "https://www.defined.fi/blog/feed"},
    {"id": "coinglass_blog", "name": "Coinglass Blog", "domain": "coinglass.com", "tier": "D", "language": "en", "category": "derivatives", "rss_url": "https://www.coinglass.com/blog/feed"},
    {"id": "laevitas", "name": "Laevitas", "domain": "laevitas.ch", "tier": "D", "language": "en", "category": "derivatives", "rss_url": "https://www.laevitas.ch/blog/feed"},
    {"id": "velo_data", "name": "Velo Data", "domain": "velodata.app", "tier": "D", "language": "en", "category": "derivatives", "rss_url": "https://velodata.app/blog/feed"},
    {"id": "theblockcrypto", "name": "The Block Data", "domain": "theblockresearch.com", "tier": "D", "language": "en", "category": "research", "rss_url": "https://www.theblockresearch.com/feed"},
]

DATA_PROVIDERS_DATA = [
    {"id": "coingecko", "name": "CoinGecko", "type": "market_data", "requires_key": False, "rate_limit": 30},
    {"id": "coinmarketcap", "name": "CoinMarketCap", "type": "market_data", "requires_key": True, "rate_limit": 30},
    {"id": "messari", "name": "Messari", "type": "research", "requires_key": True, "rate_limit": 20},
    {"id": "defillama", "name": "DefiLlama", "type": "defi", "requires_key": False, "rate_limit": 60},
    {"id": "dune", "name": "Dune Analytics", "type": "onchain", "requires_key": True, "rate_limit": 10},
    {"id": "glassnode", "name": "Glassnode", "type": "onchain", "requires_key": True, "rate_limit": 10},
    {"id": "santiment", "name": "Santiment", "type": "social", "requires_key": True, "rate_limit": 20},
    {"id": "nansen", "name": "Nansen", "type": "onchain", "requires_key": True, "rate_limit": 10},
    {"id": "arkham", "name": "Arkham Intelligence", "type": "onchain", "requires_key": True, "rate_limit": 10},
    {"id": "cryptorank", "name": "CryptoRank", "type": "market_data", "requires_key": False, "rate_limit": 30},
    {"id": "dropstab", "name": "Dropstab", "type": "unlocks", "requires_key": False, "rate_limit": 30},
    {"id": "tokenterminal", "name": "Token Terminal", "type": "fundamentals", "requires_key": True, "rate_limit": 20},
]


# ═══════════════════════════════════════════════════════════════
# DATA SOURCES FOR DISCOVERY ENGINE - PROPER TIER STRUCTURE
# Pipeline: Parser → Public API → Admin API
# System NEVER depends on single source
# ═══════════════════════════════════════════════════════════════

DATA_SOURCES_DATA = [
    # ═══════════════════════════════════════════════════════════════
    # TIER 1 - CORE DATA (sync every 10 min)
    # Primary sources for: projects, funding, investors, protocols, ecosystem
    # ═══════════════════════════════════════════════════════════════
    {
        "id": "cryptorank", "name": "CryptoRank", "domain": "cryptorank.io",
        "tier": 1, "priority_score": 100, "sync_interval_min": 10,
        "tree": "intel", "status": "active",
        "categories": ["funding", "ico", "unlocks", "activities", "analytics", "persons"],
        "owns": ["funding_rounds", "funding_amount", "investor_list", "lead_investor"],
        "access": "parser", "has_api": True, "api_key_required": False,
        "is_new": False
    },
    {
        "id": "rootdata", "name": "RootData", "domain": "rootdata.com",
        "tier": 1, "priority_score": 95, "sync_interval_min": 10,
        "tree": "intel", "status": "active",
        "categories": ["funding", "funds", "persons"],
        "owns": ["fund_profile", "fund_portfolio", "founders", "team_members", "advisors"],
        "access": "parser", "has_api": True, "api_key_required": False,
        "is_new": False
    },
    {
        "id": "defillama", "name": "DefiLlama", "domain": "defillama.com",
        "tier": 1, "priority_score": 90, "sync_interval_min": 5,
        "tree": "intel", "status": "active",
        "categories": ["defi", "projects", "analytics"],
        "owns": ["tvl", "protocols", "chains", "defi_categories"],
        "access": "api_public", "has_api": True, "api_key_required": False,
        "is_new": False
    },
    {
        "id": "dropstab", "name": "Dropstab", "domain": "dropstab.com",
        "tier": 1, "priority_score": 85, "sync_interval_min": 10,
        "tree": "intel", "status": "active",
        "categories": ["activities", "tokenomics"],
        "owns": ["token_allocations", "vesting_model", "insider_supply", "sell_pressure"],
        "access": "parser", "has_api": False, "api_key_required": False,
        "is_new": False
    },
    
    # ═══════════════════════════════════════════════════════════════
    # TIER 2 - TOKEN / MARKET DATA (sync every 15 min)
    # prices, marketcap, tokenomics, unlock schedules
    # CoinGecko/CMC: API if available, Parser if no API
    # ═══════════════════════════════════════════════════════════════
    {
        "id": "coingecko", "name": "CoinGecko", "domain": "coingecko.com",
        "tier": 2, "priority_score": 80, "sync_interval_min": 15,
        "tree": "intel", "status": "active",
        "categories": ["market", "projects", "analytics"],
        "owns": ["market_cap", "fdv", "circulating_supply", "total_supply", "official_links"],
        "access": "api_public", "has_api": True, "api_key_required": False,
        "is_new": False
    },
    {
        "id": "coinmarketcap", "name": "CoinMarketCap", "domain": "coinmarketcap.com",
        "tier": 2, "priority_score": 75, "sync_interval_min": 15,
        "tree": "intel", "status": "active",
        "categories": ["market", "projects", "ico", "activities"],
        "owns": [],
        "access": "api_key", "has_api": True, "api_key_required": True,
        "api_key_env": "CMC_API_KEY",
        "is_new": False
    },
    {
        "id": "tokenunlocks", "name": "TokenUnlocks", "domain": "token.unlocks.app",
        "tier": 2, "priority_score": 70, "sync_interval_min": 15,
        "tree": "intel", "status": "active",
        "categories": ["unlocks"],
        "owns": ["unlock_schedule", "unlock_amount_usd", "unlock_percent"],
        "access": "parser", "has_api": True, "api_key_required": False,
        "is_new": False
    },
    
    # ═══════════════════════════════════════════════════════════════
    # TIER 3 - ACTIVITIES / AIRDROPS (sync every 30 min)
    # airdrop campaigns, project activities, launches, events
    # ═══════════════════════════════════════════════════════════════
    {
        "id": "dropsearn", "name": "DropsEarn", "domain": "dropsearn.com",
        "tier": 3, "priority_score": 60, "sync_interval_min": 30,
        "tree": "intel", "status": "active",
        "categories": ["activities"],
        "owns": ["airdrop_campaigns", "ecosystem_activities"],
        "access": "parser", "has_api": False, "api_key_required": False,
        "is_new": False
    },
    {
        "id": "icodrops", "name": "ICO Drops", "domain": "icodrops.com",
        "tier": 3, "priority_score": 55, "sync_interval_min": 30,
        "tree": "intel", "status": "active",
        "categories": ["ico"],
        "owns": ["ico_sale", "launch"],
        "access": "parser", "has_api": False, "api_key_required": False,
        "is_new": False
    },
    {
        "id": "dappradar", "name": "DappRadar", "domain": "dappradar.com",
        "tier": 3, "priority_score": 50, "sync_interval_min": 30,
        "tree": "intel", "status": "active",
        "categories": ["defi", "projects"],
        "owns": [],
        "access": "parser", "has_api": True, "api_key_required": False,
        "is_new": False
    },
    {
        "id": "airdropalert", "name": "AirdropAlert", "domain": "airdropalert.com",
        "tier": 3, "priority_score": 45, "sync_interval_min": 30,
        "tree": "intel", "status": "active",
        "categories": ["activities"],
        "owns": [],
        "access": "parser", "has_api": False, "api_key_required": False,
        "is_new": False
    },
    
    # ═══════════════════════════════════════════════════════════════
    # NEWS SOURCES (handled by news_parser, separate pipeline)
    # ═══════════════════════════════════════════════════════════════
    {
        "id": "incrypted", "name": "Incrypted", "domain": "incrypted.com",
        "tier": 3, "priority_score": 65, "sync_interval_min": 15,
        "tree": "news", "status": "active",
        "categories": ["news", "analytics"],
        "owns": [],
        "access": "rss", "has_api": False, "api_key_required": False,
        "is_new": False
    },
    {
        "id": "cointelegraph", "name": "Cointelegraph", "domain": "cointelegraph.com",
        "tier": 3, "priority_score": 40, "sync_interval_min": 30,
        "tree": "news", "status": "active",
        "categories": ["news"],
        "owns": [],
        "access": "rss", "has_api": False, "api_key_required": False,
        "is_new": False
    },
    {
        "id": "theblock", "name": "The Block", "domain": "theblock.co",
        "tier": 3, "priority_score": 38, "sync_interval_min": 30,
        "tree": "news", "status": "active",
        "categories": ["news", "analytics"],
        "owns": [],
        "access": "rss", "has_api": False, "api_key_required": False,
        "is_new": False
    },
    {
        "id": "coindesk", "name": "CoinDesk", "domain": "coindesk.com",
        "tier": 3, "priority_score": 35, "sync_interval_min": 30,
        "tree": "news", "status": "active",
        "categories": ["news"],
        "owns": [],
        "access": "rss", "has_api": False, "api_key_required": False,
        "is_new": False
    },
    
    # ═══════════════════════════════════════════════════════════════
    # TIER 4 - RESEARCH DATA (sync every 3 hours)
    # Skip if no API key - NOT required for system to work
    # ═══════════════════════════════════════════════════════════════
    {
        "id": "messari", "name": "Messari", "domain": "messari.io",
        "tier": 4, "priority_score": 30, "sync_interval_min": 180,
        "tree": "intel", "status": "active",
        "categories": ["projects", "funding", "analytics", "news"],
        "owns": ["deep_research", "thesis", "tokenomics_notes", "project_description"],
        "access": "api_key", "has_api": True, "api_key_required": True,
        "api_key_env": "MESSARI_API_KEY",
        "is_new": False
    },
    
    # ═══════════════════════════════════════════════════════════════
    # PERSON / TEAM DATA (for graph: person ↔ fund ↔ project)
    # ═══════════════════════════════════════════════════════════════
    {
        "id": "github", "name": "GitHub", "domain": "github.com",
        "tier": 3, "priority_score": 42, "sync_interval_min": 60,
        "tree": "intel", "status": "active",
        "categories": ["persons", "projects"],
        "owns": [],
        "can_provide": ["developer_activity", "contributors", "team_members"],
        "access": "api_public", "has_api": True, "api_key_required": False,
        "api_key_env": "GITHUB_TOKEN",
        "is_new": False,
        "description": "Developer activity and repository metrics. Token increases rate limit from 60 to 5000 req/hour."
    },
    {
        "id": "twitter", "name": "Twitter/X", "domain": "twitter.com",
        "tier": 3, "priority_score": 40, "sync_interval_min": 60,
        "tree": "intel", "status": "planned",
        "categories": ["persons", "projects"],
        "owns": [],
        "access": "api_key", "has_api": True, "api_key_required": True,
        "is_new": False
    },
    {
        "id": "linkedin", "name": "LinkedIn", "domain": "linkedin.com",
        "tier": 4, "priority_score": 25, "sync_interval_min": 180,
        "tree": "intel", "status": "planned",
        "categories": ["persons"],
        "owns": ["person_positions", "worked_at"],
        "access": "api_key", "has_api": True, "api_key_required": True,
        "is_new": False
    },
]

# Exchange providers for Exchange Tree (separate scheduler)
EXCHANGE_PROVIDERS = [
    {"id": "binance", "name": "Binance", "type": "CEX", "status": "active", "tree": "exchange"},
    {"id": "bybit", "name": "Bybit", "type": "CEX", "status": "active", "tree": "exchange"},
    {"id": "hyperliquid", "name": "HyperLiquid", "type": "DEX", "status": "active", "tree": "exchange"},
    {"id": "okx", "name": "OKX", "type": "CEX", "status": "planned", "tree": "exchange"},
    {"id": "coinbase", "name": "Coinbase", "type": "CEX", "status": "planned", "tree": "exchange"},
]


async def create_indices(db):
    """Create MongoDB indices for performance."""
    logger.info("Creating indices...")
    
    # News Intelligence indices
    await db.raw_articles.create_index("id", unique=True)
    await db.raw_articles.create_index("content_hash")
    await db.raw_articles.create_index("source_id")
    await db.raw_articles.create_index("fetched_at")
    
    await db.normalized_articles.create_index("id", unique=True)
    await db.normalized_articles.create_index("source_id")
    await db.normalized_articles.create_index("published_at")
    
    await db.news_events.create_index("id", unique=True)
    await db.news_events.create_index("status")
    await db.news_events.create_index("event_type")
    await db.news_events.create_index("first_seen_at")
    await db.news_events.create_index("feed_score")
    await db.news_events.create_index([("primary_assets", 1)])
    
    # Intel indices
    await db.intel_persons.create_index("key", unique=True)
    await db.intel_exchanges.create_index("key", unique=True)
    await db.intel_projects.create_index("key", unique=True)
    await db.intel_investors.create_index("key", unique=True)
    await db.intel_docs.create_index("endpoint_id", unique=True)
    
    # GitHub developer activity indices
    await db.intel_github.create_index("id", unique=True)
    await db.intel_github.create_index("project_key")
    await db.intel_github.create_index("developer_activity.dev_score")
    
    # API Keys indices
    await db.api_keys.create_index("id", unique=True)
    await db.api_keys.create_index("service")
    await db.api_keys.create_index([("service", 1), ("enabled", 1)])
    
    # Data Sources indices
    await db.data_sources.create_index("id", unique=True)
    await db.data_sources.create_index("tier")
    await db.data_sources.create_index("status")
    await db.data_sources.create_index("tree")
    
    # System indices
    await db.system_proxies.create_index("id", unique=True)
    await db.news_sources.create_index("id", unique=True)
    await db.data_providers.create_index("id", unique=True)
    
    logger.info("Indices created successfully")


async def seed_persons(db, now):
    """Seed notable crypto persons."""
    count = 0
    for person in PERSONS_DATA:
        doc = {"key": f"seed:person:{person['slug']}", "source": "seed", **person, "updated_at": now}
        result = await db.intel_persons.update_one({"key": doc["key"]}, {"$set": doc}, upsert=True)
        if result.upserted_id:
            count += 1
    return count, len(PERSONS_DATA)


async def seed_exchanges(db, now):
    """Seed exchanges."""
    count = 0
    for exchange in EXCHANGES_DATA:
        doc = {"key": f"seed:exchange:{exchange['slug']}", "source": "seed", **exchange, "updated_at": now}
        result = await db.intel_exchanges.update_one({"key": doc["key"]}, {"$set": doc}, upsert=True)
        if result.upserted_id:
            count += 1
    return count, len(EXCHANGES_DATA)


async def seed_projects(db, now):
    """Seed projects/tokens."""
    count = 0
    for project in PROJECTS_DATA:
        doc = {"key": f"seed:project:{project['slug']}", "source": "seed", **project, "updated_at": now}
        result = await db.intel_projects.update_one({"key": doc["key"]}, {"$set": doc}, upsert=True)
        if result.upserted_id:
            count += 1
    return count, len(PROJECTS_DATA)


async def seed_investors(db, now):
    """Seed investors/VCs."""
    count = 0
    for investor in INVESTORS_DATA:
        doc = {"key": f"seed:investor:{investor['slug']}", "source": "seed", **investor, "updated_at": now}
        result = await db.intel_investors.update_one({"key": doc["key"]}, {"$set": doc}, upsert=True)
        if result.upserted_id:
            count += 1
    return count, len(INVESTORS_DATA)


async def seed_news_sources(db, now):
    """Seed news sources configuration with full data including category."""
    count = 0
    updated = 0
    
    # Weight mapping by tier
    tier_weights = {"A": 10, "B": 7, "C": 5, "D": 3}
    
    for source in NEWS_SOURCES_DATA:
        doc = {
            "id": source["id"],
            "name": source["name"],
            "domain": source["domain"],
            "tier": source["tier"],
            "language": source["language"],
            "category": source.get("category", "news"),  # Include category!
            "rss_url": source.get("rss_url"),
            "source_type": "rss",
            "is_active": True,
            "weight": tier_weights.get(source["tier"], 5),
            "updated_at": now
        }
        result = await db.news_sources.update_one(
            {"id": doc["id"]}, 
            {"$set": doc}, 
            upsert=True
        )
        if result.upserted_id:
            count += 1
        elif result.modified_count > 0:
            updated += 1
    
    logger.info(f"News sources: {count} new, {updated} updated, {len(NEWS_SOURCES_DATA)} total")
    return count, len(NEWS_SOURCES_DATA)


async def seed_data_providers(db, now):
    """Seed data providers configuration."""
    count = 0
    for provider in DATA_PROVIDERS_DATA:
        doc = {
            "id": provider["id"],
            "name": provider["name"],
            "type": provider["type"],
            "requires_api_key": provider["requires_key"],
            "rate_limit": provider["rate_limit"],
            "status": "active",
            "updated_at": now
        }
        result = await db.data_providers.update_one({"id": doc["id"]}, {"$set": doc}, upsert=True)
        if result.upserted_id:
            count += 1
    return count, len(DATA_PROVIDERS_DATA)


async def seed_data_sources(db, now):
    """Seed data sources for Discovery Engine."""
    count = 0
    updated = 0
    
    for source in DATA_SOURCES_DATA:
        doc = {
            "id": source["id"],
            "name": source["name"],
            "domain": source.get("domain", ""),
            "tier": source.get("tier", 3),
            "priority_score": source.get("priority_score", 50),
            "sync_interval_min": source.get("sync_interval_min", 30),
            "tree": source.get("tree", "intel"),
            "status": source.get("status", "active"),
            "categories": source.get("categories", []),
            "owns": source.get("owns", []),
            "can_provide": source.get("can_provide", []),
            "access": source.get("access", "parser"),
            "has_api": source.get("has_api", False),
            "api_key_required": source.get("api_key_required", False),
            "api_key_env": source.get("api_key_env"),
            "is_new": source.get("is_new", False),
            "description": source.get("description", ""),
            "updated_at": now
        }
        result = await db.data_sources.update_one(
            {"id": doc["id"]}, 
            {"$set": doc}, 
            upsert=True
        )
        if result.upserted_id:
            count += 1
        elif result.modified_count > 0:
            updated += 1
    
    logger.info(f"Data sources: {count} new, {updated} updated, {len(DATA_SOURCES_DATA)} total")
    return count, len(DATA_SOURCES_DATA)


async def seed_exchange_providers(db, now):
    """Seed exchange providers for Exchange Tree scheduler."""
    count = 0
    
    for provider in EXCHANGE_PROVIDERS:
        doc = {
            "id": provider["id"],
            "name": provider["name"],
            "type": provider.get("type", "CEX"),
            "status": provider.get("status", "planned"),
            "tree": "exchange",
            "updated_at": now
        }
        result = await db.exchange_providers.update_one(
            {"id": doc["id"]},
            {"$set": doc},
            upsert=True
        )
        if result.upserted_id:
            count += 1
    
    logger.info(f"Exchange providers: {count} new / {len(EXCHANGE_PROVIDERS)} total")
    return count, len(EXCHANGE_PROVIDERS)


async def seed_api_docs(db, now):
    """Seed API documentation."""
    try:
        from modules.intel.api.documentation_registry import API_DOCUMENTATION
        count = 0
        for endpoint in API_DOCUMENTATION:
            doc = {
                "endpoint_id": endpoint.endpoint_id,
                "path": endpoint.path,
                "method": endpoint.method.value,
                "title_en": endpoint.title_en,
                "title_ru": endpoint.title_ru,
                "description_en": endpoint.description_en,
                "description_ru": endpoint.description_ru,
                "category": endpoint.category,
                "tags": endpoint.tags,
                "updated_at": now
            }
            result = await db.intel_docs.update_one(
                {"endpoint_id": doc["endpoint_id"]},
                {"$set": doc},
                upsert=True
            )
            if result.upserted_id:
                count += 1
        return count, len(API_DOCUMENTATION)
    except Exception as e:
        logger.error(f"Failed to seed API docs: {e}")
        return 0, 0


# Sample AI-generated news stories for demonstration
SAMPLE_NEWS_EVENTS = [
    {
        "id": "evt_sample_solana_etf",
        "title_seed": "SEC Expected to Approve Solana Spot ETF Applications in Q2 2026",
        "title_en": "SEC Expected to Approve Solana Spot ETF Applications in Q2 2026",
        "title_ru": "SEC планирует одобрить заявки на спотовый ETF Solana во втором квартале 2026 года",
        "summary_en": "The U.S. SEC is expected to approve spot Solana (SOL) ETF applications in Q2 2026, expanding its spot crypto ETF lineup after Bitcoin and Ethereum.",
        "summary_ru": "Ожидается, что SEC США одобрит заявки на спотовый ETF Solana (SOL) во втором квартале 2026 года, расширив линейку спотовых крипто ETF после Bitcoin и Ethereum.",
        "event_type": "regulation",
        "status": "confirmed",
        "primary_assets": ["SOL", "BTC", "ETH"],
        "primary_entities": ["SEC", "Solana"],
        "source_count": 3,
        "article_count": 5,
        "confidence_score": 0.95,
        "feed_score": 0.99,
        "fomo_score": 92.0,
        "is_sample": True,
        "key_facts": [
            "SEC reviewing multiple Solana ETF applications",
            "Expected approval timeline: Q2 2026",
            "Follows successful Bitcoin and Ethereum ETF launches"
        ]
    },
    {
        "id": "evt_sample_kazakhstan_btc",
        "title_seed": "Kazakhstan Central Bank Allocates $350M to BTC and ETH Reserves",
        "title_en": "Kazakhstan Central Bank Allocates $350M to BTC and ETH Reserves",
        "title_ru": "Центральный банк Казахстана выделяет $350 млн на резервы в BTC и ETH",
        "summary_en": "Kazakhstan's central bank announced it will allocate $350 million to build reserves in Bitcoin (BTC) and Ethereum (ETH), marking a major state-level move.",
        "summary_ru": "Центральный банк Казахстана объявил о выделении $350 миллионов на формирование резервов в Bitcoin (BTC) и Ethereum (ETH), что является крупным шагом на государственном уровне.",
        "event_type": "funding",
        "status": "confirmed",
        "primary_assets": ["BTC", "ETH"],
        "primary_entities": ["Kazakhstan", "Central Bank"],
        "source_count": 2,
        "article_count": 3,
        "confidence_score": 0.90,
        "feed_score": 0.98,
        "fomo_score": 88.0,
        "is_sample": True,
        "key_facts": [
            "$350 million allocation announced",
            "Split between Bitcoin and Ethereum",
            "Part of national digital reserve strategy"
        ]
    },
    {
        "id": "evt_sample_eth_l2",
        "title_seed": "Ethereum L2s Hit $5B Daily Volume as Gas Fees Fall to Record Lows",
        "title_en": "Ethereum L2s Hit $5B Daily Volume as Ethereum Gas Fees Fall to Record Lows",
        "title_ru": "Объем L2 сетей Ethereum достиг $5 млрд в сутки на фоне рекордно низких комиссий",
        "summary_en": "Ethereum Layer 2 networks processed a record $5B in daily volume as Ethereum gas fees fell to historic lows, highlighting the shift to scaling solutions.",
        "summary_ru": "Сети второго уровня Ethereum обработали рекордные $5 млрд суточного объема на фоне падения комиссий до исторических минимумов.",
        "event_type": "news",
        "status": "confirmed",
        "primary_assets": ["ETH", "ARB", "OP", "BASE"],
        "primary_entities": ["Ethereum", "Arbitrum", "Optimism", "Base"],
        "source_count": 4,
        "article_count": 6,
        "confidence_score": 0.92,
        "feed_score": 0.97,
        "fomo_score": 85.0,
        "is_sample": True,
        "key_facts": [
            "$5 billion daily transaction volume",
            "Gas fees at historic lows",
            "Major shift to L2 scaling solutions"
        ]
    }
]


async def seed_sample_news_events(db, now):
    """Seed sample AI-generated news events for demonstration."""
    count = 0
    for event in SAMPLE_NEWS_EVENTS:
        event_data = {
            **event,
            "created_at": now.isoformat(),
            "first_seen_at": now.isoformat(),
            "updated_at": now.isoformat()
        }
        result = await db.news_events.update_one(
            {"id": event["id"]},
            {"$set": event_data},
            upsert=True
        )
        if result.upserted_id:
            count += 1
    return count, len(SAMPLE_NEWS_EVENTS)


async def seed_api_keys_from_env(db, now):
    """
    Load API keys from environment variables to database.
    This allows keys to be managed via UI while respecting .env values.
    """
    from modules.intel.api_keys_manager import SERVICE_CONFIG, get_api_keys_manager
    
    manager = get_api_keys_manager(db)
    loaded = 0
    
    # Map of env vars to service IDs
    ENV_TO_SERVICE = {
        "GITHUB_TOKEN": "github",
        "COINGECKO_API_KEY": "coingecko",
        "CMC_API_KEY": "coinmarketcap",
        "MESSARI_API_KEY": "messari",
        "TWITTER_BEARER_TOKEN": "twitter",
        "OPENAI_API_KEY": "openai"
    }
    
    for env_var, service_id in ENV_TO_SERVICE.items():
        key_value = os.environ.get(env_var)
        if key_value and len(key_value) > 5:
            # Check if already exists in DB
            existing = await db.api_keys.find_one({"service": service_id})
            if not existing:
                # Add to DB
                result = await manager.add_key(
                    service_id, 
                    key_value, 
                    f"{service_id}-env-key",
                    is_pro=True
                )
                if result.get("ok"):
                    loaded += 1
                    logger.info(f"[API Keys] Loaded {service_id} from {env_var}")
    
    return loaded


async def get_stats(db):
    """Get current database statistics."""
    return {
        "persons": await db.intel_persons.count_documents({}),
        "exchanges": await db.intel_exchanges.count_documents({}),
        "projects": await db.intel_projects.count_documents({}),
        "investors": await db.intel_investors.count_documents({}),
        "news_sources": await db.news_sources.count_documents({}),
        "data_providers": await db.data_providers.count_documents({}),
        "api_docs": await db.intel_docs.count_documents({}),
        "raw_articles": await db.raw_articles.count_documents({}),
        "normalized_articles": await db.normalized_articles.count_documents({}),
        "news_events": await db.news_events.count_documents({}),
        "proxies": await db.system_proxies.count_documents({}),
    }


async def run_bootstrap(force: bool = False):
    """Run complete bootstrap."""
    mongo_url = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
    db_name = os.environ.get("DB_NAME", "fomo_market")
    
    logger.info(f"Connecting to MongoDB: {mongo_url}")
    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]
    
    now = datetime.now(timezone.utc)
    results = {"seeded": {}, "totals": {}}
    
    logger.info("=" * 60)
    logger.info("FOMO Platform Bootstrap")
    logger.info("=" * 60)
    
    # Get initial stats
    initial_stats = await get_stats(db)
    logger.info(f"Initial stats: {initial_stats}")
    
    # Create indices
    await create_indices(db)
    
    # Seed data
    new, total = await seed_persons(db, now)
    results["seeded"]["persons"] = f"{new} new / {total} total"
    logger.info(f"Persons: {new} new / {total} total")
    
    new, total = await seed_exchanges(db, now)
    results["seeded"]["exchanges"] = f"{new} new / {total} total"
    logger.info(f"Exchanges: {new} new / {total} total")
    
    new, total = await seed_projects(db, now)
    results["seeded"]["projects"] = f"{new} new / {total} total"
    logger.info(f"Projects: {new} new / {total} total")
    
    new, total = await seed_investors(db, now)
    results["seeded"]["investors"] = f"{new} new / {total} total"
    logger.info(f"Investors: {new} new / {total} total")
    
    new, total = await seed_news_sources(db, now)
    results["seeded"]["news_sources"] = f"{new} new / {total} total"
    logger.info(f"News Sources: {new} new / {total} total")
    
    new, total = await seed_data_providers(db, now)
    results["seeded"]["data_providers"] = f"{new} new / {total} total"
    logger.info(f"Data Providers: {new} new / {total} total")
    
    new, total = await seed_data_sources(db, now)
    results["seeded"]["data_sources"] = f"{new} new / {total} total"
    logger.info(f"Data Sources (Discovery): {new} new / {total} total")
    
    new, total = await seed_exchange_providers(db, now)
    results["seeded"]["exchange_providers"] = f"{new} new / {total} total"
    logger.info(f"Exchange Providers: {new} new / {total} total")
    
    new, total = await seed_api_docs(db, now)
    results["seeded"]["api_docs"] = f"{new} new / {total} total"
    logger.info(f"API Docs: {new} new / {total} total")
    
    new, total = await seed_sample_news_events(db, now)
    results["seeded"]["sample_news_events"] = f"{new} new / {total} total"
    logger.info(f"Sample News Events: {new} new / {total} total")
    
    # ═══════════════════════════════════════════════════════════════
    # LOAD API KEYS FROM ENVIRONMENT
    # ═══════════════════════════════════════════════════════════════
    try:
        await seed_api_keys_from_env(db, now)
        results["seeded"]["api_keys"] = "checked"
        logger.info("API Keys: loaded from environment")
    except Exception as e:
        logger.warning(f"API Keys load failed: {e}")
        results["seeded"]["api_keys"] = f"failed: {e}"
    
    # ═══════════════════════════════════════════════════════════════
    # KNOWLEDGE GRAPH BOOTSTRAP
    # ═══════════════════════════════════════════════════════════════
    try:
        from modules.knowledge_graph.alias_resolver import bootstrap_common_aliases, EntityAliasResolver
        from modules.knowledge_graph.builder import GraphBuilder
        
        # Bootstrap entity aliases
        alias_count = await db.entity_aliases.count_documents({})
        if alias_count == 0 or force:
            logger.info("Bootstrapping entity aliases...")
            count = await bootstrap_common_aliases(db)
            results["seeded"]["entity_aliases"] = f"{count} aliases"
            logger.info(f"Entity Aliases: {count} bootstrapped")
        else:
            results["seeded"]["entity_aliases"] = f"{alias_count} existing"
            logger.info(f"Entity Aliases: {alias_count} existing (skipped)")
        
        # Build Knowledge Graph
        graph_nodes = await db.graph_nodes.count_documents({})
        if graph_nodes == 0 or force:
            logger.info("Building Knowledge Graph...")
            builder = GraphBuilder(db)
            snapshot = await builder.full_rebuild()
            results["seeded"]["graph_nodes"] = f"{snapshot.node_count} nodes"
            results["seeded"]["graph_edges"] = f"{snapshot.edge_count} edges"
            logger.info(f"Knowledge Graph: {snapshot.node_count} nodes, {snapshot.edge_count} edges")
        else:
            graph_edges = await db.graph_edges.count_documents({})
            results["seeded"]["graph_nodes"] = f"{graph_nodes} existing"
            results["seeded"]["graph_edges"] = f"{graph_edges} existing"
            logger.info(f"Knowledge Graph: {graph_nodes} nodes, {graph_edges} edges (skipped)")
            
    except Exception as e:
        logger.warning(f"Knowledge Graph bootstrap failed: {e}")
        results["seeded"]["knowledge_graph"] = f"failed: {e}"
    
    # Get final stats
    final_stats = await get_stats(db)
    results["totals"] = final_stats
    
    logger.info("=" * 60)
    logger.info("Bootstrap Complete!")
    logger.info(f"Final stats: {final_stats}")
    logger.info("=" * 60)
    
    # Preserved data summary
    preserved = {
        "raw_articles": final_stats["raw_articles"],
        "normalized_articles": final_stats["normalized_articles"],
        "news_events": final_stats["news_events"],
    }
    logger.info(f"Preserved data: {preserved}")
    
    return results


if __name__ == "__main__":
    force = "--force" in sys.argv
    asyncio.run(run_bootstrap(force=force))
