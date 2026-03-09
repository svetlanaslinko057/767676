"""
FOMO Sentiment Engine API Routes
"""

from fastapi import APIRouter, Query, Body, HTTPException
from typing import Optional, List
from pydantic import BaseModel
from datetime import datetime, timezone

from .engine import SentimentEngine
from .providers import ProviderType

router = APIRouter(prefix="/api/sentiment", tags=["sentiment"])

# Global engine instance
_engine: Optional[SentimentEngine] = None


def get_engine() -> SentimentEngine:
    """Get or create engine instance"""
    global _engine
    if _engine is None:
        _engine = SentimentEngine()
    return _engine


def set_database(db):
    """Set database for engine"""
    global _engine
    if _engine:
        _engine.db = db
    else:
        _engine = SentimentEngine(db)


# Request/Response Models
class AnalyzeRequest(BaseModel):
    text: str
    context: Optional[dict] = None


class BatchAnalyzeRequest(BaseModel):
    texts: List[str]


class ProviderResult(BaseModel):
    provider: str
    model: str
    score: float
    confidence: float
    label: str
    factors: List[str] = []
    error: Optional[str] = None
    latency_ms: int = 0


class SentimentResponse(BaseModel):
    """Full sentiment response with consensus and FOMO score"""
    # Consensus (summary from all providers)
    consensus: dict  # score, confidence, label
    
    # FOMO custom score
    fomo: dict  # score, confidence, available
    
    # Individual providers
    providers: List[dict]
    
    # Metadata
    meta: dict


class EngineStatusResponse(BaseModel):
    providers_configured: int
    providers_available: int
    providers: dict
    has_api_key: bool
    fomo_enabled: bool


# ═══════════════════════════════════════════════════════════════
# ROUTES
# ═══════════════════════════════════════════════════════════════

@router.get("/status")
async def get_status() -> EngineStatusResponse:
    """Get sentiment engine status"""
    engine = get_engine()
    return engine.get_status()


@router.post("/analyze")
async def analyze_sentiment(request: AnalyzeRequest) -> SentimentResponse:
    """
    Analyze text sentiment with multi-provider consensus.
    
    Returns:
    - consensus: Weighted average from all active providers
    - fomo: FOMO custom sentiment (0 if not configured)
    - providers: Individual results from each provider
    """
    engine = get_engine()
    
    if not request.text or len(request.text.strip()) < 10:
        raise HTTPException(status_code=400, detail="Text must be at least 10 characters")
    
    result = await engine.analyze(request.text, request.context)
    
    return SentimentResponse(
        consensus={
            "score": result.consensus_score,
            "confidence": result.consensus_confidence,
            "label": result.consensus_label,
            "providers_used": result.providers_used
        },
        fomo={
            "score": result.fomo_score,
            "confidence": result.fomo_confidence,
            "available": result.fomo_available,
            "label": "positive" if result.fomo_score > 0.15 else ("negative" if result.fomo_score < -0.15 else "neutral")
        },
        providers=[
            {
                "provider": p.provider,
                "model": p.model,
                "score": p.score,
                "confidence": p.confidence,
                "label": p.label,
                "factors": p.factors,
                "latency_ms": p.latency_ms,
                "error": p.error
            }
            for p in result.providers
        ],
        meta={
            "analyzed_at": result.analyzed_at,
            "text_preview": result.text_preview,
            "providers_available": result.providers_available
        }
    )


@router.post("/analyze/batch")
async def analyze_batch(request: BatchAnalyzeRequest) -> List[SentimentResponse]:
    """Analyze multiple texts in batch"""
    engine = get_engine()
    
    if not request.texts:
        raise HTTPException(status_code=400, detail="At least one text required")
    
    if len(request.texts) > 50:
        raise HTTPException(status_code=400, detail="Maximum 50 texts per batch")
    
    results = await engine.analyze_batch(request.texts)
    
    return [
        SentimentResponse(
            consensus={
                "score": r.consensus_score,
                "confidence": r.consensus_confidence,
                "label": r.consensus_label,
                "providers_used": r.providers_used
            },
            fomo={
                "score": r.fomo_score,
                "confidence": r.fomo_confidence,
                "available": r.fomo_available,
                "label": "positive" if r.fomo_score > 0.15 else ("negative" if r.fomo_score < -0.15 else "neutral")
            },
            providers=[
                {
                    "provider": p.provider,
                    "model": p.model,
                    "score": p.score,
                    "confidence": p.confidence,
                    "label": p.label,
                    "factors": p.factors,
                    "latency_ms": p.latency_ms,
                    "error": p.error
                }
                for p in r.providers
            ],
            meta={
                "analyzed_at": r.analyzed_at,
                "text_preview": r.text_preview,
                "providers_available": r.providers_available
            }
        )
        for r in results
    ]


@router.get("/providers")
async def list_providers():
    """List all available sentiment providers"""
    engine = get_engine()
    status = engine.get_status()
    
    return {
        "providers": [
            {
                "id": pid,
                "name": pid.upper() if pid != "fomo" else "FOMO Sentiment",
                "model": info["model"],
                "weight": info["weight"],
                "enabled": info["enabled"],
                "available": info["available"],
                "description": _get_provider_description(pid)
            }
            for pid, info in status["providers"].items()
        ],
        "summary": {
            "total": status["providers_configured"],
            "available": status["providers_available"],
            "has_api_key": status["has_api_key"]
        }
    }


@router.post("/providers/{provider_id}/enable")
async def enable_provider(provider_id: str, enabled: bool = True):
    """Enable or disable a sentiment provider"""
    engine = get_engine()
    
    try:
        provider_type = ProviderType(provider_id.lower())
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Unknown provider: {provider_id}")
    
    await engine.enable_provider(provider_type, enabled)
    
    return {
        "ok": True,
        "provider": provider_id,
        "enabled": enabled,
        "message": f"Provider {provider_id} {'enabled' if enabled else 'disabled'}"
    }


@router.get("/cache/stats")
async def get_cache_stats():
    """Get sentiment cache statistics"""
    from modules.scheduler.sentiment_scheduler import get_sentiment_scheduler
    
    scheduler = get_sentiment_scheduler()
    if not scheduler:
        return {"error": "Scheduler not initialized", "total_cached": 0}
    
    return await scheduler.get_cache_stats()


@router.get("/cache/{event_id}")
async def get_cached_sentiment(event_id: str):
    """Get cached sentiment for a specific event"""
    from modules.scheduler.sentiment_scheduler import get_sentiment_scheduler
    
    scheduler = get_sentiment_scheduler()
    if not scheduler:
        raise HTTPException(status_code=503, detail="Scheduler not initialized")
    
    cached = await scheduler.get_cached_sentiment(event_id)
    if not cached:
        raise HTTPException(status_code=404, detail=f"No cached sentiment for event {event_id}")
    
    return cached


@router.post("/cache/batch")
async def get_cached_sentiments_batch(event_ids: List[str] = Body(..., embed=False)):
    """Get cached sentiments for multiple events"""
    from modules.scheduler.sentiment_scheduler import get_sentiment_scheduler
    
    scheduler = get_sentiment_scheduler()
    if not scheduler:
        raise HTTPException(status_code=503, detail="Scheduler not initialized")
    
    if len(event_ids) > 100:
        raise HTTPException(status_code=400, detail="Maximum 100 event IDs per request")
    
    results = await scheduler.get_cached_sentiments_batch(event_ids)
    
    return {
        "requested": len(event_ids),
        "found": len(results),
        "sentiments": results
    }


@router.get("/scheduler/status")
async def get_scheduler_status():
    """Get sentiment scheduler status"""
    from modules.scheduler.sentiment_scheduler import get_sentiment_scheduler
    
    scheduler = get_sentiment_scheduler()
    if not scheduler:
        return {"running": False, "error": "Scheduler not initialized"}
    
    return scheduler.get_status()


@router.post("/scheduler/run-now")
async def run_analysis_now():
    """Manually trigger sentiment analysis job"""
    from modules.scheduler.sentiment_scheduler import get_sentiment_scheduler
    
    scheduler = get_sentiment_scheduler()
    if not scheduler:
        raise HTTPException(status_code=503, detail="Scheduler not initialized")
    
    result = await scheduler._run_sentiment_analysis()
    return {
        "ok": True,
        "result": result,
        "message": "Sentiment analysis completed"
    }


def _get_provider_description(provider_id: str) -> str:
    """Get provider description"""
    descriptions = {
        "fomo": "FOMO proprietary crypto sentiment model with keyword analysis",
        "openai": "OpenAI GPT-based sentiment analysis",
        "anthropic": "Anthropic Claude-based sentiment analysis",
        "gemini": "Google Gemini-based sentiment analysis"
    }
    return descriptions.get(provider_id, "Custom sentiment provider")
