"""
Graph API Routes

Headless Graph Engine endpoints for:
- Internal Dashboard
- UA Project
- Any external consumer

Endpoints:
- GET /api/graph/search
- GET /api/graph/ego/{type}/{id}
- GET /api/graph/expand/{node_id}
- GET /api/graph/path
- GET /api/graph/related/{type}/{id}
- GET /api/graph/ui/{type}/{id} - Ready-to-render payload
"""

from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from datetime import datetime, timezone, timedelta

router = APIRouter(prefix="/api/graph", tags=["Graph API"])


# =============================================================================
# SEARCH
# =============================================================================

@router.get("/search")
async def search_entities(
    q: str = Query(..., min_length=2, description="Search query"),
    limit: int = Query(20, ge=1, le=50)
):
    """
    Search for entities in the graph
    
    Returns: [{type, id, label}]
    """
    from server import db
    from modules.knowledge_graph.graph_api_service import GraphQueryService
    
    service = GraphQueryService(db)
    results = await service.search(q, limit=limit)
    
    return {"results": results, "query": q}


# =============================================================================
# EGO GRAPH
# =============================================================================

@router.get("/ego/{entity_type}/{entity_id}")
async def get_ego_graph(
    entity_type: str,
    entity_id: str,
    depth: int = Query(1, ge=1, le=3),
    max_nodes: int = Query(30, ge=10, le=80),
    max_edges: int = Query(40, ge=10, le=120),
    include_derived: bool = Query(False, description="Include derived relations"),
    no_cache: bool = Query(False, description="Skip cache")
):
    """
    Get ego graph (subgraph around an entity)
    
    This is the main endpoint for graph visualization.
    Returns nodes and edges ready for frontend rendering.
    
    Graph Explosion Protection:
    - Max depth: 3
    - Max nodes: 80
    - Max edges: 120
    - Hub suppression: 25 edges per hub
    """
    from server import db
    from modules.knowledge_graph.graph_api_service import GraphQueryService
    
    service = GraphQueryService(db)
    
    result = await service.get_ego_graph(
        entity_type=entity_type,
        entity_id=entity_id,
        depth=depth,
        max_nodes=max_nodes,
        max_edges=max_edges,
        include_derived=include_derived,
        use_cache=not no_cache
    )
    
    return result.dict()


@router.get("/ui/{entity_type}/{entity_id}")
async def get_ui_ready_graph(
    entity_type: str,
    entity_id: str,
    mode: str = Query("default", description="default or research")
):
    """
    Get UI-ready graph payload
    
    Simplified endpoint that returns optimal graph for visualization.
    
    Modes:
    - default: depth=1, max_nodes=30, no derived edges
    - research: depth=2, max_nodes=60, includes derived edges
    """
    from server import db
    from modules.knowledge_graph.graph_api_service import GraphQueryService
    
    service = GraphQueryService(db)
    
    if mode == "research":
        result = await service.get_ego_graph(
            entity_type=entity_type,
            entity_id=entity_id,
            depth=2,
            max_nodes=60,
            max_edges=80,
            include_derived=True
        )
    else:
        result = await service.get_ego_graph(
            entity_type=entity_type,
            entity_id=entity_id,
            depth=1,
            max_nodes=30,
            max_edges=40,
            include_derived=False
        )
    
    return result.dict()


# =============================================================================
# EXPAND NODE
# =============================================================================

@router.get("/expand/{node_id}")
async def expand_node(
    node_id: str,
    relation_type: Optional[str] = None,
    limit: int = Query(20, ge=5, le=30)
):
    """
    Expand a node - get its neighbors
    
    Called when user clicks on a node in the graph.
    Returns new nodes and edges to add to the visualization.
    """
    from server import db
    from modules.knowledge_graph.graph_api_service import GraphQueryService
    
    service = GraphQueryService(db)
    
    result = await service.expand_node(
        node_id=node_id,
        relation_type=relation_type,
        limit=limit
    )
    
    return result


# =============================================================================
# PATH SEARCH
# =============================================================================

@router.get("/path")
async def find_path(
    from_type: str = Query(..., description="Source entity type"),
    from_id: str = Query(..., description="Source entity ID"),
    to_type: str = Query(..., description="Target entity type"),
    to_id: str = Query(..., description="Target entity ID"),
    max_depth: int = Query(4, ge=2, le=6)
):
    """
    Find path between two entities
    
    Uses BFS to find shortest connection path.
    
    Example:
    /api/graph/path?from_type=person&from_id=vitalik_buterin&to_type=fund&to_id=a16z
    """
    from server import db
    from modules.knowledge_graph.graph_api_service import GraphQueryService
    
    service = GraphQueryService(db)
    
    result = await service.find_path(
        from_type=from_type,
        from_id=from_id,
        to_type=to_type,
        to_id=to_id,
        max_depth=max_depth
    )
    
    return result


# =============================================================================
# RELATED ENTITIES
# =============================================================================

@router.get("/related/{entity_type}/{entity_id}")
async def get_related_entities(
    entity_type: str,
    entity_id: str,
    limit: int = Query(30, ge=5, le=50)
):
    """
    Get related entities (flat list)
    
    For sidebars, cards, and non-graph UIs.
    Returns list of related entities with relation type.
    """
    from server import db
    from modules.knowledge_graph.graph_api_service import GraphQueryService
    
    service = GraphQueryService(db)
    
    related = await service.get_related(
        entity_type=entity_type,
        entity_id=entity_id,
        limit=limit
    )
    
    return {
        "entity": f"{entity_type}:{entity_id}",
        "related": related,
        "count": len(related)
    }


# =============================================================================
# GRAPH STATS
# =============================================================================

@router.get("/stats")
async def get_graph_stats():
    """Get graph statistics"""
    from server import db
    
    node_count = await db.graph_nodes.count_documents({})
    edge_count = await db.graph_edges.count_documents({})
    
    # Count by type
    node_types = await db.graph_nodes.aggregate([
        {"$group": {"_id": "$entity_type", "count": {"$sum": 1}}}
    ]).to_list(length=20)
    
    edge_types = await db.graph_edges.aggregate([
        {"$group": {"_id": "$relation_type", "count": {"$sum": 1}}}
    ]).to_list(length=50)
    
    return {
        "total_nodes": node_count,
        "total_edges": edge_count,
        "nodes_by_type": {t["_id"]: t["count"] for t in node_types},
        "edges_by_type": {t["_id"]: t["count"] for t in edge_types}
    }


# =============================================================================
# NARRATIVE EARLY DETECTION API
# =============================================================================

@router.get("/narratives/all")
async def get_all_narratives(limit: int = Query(30, ge=5, le=100)):
    """Get all active narratives with emergence scores"""
    from server import db
    from modules.narrative.early_detection import NarrativeEarlyDetector
    
    detector = NarrativeEarlyDetector(db)
    narratives = await detector.get_all_active_narratives(limit=limit)
    
    return {"narratives": narratives, "count": len(narratives)}


@router.get("/narratives/emerging")
async def get_emerging_narratives_v2(limit: int = Query(10, ge=1, le=50)):
    """
    Get emerging narratives (early detection)
    
    These are narratives just starting to form
    """
    from server import db
    from modules.narrative.early_detection import NarrativeEarlyDetector
    
    detector = NarrativeEarlyDetector(db)
    narratives = await detector.get_emerging_narratives(limit=limit)
    
    return {"narratives": narratives, "lifecycle": "emerging"}


@router.get("/narratives/growing")
async def get_growing_narratives(limit: int = Query(10, ge=1, le=50)):
    """Get growing narratives"""
    from server import db
    from modules.narrative.early_detection import NarrativeEarlyDetector
    
    detector = NarrativeEarlyDetector(db)
    narratives = await detector.get_growing_narratives(limit=limit)
    
    return {"narratives": narratives, "lifecycle": "growing"}


@router.get("/narratives/dominant")
async def get_dominant_narratives(limit: int = Query(10, ge=1, le=50)):
    """Get dominant narratives (peak attention)"""
    from server import db
    from modules.narrative.early_detection import NarrativeEarlyDetector
    
    detector = NarrativeEarlyDetector(db)
    narratives = await detector.get_dominant_narratives(limit=limit)
    
    return {"narratives": narratives, "lifecycle": "dominant"}


@router.post("/narratives/detect")
async def trigger_narrative_detection():
    """Manually trigger narrative early detection"""
    from server import db
    from modules.narrative.early_detection import NarrativeEarlyDetector
    
    detector = NarrativeEarlyDetector(db)
    emerging = await detector.detect_emerging_narratives()
    
    return {
        "status": "completed",
        "detected": len(emerging),
        "narratives": [{"id": n.id, "name": n.name, "lifecycle": n.lifecycle, "score": n.emergence_score} for n in emerging]
    }


# =============================================================================
# TEMPORAL GRAPH API
# =============================================================================

@router.get("/temporal/at")
async def get_graph_at_time(
    entity_type: str = Query(...),
    entity_id: str = Query(...),
    time: str = Query(..., description="ISO datetime or YYYY-MM-DD"),
    depth: int = Query(1, ge=1, le=2)
):
    """
    Get graph state at specific point in time
    
    Example: /api/graph/temporal/at?entity_type=project&entity_id=arbitrum&time=2023-01-01
    """
    from server import db
    from modules.knowledge_graph.temporal_graph import TemporalGraphService
    from datetime import datetime
    
    # Parse time
    try:
        if "T" in time:
            at_time = datetime.fromisoformat(time.replace("Z", "+00:00"))
        else:
            at_time = datetime.strptime(time, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except:
        raise HTTPException(status_code=400, detail="Invalid time format. Use YYYY-MM-DD or ISO datetime")
    
    service = TemporalGraphService(db)
    result = await service.get_graph_at_time(entity_type, entity_id, at_time, depth)
    
    return result


@router.get("/temporal/evolution/{entity_type}/{entity_id}")
async def get_network_evolution(
    entity_type: str,
    entity_id: str,
    days: int = Query(365, ge=30, le=1095)
):
    """
    Get how entity's network evolved over time
    
    Returns timeline of edge creations
    """
    from server import db
    from modules.knowledge_graph.temporal_graph import TemporalGraphService
    
    service = TemporalGraphService(db)
    
    to_date = datetime.now(timezone.utc)
    from_date = to_date - timedelta(days=days)
    
    evolution = await service.get_network_evolution(
        entity_type, entity_id, from_date, to_date
    )
    
    return {
        "entity": f"{entity_type}:{entity_id}",
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "events": evolution,
        "count": len(evolution)
    }


@router.post("/temporal/snapshot")
async def create_graph_snapshot(label: Optional[str] = None):
    """Create snapshot of current graph state"""
    from server import db
    from modules.knowledge_graph.temporal_graph import TemporalGraphService
    
    service = TemporalGraphService(db)
    snapshot = await service.create_snapshot(label)
    
    return snapshot.dict()


@router.get("/temporal/snapshots")
async def get_graph_snapshots(limit: int = Query(20, ge=1, le=50)):
    """Get list of graph snapshots"""
    from server import db
    
    cursor = db.graph_snapshots.find().sort("snapshot_time", -1).limit(limit)
    snapshots = await cursor.to_list(length=limit)
    
    return {"snapshots": snapshots}


# =============================================================================
# GRAPH METRICS API
# =============================================================================

@router.get("/metrics/summary")
async def get_graph_metrics_summary():
    """Get overall graph metrics"""
    from server import db
    from modules.knowledge_graph.temporal_graph import GraphMetricsService
    
    service = GraphMetricsService(db)
    return await service.get_graph_summary_metrics()


@router.get("/metrics/node/{entity_type}/{entity_id}")
async def get_node_metrics(entity_type: str, entity_id: str):
    """Get metrics for specific node"""
    from server import db
    from modules.knowledge_graph.temporal_graph import GraphMetricsService
    
    # Find node
    node = await db.graph_nodes.find_one({
        "entity_type": entity_type,
        "entity_id": entity_id
    })
    
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    
    service = GraphMetricsService(db)
    metrics = await service.calculate_node_metrics(node["id"])
    
    return metrics.dict() if metrics else {"error": "Failed to calculate"}


@router.get("/metrics/top-influence")
async def get_top_influence_nodes(limit: int = Query(20, ge=5, le=50)):
    """Get nodes with highest influence score"""
    from server import db
    from modules.knowledge_graph.temporal_graph import GraphMetricsService
    
    service = GraphMetricsService(db)
    top_nodes = await service.get_top_by_influence(limit)
    
    return {"nodes": top_nodes}


@router.post("/metrics/calculate-all")
async def calculate_all_node_metrics():
    """Trigger calculation of metrics for all nodes"""
    from server import db
    from modules.knowledge_graph.temporal_graph import GraphMetricsService
    
    service = GraphMetricsService(db)
    stats = await service.calculate_all_metrics()
    
    return {"status": "completed", "stats": stats}


# =============================================================================
# TOPIC API
# =============================================================================

@router.get("/topics/trending")
async def get_trending_topics(limit: int = Query(20, ge=5, le=50)):
    """Get trending topics"""
    from server import db
    from modules.intelligence.topic_layer import TopicService
    
    service = TopicService(db)
    topics = await service.get_trending_topics(limit=limit)
    
    return {"topics": topics}


@router.get("/topics/{topic_id}/events")
async def get_topic_events(topic_id: str, limit: int = Query(50, ge=10, le=100)):
    """Get events for a topic"""
    from server import db
    from modules.intelligence.topic_layer import TopicService
    
    service = TopicService(db)
    event_ids = await service.get_events_for_topic(topic_id, limit=limit)
    
    return {"topic_id": topic_id, "event_ids": event_ids, "count": len(event_ids)}
