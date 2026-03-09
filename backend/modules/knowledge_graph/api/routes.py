"""
Knowledge Graph API Routes

Endpoints:
- GET /api/graph/network - Full network or ego-network
- GET /api/graph/node/{type}/{id} - Get single node
- GET /api/graph/edges/{type}/{id} - Get edges for node
- GET /api/graph/neighbors/{type}/{id} - Get neighbor nodes
- GET /api/graph/search - Search nodes
- GET /api/graph/related/{type}/{id} - Get related entities
- GET /api/graph/stats - Graph statistics
- POST /api/graph/rebuild - Trigger graph rebuild
- GET /api/entities/search - Entity discovery search
"""

import logging
from typing import Optional, List
from fastapi import APIRouter, Query, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase

from ..models import GraphNetworkResponse, GraphStatsResponse, NODE_TYPES
from ..query_service import GraphQueryService
from ..builder import GraphBuilder
from ..alias_resolver import EntityAliasResolver, bootstrap_common_aliases
from ..discovery_service import EntityDiscoveryService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/graph", tags=["Knowledge Graph"])

# Global instances (initialized on startup)
_query_service: Optional[GraphQueryService] = None
_builder: Optional[GraphBuilder] = None
_alias_resolver: Optional[EntityAliasResolver] = None
_discovery_service: Optional[EntityDiscoveryService] = None
_db: Optional[AsyncIOMotorDatabase] = None


def init_graph_services(db: AsyncIOMotorDatabase):
    """Initialize graph services with database connection"""
    global _query_service, _builder, _alias_resolver, _discovery_service, _db
    _db = db
    _query_service = GraphQueryService(db)
    _builder = GraphBuilder(db)
    _alias_resolver = EntityAliasResolver(db)
    _discovery_service = EntityDiscoveryService(db)
    logger.info("[GraphAPI] Services initialized")


def get_query_service() -> GraphQueryService:
    if not _query_service:
        raise HTTPException(status_code=503, detail="Graph service not initialized")
    return _query_service


def get_builder() -> GraphBuilder:
    if not _builder:
        raise HTTPException(status_code=503, detail="Graph builder not initialized")
    return _builder


def get_alias_resolver() -> EntityAliasResolver:
    if not _alias_resolver:
        raise HTTPException(status_code=503, detail="Alias resolver not initialized")
    return _alias_resolver


def get_discovery_service() -> EntityDiscoveryService:
    if not _discovery_service:
        raise HTTPException(status_code=503, detail="Discovery service not initialized")
    return _discovery_service


# =============================================================================
# Network endpoints (for visualization)
# =============================================================================

@router.get("/network", response_model=GraphNetworkResponse)
async def get_network(
    center_type: Optional[str] = Query(None, description="Center node entity type"),
    center_id: Optional[str] = Query(None, description="Center node entity id"),
    depth: int = Query(1, ge=1, le=3, description="Traversal depth from center"),
    limit_nodes: int = Query(200, ge=10, le=500, description="Max nodes to return"),
    limit_edges: int = Query(500, ge=10, le=1000, description="Max edges to return"),
    node_types: Optional[str] = Query(None, description="Comma-separated node types to include"),
    relation_types: Optional[str] = Query(None, description="Comma-separated relation types to include")
):
    """
    Get graph network for visualization.
    
    If center_type and center_id provided, returns ego-network around that entity.
    Otherwise returns a sample of the full network.
    
    Response format is compatible with react-force-graph-2d:
    - nodes: [{id, label, type, size, ...}]
    - edges: [{source, target, relation, weight, value, ...}]
    """
    service = get_query_service()
    
    # Parse comma-separated filters
    node_types_list = node_types.split(",") if node_types else None
    relation_types_list = relation_types.split(",") if relation_types else None
    
    return await service.get_network(
        center_type=center_type,
        center_id=center_id,
        depth=depth,
        limit_nodes=limit_nodes,
        limit_edges=limit_edges,
        node_types=node_types_list,
        relation_types=relation_types_list
    )


@router.get("/network/{entity_type}/{entity_id}", response_model=GraphNetworkResponse)
async def get_entity_network(
    entity_type: str,
    entity_id: str,
    depth: int = Query(1, ge=1, le=3),
    limit_nodes: int = Query(100, ge=10, le=300),
    limit_edges: int = Query(300, ge=10, le=500)
):
    """
    Get ego-network around a specific entity.
    Shorthand for /network?center_type=X&center_id=Y
    """
    service = get_query_service()
    return await service.get_network(
        center_type=entity_type,
        center_id=entity_id,
        depth=depth,
        limit_nodes=limit_nodes,
        limit_edges=limit_edges
    )


# =============================================================================
# Node endpoints
# =============================================================================

@router.get("/node/{entity_type}/{entity_id}")
async def get_node(entity_type: str, entity_id: str):
    """Get single node by entity type and id"""
    service = get_query_service()
    node = await service.get_node(entity_type, entity_id)
    
    if not node:
        raise HTTPException(status_code=404, detail=f"Node not found: {entity_type}:{entity_id}")
    
    # Convert to API format
    return {
        "id": f"{node['entity_type']}:{node['entity_id']}",
        "entity_type": node["entity_type"],
        "entity_id": node["entity_id"],
        "label": node["label"],
        "slug": node.get("slug"),
        "status": node.get("status"),
        "metadata": node.get("metadata", {}),
        "edge_count": node.get("edge_count", 0),
        "created_at": node.get("created_at"),
        "updated_at": node.get("updated_at")
    }


@router.get("/edges/{entity_type}/{entity_id}")
async def get_edges(
    entity_type: str,
    entity_id: str,
    relation_type: Optional[str] = Query(None),
    direction: str = Query("both", regex="^(both|outgoing|incoming)$"),
    limit: int = Query(100, ge=1, le=500)
):
    """Get edges for a node"""
    service = get_query_service()
    edges = await service.get_edges(
        entity_type=entity_type,
        entity_id=entity_id,
        relation_type=relation_type,
        direction=direction,
        limit=limit
    )
    
    # Fetch node labels for edges
    result = []
    for edge in edges:
        from_node = await service.get_node_by_id(edge["from_node_id"])
        to_node = await service.get_node_by_id(edge["to_node_id"])
        
        result.append({
            "id": edge["id"],
            "source": f"{from_node['entity_type']}:{from_node['entity_id']}" if from_node else edge["from_node_id"],
            "source_label": from_node["label"] if from_node else None,
            "target": f"{to_node['entity_type']}:{to_node['entity_id']}" if to_node else edge["to_node_id"],
            "target_label": to_node["label"] if to_node else None,
            "relation": edge["relation_type"],
            "weight": edge.get("weight", 1.0),
            "source_type": edge.get("source_type", "direct"),
            "metadata": edge.get("metadata", {})
        })
    
    return {"edges": result, "total": len(result)}


@router.get("/neighbors/{entity_type}/{entity_id}")
async def get_neighbors(
    entity_type: str,
    entity_id: str,
    neighbor_type: Optional[str] = Query(None),
    relation_type: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200)
):
    """Get neighbor nodes for a node"""
    service = get_query_service()
    neighbors = await service.get_neighbors(
        entity_type=entity_type,
        entity_id=entity_id,
        neighbor_type=neighbor_type,
        relation_type=relation_type,
        limit=limit
    )
    
    result = []
    for node in neighbors:
        result.append({
            "id": f"{node['entity_type']}:{node['entity_id']}",
            "label": node["label"],
            "type": node["entity_type"],
            "entity_id": node["entity_id"],
            "slug": node.get("slug"),
            "metadata": node.get("metadata", {})
        })
    
    return {"neighbors": result, "total": len(result)}


# =============================================================================
# Search & Discovery
# =============================================================================

@router.get("/search")
async def search_nodes(
    q: str = Query(..., min_length=1, description="Search query"),
    entity_type: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100)
):
    """Search nodes by label/slug"""
    service = get_query_service()
    results = await service.search(query=q, entity_type=entity_type, limit=limit)
    return {"results": results, "total": len(results), "query": q}


@router.get("/related/{entity_type}/{entity_id}")
async def get_related(
    entity_type: str,
    entity_id: str,
    limit: int = Query(10, ge=1, le=50)
):
    """Get related entities based on shared connections"""
    service = get_query_service()
    related = await service.get_related(
        entity_type=entity_type,
        entity_id=entity_id,
        limit=limit
    )
    return {"related": related, "total": len(related)}


# =============================================================================
# Stats & Admin
# =============================================================================

@router.get("/stats", response_model=GraphStatsResponse)
async def get_stats():
    """Get graph statistics"""
    service = get_query_service()
    return await service.get_stats()


@router.get("/node-types")
async def get_node_types():
    """Get available node types"""
    return {"node_types": NODE_TYPES}


@router.post("/rebuild")
async def rebuild_graph():
    """
    Trigger full graph rebuild.
    Warning: This may take time for large datasets.
    """
    builder = get_builder()
    
    try:
        snapshot = await builder.full_rebuild()
        return {
            "status": "success",
            "snapshot_id": snapshot.id,
            "node_count": snapshot.node_count,
            "edge_count": snapshot.edge_count,
            "created_at": snapshot.created_at.isoformat()
        }
    except Exception as e:
        logger.error(f"[GraphAPI] Rebuild failed: {e}")
        raise HTTPException(status_code=500, detail=f"Rebuild failed: {str(e)}")


# =============================================================================
# Entity Discovery & Resolution
# =============================================================================

@router.get("/entities/search")
async def search_entities(
    q: str = Query(..., min_length=1, description="Search query"),
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    limit: int = Query(10, ge=1, le=50)
):
    """
    Search entities with alias resolution and discovery.
    
    Flow:
    1. Search aliases first (handles a16z -> Andreessen Horowitz)
    2. Search local database
    3. Return combined suggestions
    """
    discovery = get_discovery_service()
    results = await discovery.search_suggestions(q, limit=limit)
    
    # Filter by type if specified
    if entity_type:
        results = [r for r in results if r["type"] == entity_type]
    
    return {
        "results": results,
        "total": len(results),
        "query": q
    }


@router.get("/entities/resolve/{query}")
async def resolve_entity(
    query: str,
    entity_type: Optional[str] = Query(None)
):
    """
    Resolve an entity query to canonical entity.
    Uses alias resolution + discovery.
    
    Returns canonical entity or 404 if not found.
    """
    alias_resolver = get_alias_resolver()
    
    # Try alias resolution first
    resolved = await alias_resolver.resolve(query, entity_type)
    if resolved:
        etype, eid = resolved
        return {
            "resolved": True,
            "entity_type": etype,
            "entity_id": eid,
            "canonical_id": f"{etype}:{eid}",
            "source": "alias"
        }
    
    # Try discovery
    discovery = get_discovery_service()
    entity = await discovery.discover_entity(query, entity_type)
    
    if entity:
        etype = entity.get("_entity_type") or entity.get("entity_type")
        eid = entity.get("_entity_id") or entity.get("entity_id")
        return {
            "resolved": True,
            "entity_type": etype,
            "entity_id": eid,
            "canonical_id": f"{etype}:{eid}",
            "source": "discovery",
            "entity": entity
        }
    
    raise HTTPException(status_code=404, detail=f"Entity not found: {query}")


@router.post("/aliases/add")
async def add_alias(
    entity_type: str = Query(...),
    entity_id: str = Query(...),
    alias: str = Query(...),
    source: str = Query("manual")
):
    """Add a new alias for an entity"""
    resolver = get_alias_resolver()
    success = await resolver.add_alias(
        entity_type=entity_type,
        entity_id=entity_id,
        alias=alias,
        source=source
    )
    
    if success:
        return {"status": "success", "alias": alias, "entity": f"{entity_type}:{entity_id}"}
    raise HTTPException(status_code=400, detail="Failed to add alias")


@router.get("/aliases/{entity_type}/{entity_id}")
async def get_aliases(entity_type: str, entity_id: str):
    """Get all aliases for an entity"""
    resolver = get_alias_resolver()
    aliases = await resolver.get_all_aliases(entity_type, entity_id)
    return {
        "entity_type": entity_type,
        "entity_id": entity_id,
        "aliases": aliases,
        "count": len(aliases)
    }


@router.post("/aliases/bootstrap")
async def bootstrap_aliases():
    """Bootstrap common entity aliases"""
    try:
        count = await bootstrap_common_aliases(_db)
        return {"status": "success", "aliases_added": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

