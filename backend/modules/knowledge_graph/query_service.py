"""
Graph Query Service - Query and traverse the knowledge graph

Responsibilities:
- Get node neighbors
- Get edges by node
- Path finding
- Graph statistics
- Network subgraph extraction
"""

import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Tuple
from motor.motor_asyncio import AsyncIOMotorDatabase

from .models import (
    GraphNetworkResponse, 
    GraphStatsResponse,
    NODE_TYPES
)

logger = logging.getLogger(__name__)


class GraphQueryService:
    """
    Query service for the knowledge graph.
    Provides API-ready responses for graph visualization.
    """
    
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.nodes_collection = db.graph_nodes
        self.edges_collection = db.graph_edges
    
    async def get_node(self, entity_type: str, entity_id: str) -> Optional[Dict[str, Any]]:
        """Get node by entity type and id"""
        node = await self.nodes_collection.find_one({
            "entity_type": entity_type,
            "entity_id": entity_id
        })
        
        if node:
            # Add neighbor count
            edge_count = await self.edges_collection.count_documents({
                "$or": [
                    {"from_node_id": node["id"]},
                    {"to_node_id": node["id"]}
                ]
            })
            node["edge_count"] = edge_count
        
        return node
    
    async def get_node_by_id(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Get node by internal id"""
        return await self.nodes_collection.find_one({"id": node_id})
    
    async def get_edges(
        self,
        entity_type: str,
        entity_id: str,
        relation_type: Optional[str] = None,
        direction: str = "both",  # both, outgoing, incoming
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get edges for a node"""
        node = await self.get_node(entity_type, entity_id)
        if not node:
            return []
        
        node_id = node["id"]
        
        filter_dict = {}
        if direction == "outgoing":
            filter_dict["from_node_id"] = node_id
        elif direction == "incoming":
            filter_dict["to_node_id"] = node_id
        else:
            filter_dict["$or"] = [
                {"from_node_id": node_id},
                {"to_node_id": node_id}
            ]
        
        if relation_type:
            filter_dict["relation_type"] = relation_type
        
        cursor = self.edges_collection.find(filter_dict).limit(limit)
        return await cursor.to_list(length=limit)
    
    async def get_neighbors(
        self,
        entity_type: str,
        entity_id: str,
        neighbor_type: Optional[str] = None,
        relation_type: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Get neighbor nodes for a node"""
        edges = await self.get_edges(entity_type, entity_id, relation_type, limit=limit * 2)
        
        node = await self.get_node(entity_type, entity_id)
        if not node:
            return []
        
        node_id = node["id"]
        neighbor_ids = set()
        
        for edge in edges:
            if edge["from_node_id"] == node_id:
                neighbor_ids.add(edge["to_node_id"])
            else:
                neighbor_ids.add(edge["from_node_id"])
        
        # Fetch neighbor nodes
        filter_dict = {"id": {"$in": list(neighbor_ids)}}
        if neighbor_type:
            filter_dict["entity_type"] = neighbor_type
        
        cursor = self.nodes_collection.find(filter_dict).limit(limit)
        return await cursor.to_list(length=limit)
    
    async def get_network(
        self,
        center_type: Optional[str] = None,
        center_id: Optional[str] = None,
        depth: int = 1,
        limit_nodes: int = 200,
        limit_edges: int = 500,
        node_types: Optional[List[str]] = None,
        relation_types: Optional[List[str]] = None
    ) -> GraphNetworkResponse:
        """
        Get graph network for visualization.
        
        If center_type/center_id provided, returns ego-network around that node.
        Otherwise returns full network sample.
        
        Returns format compatible with react-force-graph-2d:
        {
            "nodes": [{"id": "type:id", "label": "...", "type": "...", ...}],
            "edges": [{"source": "type:id", "target": "type:id", "relation": "...", ...}]
        }
        """
        nodes_dict = {}  # node_id -> node data
        edges_list = []
        
        if center_type and center_id:
            # Ego-network mode: start from center node
            center_node = await self.get_node(center_type, center_id)
            if not center_node:
                return GraphNetworkResponse(nodes=[], edges=[], stats={"error": "Center node not found"})
            
            # Add center node
            node_key = f"{center_node['entity_type']}:{center_node['entity_id']}"
            nodes_dict[center_node["id"]] = {
                "id": node_key,
                "label": center_node["label"],
                "type": center_node["entity_type"],
                "entity_id": center_node["entity_id"],
                "size": 20,  # Larger for center
                "metadata": center_node.get("metadata", {})
            }
            
            # BFS to collect neighbors up to depth
            visited = {center_node["id"]}
            current_level = [center_node["id"]]
            
            for d in range(depth):
                next_level = []
                
                for current_node_id in current_level:
                    # Get edges from this node
                    edge_filter = {
                        "$or": [
                            {"from_node_id": current_node_id},
                            {"to_node_id": current_node_id}
                        ]
                    }
                    if relation_types:
                        edge_filter["relation_type"] = {"$in": relation_types}
                    
                    cursor = self.edges_collection.find(edge_filter).limit(limit_edges // max(1, len(current_level)))
                    
                    async for edge in cursor:
                        # Get neighbor node id
                        neighbor_id = edge["to_node_id"] if edge["from_node_id"] == current_node_id else edge["from_node_id"]
                        
                        # Fetch neighbor if not visited
                        if neighbor_id not in visited and len(nodes_dict) < limit_nodes:
                            neighbor = await self.nodes_collection.find_one({"id": neighbor_id})
                            if neighbor:
                                # Filter by node type if specified
                                if node_types and neighbor["entity_type"] not in node_types:
                                    continue
                                
                                visited.add(neighbor_id)
                                next_level.append(neighbor_id)
                                
                                neighbor_key = f"{neighbor['entity_type']}:{neighbor['entity_id']}"
                                nodes_dict[neighbor_id] = {
                                    "id": neighbor_key,
                                    "label": neighbor["label"],
                                    "type": neighbor["entity_type"],
                                    "entity_id": neighbor["entity_id"],
                                    "size": 10,
                                    "metadata": neighbor.get("metadata", {})
                                }
                        
                        # Add edge if both nodes are in our set
                        if edge["from_node_id"] in visited and edge["to_node_id"] in visited:
                            from_node = nodes_dict.get(edge["from_node_id"])
                            to_node = nodes_dict.get(edge["to_node_id"])
                            if from_node and to_node:
                                edges_list.append({
                                    "source": from_node["id"],
                                    "target": to_node["id"],
                                    "relation": edge["relation_type"],
                                    "weight": edge.get("weight", 1.0),
                                    "value": edge.get("weight", 1.0) * 100 - 50,  # Convert to -50..50 range for color
                                    "metadata": edge.get("metadata", {})
                                })
                
                current_level = next_level
                if not current_level:
                    break
        
        else:
            # Full network sample mode
            node_filter = {}
            if node_types:
                node_filter["entity_type"] = {"$in": node_types}
            
            # Get sample of nodes
            cursor = self.nodes_collection.find(node_filter).limit(limit_nodes)
            async for node in cursor:
                node_key = f"{node['entity_type']}:{node['entity_id']}"
                nodes_dict[node["id"]] = {
                    "id": node_key,
                    "label": node["label"],
                    "type": node["entity_type"],
                    "entity_id": node["entity_id"],
                    "size": 15 if node["entity_type"] in ["exchange", "fund"] else 10,
                    "metadata": node.get("metadata", {})
                }
            
            # Get edges between these nodes
            node_ids = list(nodes_dict.keys())
            edge_filter = {
                "from_node_id": {"$in": node_ids},
                "to_node_id": {"$in": node_ids}
            }
            if relation_types:
                edge_filter["relation_type"] = {"$in": relation_types}
            
            cursor = self.edges_collection.find(edge_filter).limit(limit_edges)
            async for edge in cursor:
                from_node = nodes_dict.get(edge["from_node_id"])
                to_node = nodes_dict.get(edge["to_node_id"])
                if from_node and to_node:
                    edges_list.append({
                        "source": from_node["id"],
                        "target": to_node["id"],
                        "relation": edge["relation_type"],
                        "weight": edge.get("weight", 1.0),
                        "value": edge.get("weight", 1.0) * 100 - 50,
                        "metadata": edge.get("metadata", {})
                    })
        
        # Keep ALL edges - multiple edges between same nodes represent multiple investments/relations
        # We DON'T deduplicate to show real investment count
        # Previously we deduped by source|target|relation which lost multi-round investments
        
        return GraphNetworkResponse(
            nodes=list(nodes_dict.values()),
            edges=edges_list,  # Return ALL edges (not deduplicated)
            stats={
                "node_count": len(nodes_dict),
                "edge_count": len(edges_list),
                "center": f"{center_type}:{center_id}" if center_type else None,
                "depth": depth
            }
        )
    
    async def get_stats(self) -> GraphStatsResponse:
        """Get graph statistics"""
        # Count nodes by type
        nodes_pipeline = [
            {"$group": {"_id": "$entity_type", "count": {"$sum": 1}}}
        ]
        nodes_by_type = {}
        async for doc in self.nodes_collection.aggregate(nodes_pipeline):
            nodes_by_type[doc["_id"]] = doc["count"]
        
        # Count edges by type
        edges_pipeline = [
            {"$group": {"_id": "$relation_type", "count": {"$sum": 1}}}
        ]
        edges_by_type = {}
        async for doc in self.edges_collection.aggregate(edges_pipeline):
            edges_by_type[doc["_id"]] = doc["count"]
        
        # Get last rebuild
        last_snapshot = await self.db.graph_snapshots.find_one(
            sort=[("created_at", -1)]
        )
        
        return GraphStatsResponse(
            total_nodes=sum(nodes_by_type.values()),
            total_edges=sum(edges_by_type.values()),
            nodes_by_type=nodes_by_type,
            edges_by_type=edges_by_type,
            last_rebuild=last_snapshot["created_at"] if last_snapshot else None
        )
    
    async def search(
        self,
        query: str,
        entity_type: Optional[str] = None,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Search nodes by label/slug"""
        filter_dict = {
            "$or": [
                {"label": {"$regex": query, "$options": "i"}},
                {"slug": {"$regex": query, "$options": "i"}},
                {"entity_id": {"$regex": query, "$options": "i"}}
            ]
        }
        if entity_type:
            filter_dict["entity_type"] = entity_type
        
        cursor = self.nodes_collection.find(filter_dict).limit(limit)
        results = []
        async for node in cursor:
            results.append({
                "id": f"{node['entity_type']}:{node['entity_id']}",
                "label": node["label"],
                "type": node["entity_type"],
                "entity_id": node["entity_id"],
                "slug": node.get("slug")
            })
        return results
    
    async def get_related(
        self,
        entity_type: str,
        entity_id: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get related entities based on shared connections"""
        node = await self.get_node(entity_type, entity_id)
        if not node:
            return []
        
        # Find entities that share connections
        # For projects: shared investors, shared founders
        # For funds: shared portfolio companies
        # For persons: shared organizations
        
        neighbors = await self.get_neighbors(entity_type, entity_id, limit=50)
        neighbor_ids = [n["id"] for n in neighbors]
        
        # Find other nodes connected to same neighbors
        related_pipeline = [
            {"$match": {
                "$or": [
                    {"from_node_id": {"$in": neighbor_ids}},
                    {"to_node_id": {"$in": neighbor_ids}}
                ]
            }},
            {"$project": {
                "other_node": {
                    "$cond": [
                        {"$in": ["$from_node_id", neighbor_ids]},
                        "$to_node_id",
                        "$from_node_id"
                    ]
                }
            }},
            {"$match": {"other_node": {"$ne": node["id"]}}},
            {"$group": {"_id": "$other_node", "shared_count": {"$sum": 1}}},
            {"$sort": {"shared_count": -1}},
            {"$limit": limit}
        ]
        
        related = []
        async for doc in self.edges_collection.aggregate(related_pipeline):
            related_node = await self.nodes_collection.find_one({"id": doc["_id"]})
            if related_node and related_node["entity_type"] == entity_type:
                related.append({
                    "id": f"{related_node['entity_type']}:{related_node['entity_id']}",
                    "label": related_node["label"],
                    "type": related_node["entity_type"],
                    "shared_connections": doc["shared_count"]
                })
        
        return related
