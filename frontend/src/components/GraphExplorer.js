import React, { useState, useEffect, useCallback } from 'react';
import { Search, Users, Building2, Briefcase, Network, ChevronLeft, ChevronRight } from 'lucide-react';
import ForceGraphViewer from './ForceGraphViewer';

const API_URL = process.env.REACT_APP_BACKEND_URL || '';

// Entity type colors
const TYPE_COLORS = {
  project: '#8b5cf6',
  fund: '#f59e0b',
  person: '#ec4899',
  exchange: '#22c55e',
  token: '#3b82f6',
  asset: '#06b6d4',
  default: '#64748b'
};

const TYPE_ICONS = {
  project: Briefcase,
  fund: Building2,
  person: Users,
  exchange: Network,
  default: Network
};

const GraphExplorer = ({ colors }) => {
  const [selectedEntity, setSelectedEntity] = useState(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [suggestion, setSuggestion] = useState(null); // Single autocomplete suggestion
  const [isSearching, setIsSearching] = useState(false);
  const [isResolving, setIsResolving] = useState(false);
  
  // Relations data
  const [relations, setRelations] = useState([]);
  const [insights, setInsights] = useState(null);
  const [loadingRelations, setLoadingRelations] = useState(false);
  
  // Pagination
  const [currentPage, setCurrentPage] = useState(1);
  const itemsPerPage = 10;

  // Get autocomplete suggestion (single best match)
  const getSuggestion = useCallback(async (query) => {
    if (!query || query.length < 2) {
      setSuggestion(null);
      return;
    }
    
    setIsSearching(true);
    try {
      const response = await fetch(`${API_URL}/api/graph/entities/search?q=${encodeURIComponent(query)}&limit=1`);
      if (response.ok) {
        const data = await response.json();
        if (data.results && data.results.length > 0) {
          setSuggestion(data.results[0]);
        } else {
          setSuggestion(null);
        }
      }
    } catch (err) {
      console.error('Search failed:', err);
      setSuggestion(null);
    } finally {
      setIsSearching(false);
    }
  }, []);

  // Debounced suggestion
  useEffect(() => {
    const timer = setTimeout(() => {
      getSuggestion(searchQuery);
    }, 200);
    return () => clearTimeout(timer);
  }, [searchQuery, getSuggestion]);

  // Execute search - resolve and load entity
  const executeSearch = async () => {
    const query = searchQuery.trim();
    if (!query) return;
    
    setIsResolving(true);
    try {
      // Use resolve endpoint - system determines type automatically
      const response = await fetch(`${API_URL}/api/graph/entities/resolve/${encodeURIComponent(query)}`);
      
      if (response.ok) {
        const data = await response.json();
        if (data.resolved) {
          // Got canonical entity - load it
          setSelectedEntity({
            id: data.canonical_id,
            label: query,
            type: data.entity_type,
            entity_id: data.entity_id
          });
          setSuggestion(null);
        }
      } else {
        // Not found - try suggestion if available
        if (suggestion) {
          setSelectedEntity({
            id: suggestion.id,
            label: suggestion.label,
            type: suggestion.type,
            entity_id: suggestion.entity_id
          });
          setSuggestion(null);
        } else {
          alert(`Entity not found: ${query}`);
        }
      }
    } catch (err) {
      console.error('Resolve failed:', err);
      // Fallback to suggestion
      if (suggestion) {
        setSelectedEntity({
          id: suggestion.id,
          label: suggestion.label,
          type: suggestion.type,
          entity_id: suggestion.entity_id
        });
        setSuggestion(null);
      }
    } finally {
      setIsResolving(false);
    }
  };

  // Handle Enter key
  const handleKeyDown = (e) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      // If suggestion exists and matches, use it directly
      if (suggestion && suggestion.label.toLowerCase().startsWith(searchQuery.toLowerCase())) {
        setSelectedEntity({
          id: suggestion.id,
          label: suggestion.label,
          type: suggestion.type,
          entity_id: suggestion.entity_id
        });
        setSearchQuery(suggestion.label);
        setSuggestion(null);
      } else {
        executeSearch();
      }
    }
  };

  // Accept suggestion (Tab or click)
  const acceptSuggestion = () => {
    if (suggestion) {
      setSearchQuery(suggestion.label);
      setSelectedEntity({
        id: suggestion.id,
        label: suggestion.label,
        type: suggestion.type,
        entity_id: suggestion.entity_id
      });
      setSuggestion(null);
    }
  };

  // Load relations when entity selected
  useEffect(() => {
    if (!selectedEntity) {
      setRelations([]);
      setInsights(null);
      return;
    }

    const loadRelations = async () => {
      setLoadingRelations(true);
      try {
        const [type, id] = selectedEntity.id.split(':');
        
        const edgesRes = await fetch(`${API_URL}/api/graph/edges/${type}/${id}?limit=100`);
        const edgesData = await edgesRes.ok ? await edgesRes.json() : { edges: [] };
        
        const neighborsRes = await fetch(`${API_URL}/api/graph/neighbors/${type}/${id}?limit=100`);
        const neighborsData = await neighborsRes.ok ? await neighborsRes.json() : { neighbors: [] };
        
        const relationsList = edgesData.edges.map(edge => {
          const isOutgoing = edge.source === selectedEntity.id;
          const targetEntity = isOutgoing ? edge.target : edge.source;
          const targetLabel = isOutgoing ? edge.target_label : edge.source_label;
          const [targetType] = targetEntity.split(':');
          
          return {
            id: edge.id,
            type: targetType,
            entity: targetLabel || targetEntity.split(':')[1],
            entityId: targetEntity,
            relation: edge.relation.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()),
            status: edge.source_type === 'direct' ? 'Active' : 'Historical',
            weight: edge.weight || 1
          };
        });
        
        setRelations(relationsList);
        
        const typeCount = {};
        neighborsData.neighbors.forEach(n => {
          typeCount[n.type] = (typeCount[n.type] || 0) + 1;
        });
        
        setInsights({
          totalRelations: relationsList.length,
          activeCount: relationsList.filter(r => r.status === 'Active').length,
          networkReach: 2,
          canReach: Math.min(400, relationsList.length * 20),
          persons: typeCount.person || 0,
          funds: typeCount.fund || 0,
          projects: typeCount.project || 0,
          exchanges: typeCount.exchange || 0
        });
        
      } catch (err) {
        console.error('Failed to load relations:', err);
      } finally {
        setLoadingRelations(false);
      }
    };

    loadRelations();
    setCurrentPage(1);
  }, [selectedEntity]);

  // Clear selection
  const clearSelection = () => {
    setSelectedEntity(null);
    setSearchQuery('');
    setRelations([]);
    setInsights(null);
    setSuggestion(null);
  };

  // Navigate to entity from table
  const navigateToEntity = (rel) => {
    const [type, id] = rel.entityId.split(':');
    setSelectedEntity({ 
      id: rel.entityId, 
      label: rel.entity, 
      type 
    });
    setSearchQuery(rel.entity);
  };

  // Paginated relations
  const paginatedRelations = relations.slice(
    (currentPage - 1) * itemsPerPage,
    currentPage * itemsPerPage
  );
  const totalPages = Math.ceil(relations.length / itemsPerPage);

  // Empty state
  const EmptyState = () => (
    <div 
      className="flex flex-col items-center justify-center h-full"
      style={{ backgroundColor: '#0a0e1a', minHeight: '400px' }}
    >
      <svg width="200" height="200" viewBox="0 0 200 200" fill="none" xmlns="http://www.w3.org/2000/svg">
        {/* Stars background */}
        <circle cx="20" cy="30" r="1.5" fill="#6366f1" opacity="0.8"/>
        <circle cx="180" cy="25" r="1" fill="#8b5cf6" opacity="0.6"/>
        <circle cx="45" cy="150" r="1.2" fill="#6366f1" opacity="0.7"/>
        <circle cx="170" cy="120" r="1" fill="#a78bfa" opacity="0.5"/>
        <circle cx="30" cy="90" r="0.8" fill="#818cf8" opacity="0.6"/>
        <circle cx="165" cy="170" r="1.3" fill="#6366f1" opacity="0.7"/>
        <circle cx="55" cy="45" r="0.8" fill="#c4b5fd" opacity="0.5"/>
        <circle cx="145" cy="55" r="1" fill="#818cf8" opacity="0.6"/>
        
        {/* Large planet with ring */}
        <circle cx="150" cy="140" r="28" fill="url(#planetGradient)"/>
        <ellipse cx="150" cy="140" rx="45" ry="10" stroke="url(#ringGradient)" strokeWidth="3" fill="none" opacity="0.7"/>
        <ellipse cx="150" cy="140" rx="38" ry="7" stroke="#6366f1" strokeWidth="1.5" fill="none" opacity="0.4"/>
        
        {/* Small moon */}
        <circle cx="115" cy="165" r="8" fill="#374151"/>
        <circle cx="113" cy="163" r="2" fill="#475569" opacity="0.5"/>
        
        {/* Rocket */}
        <g transform="translate(40, 35) rotate(-25)">
          {/* Rocket body */}
          <ellipse cx="50" cy="60" rx="16" ry="45" fill="url(#rocketBody)"/>
          
          {/* Rocket nose */}
          <path d="M50 15 L38 45 L62 45 Z" fill="#e0e7ff"/>
          <path d="M50 15 L44 35 L56 35 Z" fill="#c7d2fe"/>
          
          {/* Window */}
          <circle cx="50" cy="55" r="8" fill="#0f172a"/>
          <circle cx="50" cy="55" r="6" fill="#1e293b"/>
          <circle cx="48" cy="53" r="2" fill="#6366f1" opacity="0.6"/>
          
          {/* Fins */}
          <path d="M34 85 L20 110 L34 100 Z" fill="#6366f1"/>
          <path d="M66 85 L80 110 L66 100 Z" fill="#6366f1"/>
          <path d="M34 85 L25 105 L34 97 Z" fill="#818cf8"/>
          <path d="M66 85 L75 105 L66 97 Z" fill="#818cf8"/>
          
          {/* Engine flames */}
          <ellipse cx="50" cy="108" rx="10" ry="6" fill="#f59e0b"/>
          <ellipse cx="50" cy="115" rx="7" ry="12" fill="#fbbf24"/>
          <ellipse cx="50" cy="120" rx="5" ry="10" fill="#fde68a"/>
          <ellipse cx="50" cy="125" rx="3" ry="8" fill="#fef3c7" opacity="0.8"/>
        </g>
        
        {/* Small distant planet */}
        <circle cx="35" cy="130" r="12" fill="#4f46e5"/>
        <circle cx="32" cy="127" r="3" fill="#6366f1" opacity="0.5"/>
        
        {/* Orbit lines */}
        <ellipse cx="100" cy="120" rx="80" ry="25" stroke="#6366f1" strokeWidth="0.5" fill="none" opacity="0.2" strokeDasharray="4 4"/>
        <ellipse cx="100" cy="100" rx="60" ry="18" stroke="#8b5cf6" strokeWidth="0.5" fill="none" opacity="0.15" strokeDasharray="3 3"/>
        
        {/* Gradients */}
        <defs>
          <linearGradient id="planetGradient" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" stopColor="#6366f1"/>
            <stop offset="50%" stopColor="#4f46e5"/>
            <stop offset="100%" stopColor="#3730a3"/>
          </linearGradient>
          <linearGradient id="ringGradient" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="#818cf8" stopOpacity="0.3"/>
            <stop offset="50%" stopColor="#a78bfa"/>
            <stop offset="100%" stopColor="#818cf8" stopOpacity="0.3"/>
          </linearGradient>
          <linearGradient id="rocketBody" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="#94a3b8"/>
            <stop offset="30%" stopColor="#e2e8f0"/>
            <stop offset="70%" stopColor="#e2e8f0"/>
            <stop offset="100%" stopColor="#94a3b8"/>
          </linearGradient>
        </defs>
      </svg>
      
      <h3 className="text-xl font-medium text-slate-300 mt-8 mb-2">
        Start exploring the network
      </h3>
      <p className="text-slate-500 text-sm">
        Enter an entity name and press Search
      </p>
    </div>
  );

  return (
    <div data-testid="graph-explorer" className="h-full flex flex-col">
      {/* Search Bar with Autocomplete */}
      <div className="mb-4 relative">
        <div className="flex gap-2">
          {/* Input with inline suggestion */}
          <div className="relative flex-1">
            <Search className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-slate-400" />
            
            {/* Suggestion overlay (ghost text) */}
            {suggestion && searchQuery && suggestion.label.toLowerCase().startsWith(searchQuery.toLowerCase()) && (
              <div 
                className="absolute left-12 top-1/2 -translate-y-1/2 pointer-events-none text-slate-500"
                style={{ color: colors.textSecondary }}
              >
                <span className="invisible">{searchQuery}</span>
                <span className="text-slate-500">{suggestion.label.slice(searchQuery.length)}</span>
              </div>
            )}
            
            <input
              data-testid="graph-search-input"
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Enter entity name (e.g., Solana, a16z, Vitalik)..."
              className="w-full pl-12 pr-4 py-3 rounded-xl border transition-all"
              style={{
                backgroundColor: colors.background,
                borderColor: colors.border,
                color: colors.text
              }}
            />
            
            {/* Clear button */}
            {searchQuery && (
              <button
                onClick={() => {
                  setSearchQuery('');
                  setSuggestion(null);
                }}
                className="absolute right-4 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-200 text-xl"
              >
                ×
              </button>
            )}
          </div>
          
          {/* Search Button */}
          <button
            data-testid="graph-search-btn"
            onClick={() => {
              if (suggestion) {
                acceptSuggestion();
              } else {
                executeSearch();
              }
            }}
            disabled={!searchQuery.trim() || isResolving}
            className="px-6 py-3 rounded-xl font-medium transition-all flex items-center gap-2 disabled:opacity-50"
            style={{
              backgroundColor: '#10b981',
              color: 'white'
            }}
          >
            {isResolving ? (
              <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
            ) : (
              <Search className="w-4 h-4" />
            )}
            Search
          </button>
        </div>
        
        {/* Single suggestion hint below input */}
        {suggestion && searchQuery && !selectedEntity && (
          <div 
            className="mt-2 px-4 py-2 rounded-lg text-sm flex items-center gap-2 cursor-pointer hover:bg-slate-800"
            style={{ backgroundColor: colors.surface, color: colors.textSecondary }}
            onClick={acceptSuggestion}
          >
            <span 
              className="w-2 h-2 rounded-full"
              style={{ backgroundColor: TYPE_COLORS[suggestion.type] || TYPE_COLORS.default }}
            />
            <span>Press Enter or click to search for </span>
            <span className="font-medium" style={{ color: colors.text }}>{suggestion.label}</span>
          </div>
        )}
        
        {/* Loading indicator */}
        {isSearching && (
          <div className="mt-2 px-4 py-2 text-sm text-slate-500 flex items-center gap-2">
            <div className="w-3 h-3 border-2 border-violet-500 border-t-transparent rounded-full animate-spin" />
            Searching...
          </div>
        )}
      </div>

      {/* Selected Entity Badge */}
      {selectedEntity && (
        <div className="mb-4 flex items-center gap-2">
          <span className="text-sm text-slate-400">Showing graph for:</span>
          <span 
            className="px-3 py-1 rounded-full text-sm font-medium flex items-center gap-2"
            style={{ 
              backgroundColor: TYPE_COLORS[selectedEntity.type] || TYPE_COLORS.default,
              color: 'white'
            }}
          >
            {selectedEntity.label}
            <button onClick={clearSelection} className="hover:opacity-70">×</button>
          </span>
        </div>
      )}

      {/* Main Content */}
      {!selectedEntity ? (
        <div 
          className="rounded-2xl border overflow-hidden flex-1"
          style={{ 
            backgroundColor: '#0a0e1a',
            borderColor: colors.border,
            minHeight: '400px'
          }}
        >
          <EmptyState />
        </div>
      ) : (
        <div className="flex flex-col gap-4 flex-1">
          {/* Graph Section - Full Width */}
          <div 
            className="rounded-2xl border overflow-hidden"
            style={{ 
              backgroundColor: '#0a0e1a', 
              borderColor: colors.border,
              height: '450px'
            }}
          >
            <ForceGraphViewer centerEntity={selectedEntity.id} />
          </div>
          
          {/* Relations Section - Full Width Below Graph */}
          <div 
            className="rounded-2xl border overflow-hidden flex-1"
            style={{ backgroundColor: colors.background, borderColor: colors.border }}
          >
            {/* Header with Entity and Insights */}
            <div className="p-4 border-b flex items-start justify-between" style={{ borderColor: colors.border }}>
              <div>
                <h3 className="text-lg font-semibold flex items-center gap-2" style={{ color: colors.text }}>
                  Relations
                </h3>
              </div>
              
              {/* Relation Insights - Inline */}
              {insights && (
                <div className="flex gap-6">
                  <div className="text-right">
                    <p className="text-xs text-slate-400">Total relations</p>
                    <p className="text-xl font-bold text-emerald-400">{insights.totalRelations}</p>
                  </div>
                  <div className="text-right">
                    <p className="text-xs text-slate-400">Network reach</p>
                    <p className="text-xl font-bold text-emerald-400">{insights.networkReach} hops</p>
                  </div>
                  <div className="flex gap-4 items-center border-l pl-4" style={{ borderColor: colors.border }}>
                    <div className="text-center">
                      <p className="text-lg font-semibold" style={{ color: colors.text }}>{insights.persons}</p>
                      <p className="text-xs text-slate-400">Persons</p>
                    </div>
                    <div className="text-center">
                      <p className="text-lg font-semibold" style={{ color: colors.text }}>{insights.funds}</p>
                      <p className="text-xs text-slate-400">Funds</p>
                    </div>
                    <div className="text-center">
                      <p className="text-lg font-semibold" style={{ color: colors.text }}>{insights.projects}</p>
                      <p className="text-xs text-slate-400">Projects</p>
                    </div>
                  </div>
                </div>
              )}
            </div>
            
            {/* Relations Table */}
            <div className="overflow-auto" style={{ maxHeight: '300px' }}>
              {loadingRelations ? (
                <div className="flex items-center justify-center py-12">
                  <div className="w-8 h-8 border-2 border-violet-500 border-t-transparent rounded-full animate-spin" />
                </div>
              ) : relations.length === 0 ? (
                <div className="flex items-center justify-center py-12 text-slate-500">
                  No relations found
                </div>
              ) : (
                <table className="w-full">
                  <thead className="sticky top-0" style={{ backgroundColor: colors.background }}>
                    <tr className="text-xs text-slate-400 border-b" style={{ borderColor: colors.border }}>
                      <th className="text-left p-4 w-24">Type</th>
                      <th className="text-left p-4">Entity</th>
                      <th className="text-left p-4">Relation</th>
                      <th className="text-left p-4 w-28">Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {paginatedRelations.map((rel, idx) => {
                      const Icon = TYPE_ICONS[rel.type] || TYPE_ICONS.default;
                      return (
                        <tr 
                          key={rel.id || idx}
                          className="border-b hover:bg-slate-800/50 cursor-pointer transition-colors"
                          style={{ borderColor: colors.border }}
                          onClick={() => navigateToEntity(rel)}
                        >
                          <td className="p-4">
                            <div className="flex items-center gap-2">
                              <span 
                                className="w-2 h-2 rounded-full"
                                style={{ backgroundColor: TYPE_COLORS[rel.type] || TYPE_COLORS.default }}
                              />
                              <span className="text-xs text-slate-400 capitalize">{rel.type}</span>
                            </div>
                          </td>
                          <td className="p-4">
                            <div className="flex items-center gap-2">
                              <div className="w-7 h-7 rounded-full bg-slate-700 flex items-center justify-center">
                                <Icon className="w-3.5 h-3.5 text-slate-400" />
                              </div>
                              <span className="text-sm font-medium" style={{ color: colors.text }}>
                                {rel.entity}
                              </span>
                            </div>
                          </td>
                          <td className="p-4 text-sm text-slate-400">{rel.relation}</td>
                          <td className="p-4">
                            <span className={`text-xs px-2 py-1 rounded ${
                              rel.status === 'Active' ? 'text-emerald-400 bg-emerald-400/10' : 'text-slate-400 bg-slate-400/10'
                            }`}>
                              {rel.status}
                            </span>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              )}
            </div>
            
            {/* Pagination */}
            {totalPages > 1 && (
              <div className="p-3 border-t flex items-center justify-between" style={{ borderColor: colors.border }}>
                <div className="flex items-center gap-1">
                  <button
                    onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
                    disabled={currentPage === 1}
                    className="p-1.5 rounded hover:bg-slate-700 disabled:opacity-50"
                  >
                    <ChevronLeft className="w-4 h-4 text-slate-400" />
                  </button>
                  {[...Array(Math.min(5, totalPages))].map((_, i) => {
                    const page = i + 1;
                    return (
                      <button
                        key={page}
                        onClick={() => setCurrentPage(page)}
                        className={`w-8 h-8 rounded text-sm ${
                          currentPage === page 
                            ? 'bg-emerald-500 text-white' 
                            : 'text-slate-400 hover:bg-slate-700'
                        }`}
                      >
                        {page}
                      </button>
                    );
                  })}
                  {totalPages > 5 && <span className="text-slate-500 px-2">...</span>}
                  <button
                    onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
                    disabled={currentPage === totalPages}
                    className="p-1.5 rounded hover:bg-slate-700 disabled:opacity-50"
                  >
                    <ChevronRight className="w-4 h-4 text-slate-400" />
                  </button>
                </div>
                <span className="text-xs text-slate-500">
                  Showing {(currentPage - 1) * itemsPerPage + 1} - {Math.min(currentPage * itemsPerPage, relations.length)} of {relations.length}
                </span>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default GraphExplorer;
