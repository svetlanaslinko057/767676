import React, { useRef, useCallback, useState, useEffect } from "react";
import ForceGraph from "react-force-graph-2d";

const API_URL = process.env.REACT_APP_BACKEND_URL || '';

const ForceGraphViewer = ({ centerEntity = null }) => {
  const forceRef = useRef(null);
  const containerRef = useRef(null);
  const [graphData, setGraphData] = useState({
    nodes: [],
    links: [],
  });
  const [lockIcon, setLockIcon] = useState(null);
  const [cryptoIcons, setCryptoIcons] = useState({});
  const [hoveredLink, setHoveredLink] = useState(null);
  const [mousePos, setMousePos] = useState({ x: 0, y: 0 });
  const [currentMainNode, setCurrentMainNode] = useState(null);
  const [hopLevel, setHopLevel] = useState(1);
  const [navigationHistory, setNavigationHistory] = useState([]);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [loading, setLoading] = useState(true);
  const [fsSize, setFsSize] = useState(() => ({
    w: typeof window !== "undefined" ? window.innerWidth : 800,
    h: typeof window !== "undefined" ? window.innerHeight : 600,
  }));

  // Entity from prop or default
  const selectedEntity = centerEntity ? { name: centerEntity } : { name: "Binance" };
  const selectedTab = "on-chain";
  const filters = { persons: true, funds: true, projects: true };
  const onChainFilters = {
    centralizedExchanges: true,
    depositAddresses: true,
    individualsAndFunds: true,
    decentralizedExchanges: true,
    lending: true,
    misc: true,
    uncategorized: true,
    all: true,
  };

  const toggleFullscreen = async () => {
    const el = containerRef.current;
    if (!el || typeof document === "undefined") {
      setIsFullscreen((s) => !s);
      return;
    }

    const isNowFullscreen =
      document.fullscreenElement === el ||
      document.webkitFullscreenElement === el ||
      document.mozFullScreenElement === el ||
      document.msFullscreenElement === el;

    try {
      if (!isNowFullscreen) {
        if (el.requestFullscreen) await el.requestFullscreen();
        else if (el.webkitRequestFullscreen) await el.webkitRequestFullscreen();
        else if (el.mozRequestFullScreen) await el.mozRequestFullScreen();
        else if (el.msRequestFullscreen) await el.msRequestFullscreen();
      } else {
        if (document.exitFullscreen) await document.exitFullscreen();
        else if (document.webkitExitFullscreen) await document.webkitExitFullscreen();
        else if (document.mozCancelFullScreen) await document.mozCancelFullScreen();
        else if (document.msExitFullscreen) await document.msExitFullscreen();
      }
    } catch (err) {
      setIsFullscreen((s) => !s);
    }
  };

  useEffect(() => {
    const onFsChange = () => {
      const el = containerRef.current;
      if (!el || typeof document === "undefined") return;

      const isNowFullscreen =
        document.fullscreenElement === el ||
        document.webkitFullscreenElement === el ||
        document.mozFullScreenElement === el ||
        document.msFullscreenElement === el;

      setIsFullscreen(Boolean(isNowFullscreen));
    };

    document.addEventListener("fullscreenchange", onFsChange);
    document.addEventListener("webkitfullscreenchange", onFsChange);
    document.addEventListener("mozfullscreenchange", onFsChange);
    document.addEventListener("MSFullscreenChange", onFsChange);

    return () => {
      document.removeEventListener("fullscreenchange", onFsChange);
      document.removeEventListener("webkitfullscreenchange", onFsChange);
      document.removeEventListener("mozfullscreenchange", onFsChange);
      document.removeEventListener("MSFullscreenChange", onFsChange);
    };
  }, []);

  useEffect(() => {
    if (!isFullscreen) return;

    const update = () =>
      setFsSize({ w: window.innerWidth, h: window.innerHeight });

    update();

    window.addEventListener("resize", update);
    return () => window.removeEventListener("resize", update);
  }, [isFullscreen]);

  useEffect(() => {
    // Create lock icon image from SVG
    const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 32 32" fill="none" stroke="#fff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect width="18" height="11" x="3" y="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 9.9-1"/></svg>`;
    const blob = new Blob([svg], { type: "image/svg+xml" });
    const url = URL.createObjectURL(blob);
    const img = new Image();
    img.onload = () => {
      setLockIcon(img);
      URL.revokeObjectURL(url);
    };
    img.src = url;
  }, []);

  useEffect(() => {
    const loadCryptoIcon = (name, ticker) => {
      const img = new Image();
      img.crossOrigin = "anonymous";
      img.src =
        window.location.origin +
        `/static/crypto-icons/${ticker.toLowerCase()}.svg`;
      img.onload = () => {
        setCryptoIcons((prev) => ({ ...prev, [name]: img }));
      };
      img.style.width = "100%";
      img.style.height = "100%";
      img.onerror = () => {
        const fallbackImg = new Image();
        fallbackImg.crossOrigin = "anonymous";
        fallbackImg.src = `${window.location.origin}/static/crypto-icons/${ticker.toLowerCase()}.svg`;
        fallbackImg.onload = () => {
          setCryptoIcons((prev) => ({ ...prev, [name]: fallbackImg }));
        };
      };
    };

    loadCryptoIcon("Binance", "binance");
    loadCryptoIcon("Gate.io", "gate");
    loadCryptoIcon("Bitcoin", "btc");
    loadCryptoIcon("Coinbase", "coinbase");
    loadCryptoIcon("Ethereum", "eth");
    loadCryptoIcon("BNB", "bnb");
    loadCryptoIcon("Solana", "sol");
  }, []);

  useEffect(() => {
    // Fetch graph data from API
    const fetchGraphData = async () => {
      setLoading(true);
      try {
        // Build API URL based on centerEntity
        let url = `${API_URL}/api/graph/network?limit_nodes=150&limit_edges=400&depth=2`;
        
        if (centerEntity) {
          const [type, id] = centerEntity.split(':');
          if (type && id) {
            url = `${API_URL}/api/graph/network/${type}/${id}?limit_nodes=150&limit_edges=400&depth=2`;
          }
        }
        
        const response = await fetch(url);
        if (!response.ok) throw new Error('Failed to fetch graph');
        
        const data = await response.json();
        
        // Transform API data to original format
        // API returns: nodes [{id, label, type, ...}], edges [{source, target, relation, value, ...}]
        // Original format: nodes [{id, label, type, size}], links [{source, target, value}]
        
        const nodes = [];
        const nodeMap = new Map();
        
        // Find main node - use centerEntity if provided, else first exchange or first node
        let mainNodeData;
        if (centerEntity) {
          mainNodeData = data.nodes.find(n => n.id === centerEntity) || data.nodes[0];
        } else {
          mainNodeData = data.nodes.find(n => n.id === 'exchange:binance') || data.nodes[0];
        }
        
        if (!mainNodeData) {
          setGraphData({ nodes: [], links: [] });
          setLoading(false);
          return;
        }
        
        // Add main node
        const mainNode = {
          id: mainNodeData.id,
          label: mainNodeData.label,
          type: "main",
          size: 5,
        };
        nodes.push(mainNode);
        nodeMap.set(mainNodeData.id, mainNode);
        
        // Add other nodes as "lock" type (satellite nodes)
        data.nodes.forEach((apiNode, idx) => {
          if (apiNode.id === mainNodeData.id) return;
          
          const node = {
            id: apiNode.id,
            label: apiNode.label.slice(0, 8) + (apiNode.label.length > 8 ? '...' : ''),
            type: "lock",
            size: 5,
          };
          nodes.push(node);
          nodeMap.set(apiNode.id, node);
        });
        
        // Position nodes in circular layout around main node
        const childNodes = nodes.slice(1);
        const spreadRadius = 180 + Math.max(0, childNodes.length + 500) * 1.2;
        const angleStep = (2 * Math.PI) / childNodes.length;
        childNodes.forEach((node, idx) => {
          node.x = Math.cos(idx * angleStep) * spreadRadius;
          node.y = Math.sin(idx * angleStep) * spreadRadius;
        });
        
        // Transform edges to links with multiple connections for visual effect
        const links = [];
        
        // Group edges by source-target pair and count them properly
        const edgeGroups = new Map();
        data.edges.forEach(edge => {
          const key = `${edge.source}|${edge.target}`;
          if (!edgeGroups.has(key)) {
            edgeGroups.set(key, []);
          }
          edgeGroups.get(key).push(edge);
        });
        
        // Create links - ONE line per investment/relation (not artificially multiplied)
        // Multiple edges in API = multiple actual investments = multiple visual lines
        edgeGroups.forEach((edges, key) => {
          const [source, target] = key.split('|');
          if (!nodeMap.has(source) || !nodeMap.has(target)) return;
          
          // Each edge from API represents a REAL investment/relation
          // So we create exactly that many lines
          edges.forEach((edge, index) => {
            const value = edge.value !== undefined ? edge.value : (Math.random() * 200 - 100);
            links.push({ 
              source, 
              target, 
              value,
              metadata: edge.metadata || {},
              relation: edge.relation || 'invested_in'
            });
          });
        });
        
        // Process links for curved lines (multiple connections)
        const linkMap = new Map();
        const processedLinks = links.map((link) => {
          const key = [link.source, link.target].sort().join("-");
          const count = linkMap.get(key) || 0;
          linkMap.set(key, count + 1);
          return { ...link, connectionIndex: count };
        });

        const finalLinks = processedLinks.map((link) => {
          const key = [link.source, link.target].sort().join("-");
          return { ...link, total: linkMap.get(key) };
        });
        
        setGraphData({ nodes, links: finalLinks });
        setCurrentMainNode(mainNode);
        setHopLevel(1);
        setNavigationHistory([mainNode]);
        
      } catch (err) {
        console.error('[GraphViewer] API fetch failed, using fallback:', err);
        // Fallback to original mock data generation
        generateMockData();
      } finally {
        setLoading(false);
      }
    };
    
    // Fallback mock data generator (original logic)
    const generateMockData = () => {
      const entityNodeMap = {
        Binance: { id: "Binance", label: "Binance", type: "exchange" },
        Coinbase: { id: "Coinbase", label: "Coinbase", type: "exchange" },
        "Gate.io": { id: "Gate.io", label: "Gate.io", type: "exchange" },
        Bitcoin: { id: "Bitcoin", label: "Bitcoin", type: "token" },
        Ethereum: { id: "Ethereum", label: "Ethereum", type: "token" },
      };

      const selectedNode = entityNodeMap[selectedEntity.name];
      if (!selectedNode) {
        setGraphData({ nodes: [], links: [] });
        return;
      }

      const mainNode = {
        id: selectedNode.id,
        label: selectedNode.label,
        type: selectedNode.type,
        size: 5,
      };

      const nodes = [mainNode];
      setCurrentMainNode(mainNode);
      setHopLevel(1);
      setNavigationHistory([mainNode]);

      const generateAddresses = (prefix, count, targetId) => {
        const addresses = [];
        for (let i = 0; i < count; i++) {
          addresses.push({
            id: `${targetId}_${prefix}${i}`,
            label: `${prefix.slice(0, 5)}...`,
            type: "lock",
            size: 5,
          });
        }
        return addresses;
      };

      const nodeCount = 120;
      const childAddresses = generateAddresses(
        selectedNode.label.substring(0, 7),
        nodeCount,
        selectedNode.id
      );

      const spreadRadius = 180 + Math.max(0, childAddresses.length + 500) * 1.2;
      const angleStep = (2 * Math.PI) / childAddresses.length;
      childAddresses.forEach((node, idx) => {
        node.x = Math.cos(idx * angleStep) * spreadRadius;
        node.y = Math.sin(idx * angleStep) * spreadRadius;
      });

      const allNodes = [...nodes, ...childAddresses];
      const links = [];

      const addLink = (source, target, multiplier = 1) => {
        for (let i = 0; i < multiplier; i++) {
          links.push({ source, target, value: Math.random() * 200 - 100 });
        }
      };

      childAddresses.forEach((addr, idx) => {
        let mult = 1;
        if (idx < 10) mult = Math.floor(Math.random() * 18) + 3;
        else if (idx < 30) mult = Math.floor(Math.random() * 9) + 2;
        else if (idx < 60) mult = Math.floor(Math.random() * 4) + 1;
        addLink(selectedNode.id, addr.id, mult);
      });

      const linkMap = new Map();
      const processedLinks = links.map((link) => {
        const key = [link.source, link.target].sort().join("-");
        const count = linkMap.get(key) || 0;
        linkMap.set(key, count + 1);
        return { ...link, connectionIndex: count };
      });

      const finalLinks = processedLinks.map((link) => {
        const key = [link.source, link.target].sort().join("-");
        return { ...link, total: linkMap.get(key) };
      });

      setGraphData({ nodes: allNodes, links: finalLinks });
    };
    
    fetchGraphData();
  }, [centerEntity, selectedEntity.name]);

  const drawNode = useCallback(
    (node, ctx, globalScale) => {
      const size = node.size || 5;

      ctx.save();

      if (node.type !== "lock") {
        if (node.type === "main") {
          ctx.shadowColor = "#3b82f6";
          ctx.shadowBlur = 20;
        } else if (node.type === "exchange") {
          ctx.shadowColor = "#f97316";
          ctx.shadowBlur = 15;
        } else if (node.type === "token") {
          ctx.shadowColor = "#22c55e";
          ctx.shadowBlur = 15;
        } else if (node.id === "Bitcoin") {
          ctx.shadowColor = "#fbbf24";
          ctx.shadowBlur = 15;
        }

        const cryptoIcon = cryptoIcons[node.id];

        if (node.id.startsWith("Wallet")) {
          ctx.beginPath();
          ctx.arc(node.x, node.y, size, 0, 2 * Math.PI);
          ctx.fillStyle = "#2D2F37";
          ctx.fill();
          ctx.fillStyle = "#fff";
          ctx.font = `2px Inter, sans-serif`;
          ctx.textAlign = "center";
          ctx.textBaseline = "middle";
          ctx.fillText(node.label, node.x, node.y);
        } else {
          if (cryptoIcon) {
            const isWithoutScale =
              selectedEntity.name === "Binance" ||
              selectedEntity.name === "Coinbase" ||
              selectedEntity.name === "Gate.io";
            const iconSize = size * (isWithoutScale ? 3 : 2);
            ctx.drawImage(
              cryptoIcon,
              node.x - size * (isWithoutScale ? 2.25 : 1),
              node.y - size * (isWithoutScale ? 1.5 : 1),
              iconSize * (isWithoutScale ? 1.5 : 1),
              iconSize
            );
          } else {
            ctx.beginPath();
            ctx.arc(node.x, node.y, size, 0, 2 * Math.PI);
            ctx.fillStyle = "#2D2F37";
            ctx.fill();
            ctx.fillStyle = "#fff";
            ctx.font = `2px Inter, sans-serif`;
            ctx.textAlign = "center";
            ctx.textBaseline = "middle";
            ctx.fillText(node.label, node.x, node.y);
          }
        }

        ctx.shadowBlur = 0;
      } else {
        ctx.beginPath();
        ctx.arc(node.x, node.y, size, 0, 2 * Math.PI);
        ctx.fillStyle = "#2D2F37";
        ctx.fill();

        ctx.fillStyle = "#fff";
        ctx.font = `2px Inter, sans-serif`;
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        ctx.fillText(node.label, node.x, node.y);
      }

      if (node.fx !== undefined && node.fy !== undefined && lockIcon) {
        const lockSize = size * 0.7;
        const lockX = node.x - size * 0.7;
        const lockY = node.y - size * 0.7;

        ctx.drawImage(
          lockIcon,
          lockX - lockSize / 2,
          lockY - lockSize / 2,
          lockSize,
          lockSize
        );
      }

      ctx.restore();
    },
    [lockIcon, cryptoIcons, selectedEntity]
  );

  const drawLink = useCallback(
    (link, ctx, globalScale) => {
      const start = link.source;
      const end = link.target;

      if (typeof start !== "object" || typeof end !== "object") return;

      ctx.save();
      ctx.beginPath();

      const isMultiple = link.total && link.total > 1;

      if (!isMultiple || link.connectionIndex === 0) {
        ctx.moveTo(start.x, start.y);
        ctx.lineTo(end.x, end.y);
      } else {
        const dx = end.x - start.x;
        const dy = end.y - start.y;
        const distance = Math.sqrt(dx * dx + dy * dy);

        const side = link.connectionIndex % 2 === 1 ? 1 : -1;
        const curveMultiplier = Math.ceil(link.connectionIndex / 2);
        const curveOffset = side * curveMultiplier;

        const midX = (start.x + end.x) / 2;
        const midY = (start.y + end.y) / 2;
        const normalX = -dy / distance;
        const normalY = dx / distance;

        const controlX = midX + normalX * curveOffset;
        const controlY = midY + normalY * curveOffset;

        ctx.moveTo(start.x, start.y);
        ctx.quadraticCurveTo(controlX, controlY, end.x, end.y);
      }

      const absValue = Math.abs(link.value || 0);
      const values = graphData.links
        .map((l) => Math.abs(l.value || 0))
        .filter((v) => !isNaN(v) && v !== 0);
      const minValue = values.length > 0 ? Math.min(...values) : 1;
      const maxValue = values.length > 0 ? Math.max(...values) : 1;
      let opacity = 0.5;

      if (values.length > 0 && !isNaN(absValue) && maxValue !== minValue) {
        opacity = 0.1 + ((absValue - minValue) / (maxValue - minValue)) * 0.4;
      }

      opacity = Math.max(0.1, Math.min(1.0, opacity));

      if (link.value < 0) {
        ctx.strokeStyle = `rgba(239, 68, 68, ${opacity})`;
      } else {
        ctx.strokeStyle = `rgba(90, 169, 113, ${opacity})`;
      }
      ctx.lineWidth = 0.1;

      ctx.stroke();
      ctx.restore();
    },
    [graphData.links]
  );

  const handleNodeDrag = useCallback((node) => {
    if (node) {
      node.fx = node.x;
      node.fy = node.y;
    }
  }, []);

  const handleNodeDragEnd = useCallback((node) => {
    if (node) {
      node.fx = node.x;
      node.fy = node.y;
    }
  }, []);

  const handleEngineStop = useCallback(() => {
    if (forceRef.current?.d3Force) {
      forceRef.current.d3Force("charge")?.strength(-30);
      forceRef.current.d3Force("link")?.distance(60);
      forceRef.current.d3Force("center")?.strength(1.2);
    }
  }, []);

  const handleNodeClick = useCallback(
    (node) => {
      if (
        (node.type === "exchange" || node.type === "token") &&
        navigationHistory.length > 1
      ) {
        const clickedIndex = navigationHistory.findIndex(
          (histNode) => histNode.id === node.id
        );

        if (clickedIndex !== -1) {
          const targetNode = navigationHistory[clickedIndex];
          const newHistory = navigationHistory.slice(0, clickedIndex + 1);
          const newHopLevel = newHistory.length;

          setNavigationHistory(newHistory);
          setHopLevel(newHopLevel);
          setCurrentMainNode(targetNode);

          const newMainNode = {
            ...targetNode,
            type: "main",
            size: 5,
          };

          const nodes = [newMainNode];

          if (clickedIndex > 0) {
            const parentNodeInGraph = {
              ...newHistory[clickedIndex - 1],
              type:
                newHistory[clickedIndex - 1].type === "main"
                  ? "exchange"
                  : newHistory[clickedIndex - 1].type,
              size: 5,
            };
            nodes.push(parentNodeInGraph);
          }

          let nodeCount = 120;
          const activeFilters =
            Object.values(onChainFilters).filter(Boolean).length;
          if (activeFilters < 8) {
            nodeCount = Math.max(30, Math.floor(120 * (activeFilters / 8)));
          }

          let childAddresses = [];
          const shouldGenerateChildren = newHopLevel < 3;

          if (shouldGenerateChildren) {
            const generateAddresses = (prefix, count, targetId) => {
              const addresses = [];
              for (let i = 0; i < count; i++) {
                addresses.push({
                  id: `${targetId}_back_${prefix}${i}`,
                  label: `${prefix.slice(0, 5)}...`,
                  type: "lock",
                  size: clickedIndex === 0 ? 5 : 4,
                });
              }
              return addresses;
            };

            const count = clickedIndex === 0 ? nodeCount : 30;
            childAddresses = generateAddresses(
              targetNode.label.substring(0, 5),
              count,
              targetNode.id
            );
          }

          const totalNodes = shouldGenerateChildren
            ? childAddresses.length + (clickedIndex > 0 ? 1 : 0)
            : clickedIndex > 0
            ? 1
            : 0;
          const spreadRadius =
            clickedIndex === 0
              ? 180 + Math.max(0, childAddresses.length + 500) * 1.2
              : 200;
          const angleStep = (2 * Math.PI) / (totalNodes + 1);
          const mainNodeX = newMainNode.x || 0;
          const mainNodeY = newMainNode.y || 0;

          if (clickedIndex > 0) {
            nodes[1].x = mainNodeX + Math.cos(0) * spreadRadius;
            nodes[1].y = mainNodeY + Math.sin(0) * spreadRadius;
          }

          if (shouldGenerateChildren) {
            childAddresses.forEach((childNode, idx) => {
              childNode.x =
                mainNodeX + Math.cos((idx + 1) * angleStep) * spreadRadius;
              childNode.y =
                mainNodeY + Math.sin((idx + 1) * angleStep) * spreadRadius;
            });
          }

          const allNodes = [...nodes, ...childAddresses];

          const links = [];

          const addLink = (source, target, multiplier = 1) => {
            for (let i = 0; i < multiplier; i++) {
              links.push({ source, target, value: Math.random() * 200 - 100 });
            }
          };

          if (clickedIndex > 0) {
            addLink(
              newMainNode.id,
              nodes[1].id,
              Math.floor(Math.random() * 5) + 3
            );
          }

          if (shouldGenerateChildren) {
            childAddresses.forEach((addr, idx) => {
              let mult = 1;
              if (clickedIndex === 0) {
                if (idx < 10) {
                  mult = Math.floor(Math.random() * 18) + 3;
                } else if (idx < 30) {
                  mult = Math.floor(Math.random() * 9) + 2;
                } else if (idx < 60) {
                  mult = Math.floor(Math.random() * 4) + 1;
                }
              } else {
                if (idx < 5) {
                  mult = Math.floor(Math.random() * 4) + 2;
                } else if (idx < 15) {
                  mult = Math.floor(Math.random() * 3) + 1;
                }
              }
              addLink(newMainNode.id, addr.id, mult);
            });
          }

          const linkMap = new Map();
          const processedLinks = links.map((link) => {
            const key = [link.source, link.target].sort().join("-");
            const count = linkMap.get(key) || 0;
            linkMap.set(key, count + 1);
            return { ...link, connectionIndex: count };
          });

          const finalLinks = processedLinks.map((link) => {
            const key = [link.source, link.target].sort().join("-");
            return { ...link, total: linkMap.get(key) };
          });

          setGraphData({ nodes: allNodes, links: finalLinks });
          return;
        }
      }

      if (node.type === "lock" && currentMainNode) {
        const newHopLevel = hopLevel + 1;
        setHopLevel(newHopLevel);

        const newMainNode = {
          id: node.id,
          label: node.label,
          type: "main",
          size: 5,
        };

        setNavigationHistory([...navigationHistory, newMainNode]);

        const parentNodeInGraph = {
          ...currentMainNode,
          type:
            currentMainNode.type === "main" ? "exchange" : currentMainNode.type,
          size: 5,
        };

        const nodes = [newMainNode, parentNodeInGraph];

        const shouldGenerateChildren = newHopLevel < 3;

        let childAddresses = [];

        if (shouldGenerateChildren) {
          const generateAddresses = (prefix, count, targetId) => {
            const addresses = [];
            for (let i = 0; i < count; i++) {
              addresses.push({
                id: `${targetId}_child_${prefix}${i}`,
                label: `${prefix.slice(0, 5)}...`,
                type: "lock",
                size: 4,
              });
            }
            return addresses;
          };

          childAddresses = generateAddresses(
            node.label.substring(0, 5),
            30,
            node.id
          );
        }

        const totalNodes = shouldGenerateChildren
          ? childAddresses.length + 1
          : 1;
        const spreadRadius = 200;
        const angleStep = (2 * Math.PI) / (totalNodes + 1);

        const mainNodeX = newMainNode.x || 0;
        const mainNodeY = newMainNode.y || 0;
        parentNodeInGraph.x = mainNodeX + Math.cos(0) * spreadRadius;
        parentNodeInGraph.y = mainNodeY + Math.sin(0) * spreadRadius;

        if (shouldGenerateChildren) {
          childAddresses.forEach((childNode, idx) => {
            childNode.x =
              mainNodeX + Math.cos((idx + 1) * angleStep) * spreadRadius;
            childNode.y =
              mainNodeY + Math.sin((idx + 1) * angleStep) * spreadRadius;
          });
        }

        const allNodes = [...nodes, ...childAddresses];

        const links = [];

        const addLink = (source, target, multiplier = 1) => {
          for (let i = 0; i < multiplier; i++) {
            links.push({ source, target, value: Math.random() * 200 - 100 });
          }
        };

        addLink(
          newMainNode.id,
          parentNodeInGraph.id,
          Math.floor(Math.random() * 5) + 3
        );

        if (shouldGenerateChildren) {
          childAddresses.forEach((addr, idx) => {
            let mult = 1;
            if (idx < 5) {
              mult = Math.floor(Math.random() * 4) + 2;
            } else if (idx < 15) {
              mult = Math.floor(Math.random() * 3) + 1;
            }
            addLink(newMainNode.id, addr.id, mult);
          });
        }

        const linkMap = new Map();
        const processedLinks = links.map((link) => {
          const key = [link.source, link.target].sort().join("-");
          const count = linkMap.get(key) || 0;
          linkMap.set(key, count + 1);
          return { ...link, connectionIndex: count };
        });

        const finalLinks = processedLinks.map((link) => {
          const key = [link.source, link.target].sort().join("-");
          return { ...link, total: linkMap.get(key) };
        });

        setCurrentMainNode(newMainNode);
        setGraphData({ nodes: allNodes, links: finalLinks });
      }
    },
    [currentMainNode, hopLevel, navigationHistory, selectedTab, filters, onChainFilters]
  );

  const effectiveWidth = isFullscreen
    ? fsSize.w
    : typeof window !== "undefined"
    ? window.innerWidth
    : 800;
  const effectiveHeight = isFullscreen
    ? fsSize.h
    : typeof window !== "undefined"
    ? window.innerHeight
    : 600;

  return (
    <div
      ref={containerRef}
      data-testid="force-graph-container"
      style={{
        position: isFullscreen ? "fixed" : "relative",
        width: isFullscreen ? "100vw" : "100%",
        height: isFullscreen ? "100vh" : "100%",
        top: isFullscreen ? 0 : "auto",
        left: isFullscreen ? 0 : "auto",
        zIndex: isFullscreen ? 9999 : "auto",
        backgroundColor: isFullscreen ? "#0a0e1a" : "transparent",
      }}
      onMouseMove={(e) => setMousePos({ x: e.clientX, y: e.clientY })}
    >
      {/* Fullscreen toggle button */}
      <button
        data-testid="graph-fullscreen-btn"
        onClick={toggleFullscreen}
        style={{
          position: "absolute",
          top: "20px",
          right: "20px",
          zIndex: 10,
          backgroundColor: "rgba(30, 41, 59, 0.8)",
          border: "1px solid rgba(148, 163, 184, 0.2)",
          borderRadius: "8px",
          padding: "9.5px 12px",
          color: "#f8fafc",
          cursor: "pointer",
          display: "flex",
          alignItems: "center",
          gap: "8px",
          fontSize: "14px",
          fontWeight: 500,
          transition: "all 0.2s ease",
          backdropFilter: "blur(10px)",
        }}
        onMouseEnter={(e) => {
          e.currentTarget.style.backgroundColor = "rgba(30, 41, 59, 1)";
          e.currentTarget.style.borderColor = "rgba(148, 163, 184, 0.4)";
        }}
        onMouseLeave={(e) => {
          e.currentTarget.style.backgroundColor = "rgba(30, 41, 59, 0.8)";
          e.currentTarget.style.borderColor = "rgba(148, 163, 184, 0.2)";
        }}
      >
        {isFullscreen ? (
          <>
            <svg
              width="16"
              height="16"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <path d="M8 3v3a2 2 0 0 1-2 2H3m18 0h-3a2 2 0 0 1-2-2V3m0 18v-3a2 2 0 0 1 2-2h3M3 16h3a2 2 0 0 1 2 2v3" />
            </svg>
            <span>Exit Fullscreen</span>
          </>
        ) : (
          <>
            <svg
              width="16"
              height="16"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <path d="M8 3H5a2 2 0 0 0-2 2v3m18 0V5a2 2 0 0 0-2-2h-3m0 18h3a2 2 0 0 0 2-2v-3M3 16v3a2 2 0 0 0 2 2h3" />
            </svg>
            <span>Fullscreen</span>
          </>
        )}
      </button>
      <ForceGraph
        ref={forceRef}
        graphData={graphData}
        width={containerRef.current?.clientWidth || effectiveWidth}
        height={effectiveHeight}
        backgroundColor="#0a0e1a"
        nodeCanvasObject={drawNode}
        linkCanvasObject={drawLink}
        nodeLabel={(node) => node.label}
        nodeRelSize={6}
        nodePointerAreaPaint={(node, color, ctx) => {
          const size = node.size || 5;
          ctx.fillStyle = color;
          ctx.beginPath();
          ctx.arc(node.x, node.y, size, 0, 2 * Math.PI);
          ctx.fill();
        }}
        linkWidth={1}
        linkDirectionalParticles={0}
        cooldownTicks={100}
        onNodeDrag={handleNodeDrag}
        onNodeDragEnd={handleNodeDragEnd}
        onEngineStop={handleEngineStop}
        onNodeHover={(node) => {
          document.body.style.cursor = node ? "pointer" : "default";
        }}
        onNodeClick={handleNodeClick}
        onLinkHover={(link) => {
          setHoveredLink(link);
        }}
        enableNodeDrag={true}
        enableZoomInteraction={true}
        enablePanInteraction={true}
        warmupTicks={50}
        d3VelocityDecay={0.5}
      />
      {hoveredLink && (
        <div
          style={{
            position: "fixed",
            left: mousePos.x + 10,
            top: mousePos.y + 10,
            backgroundColor: "rgba(0, 0, 0, 0.8)",
            color: "white",
            padding: "4px 8px",
            borderRadius: "4px",
            fontSize: "12px",
            pointerEvents: "none",
            zIndex: 1000,
          }}
        >
          {hoveredLink.value.toFixed(2)}$
        </div>
      )}
    </div>
  );
};

export default ForceGraphViewer;
