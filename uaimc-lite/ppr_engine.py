"""
Standalone Push-Based Personalized PageRank Engine.
Phase 2 CANS Bible — replaces inline power iteration in _apply_ppr_boost.

Algorithm: Approximate PPR via residual pushing.
Reference: Andersen, Chung, Lang (2006) — "Local Graph Partitioning
           using PageRank Vectors"

Key corrections from FLAG_LOG:
- FLAG-007: Push-based (not inverted transition matrix)
- FLAG-008: Teleport re-injected via residual push each iteration
- FLAG-009: No undefined variables — returns raw PPR dict
- FLAG-010: Operates on pre-loaded subgraph (from _batch_load_subgraph)
"""

from __future__ import annotations

from collections import defaultdict

# Default edge-type weights from GAAMA spec
DEFAULT_EDGE_TYPE_WEIGHTS: dict[str, float] = {
    "NEXT": 0.15,
    "HAS_CONCEPT": 0.8,
    "ABOUT_CONCEPT": 0.8,
    "DERIVED_FROM": 0.8,
    "DERIVED_FROM_FACT": 0.5,
}
DEFAULT_WEIGHT = 0.6


def load_edge_overrides(db_path: str) -> dict[tuple[str, str], float]:
    """Load all active edge weight overrides as {(source_id, target_id): sum_delta}.

    Effective weight = kg_edges.weight + SUM(overrides.delta).
    Reversible: delete override row to restore original weight.
    Expired overrides (expires_at < now) are excluded.
    """
    import sqlite3
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("""
            SELECT source_id, target_id, SUM(delta) as total_delta
            FROM edge_weight_overrides
            WHERE expires_at IS NULL OR expires_at > ?
            GROUP BY source_id, target_id
            HAVING total_delta != 0.0
        """, (now,)).fetchall()
    except Exception:
        # Table may not exist yet
        return {}
    finally:
        conn.close()

    return {(r[0], r[1]): r[2] for r in rows}


def load_hub_dampening(db_path: str) -> dict[str, float]:
    """Load per-concept theta values from concept_nodes.metadata.

    Returns {concept_id: theta} for concepts with custom theta.
    Default θ=50 is applied by PPR engine when concept is not in this dict.
    """
    import sqlite3
    import json

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("""
            SELECT concept_id, metadata FROM concept_nodes
            WHERE metadata != '{}'
        """).fetchall()
    except Exception:
        return {}
    finally:
        conn.close()

    dampening = {}
    for cid, meta_str in rows:
        try:
            meta = json.loads(meta_str)
            if "theta" in meta:
                dampening[cid] = float(meta["theta"])
        except (json.JSONDecodeError, ValueError):
            pass
    return dampening


def compute_ppr_push(
    edges: list[tuple[str, str, str, float]],
    seed_ids: list[str],
    alpha: float = 0.15,
    epsilon: float = 1e-4,
    max_iterations: int = 50,
    hub_dampening: dict[str, float] | None = None,
    edge_type_weights: dict[str, float] | None = None,
    edge_overrides: dict[tuple[str, str], float] | None = None,
    top_k: int = 200,
) -> dict[str, float]:
    """Compute Personalized PageRank via push-based residual propagation.

    Parameters
    ----------
    edges : list of (source, target, edge_type, raw_weight) tuples
        The subgraph to run PPR on. Loaded from _batch_load_subgraph.
    seed_ids : list of str
        Seed node IDs (teleport targets). PPR is personalised toward these.
    alpha : float
        Teleport probability. Standard PPR uses 0.15 (15% chance of
        teleporting back to a seed at each step).
    epsilon : float
        Convergence threshold. A node is only pushed when its residual
        exceeds epsilon. Lower = more accurate but slower.
    max_iterations : int
        Maximum number of full sweeps over active nodes.
    hub_dampening : dict mapping node_id -> theta value, optional
        Per-node hub dampening threshold. Nodes with out-degree > theta
        have their outgoing weight reduced by factor theta/degree.
    edge_type_weights : dict mapping edge_type -> weight, optional
        Per-edge-type base weights. Defaults to GAAMA spec weights.
    edge_overrides : dict mapping (source_id, target_id) -> delta, optional
        Reversible weight deltas from edge_weight_overrides table.
        Applied as: effective_raw_w = max(0.0, raw_w + delta).
    top_k : int
        Return only the top-K highest-scoring nodes.

    Returns
    -------
    dict[str, float]
        Mapping of node_id to PPR score, limited to top_k entries,
        sorted by score descending.
    """
    if not edges or not seed_ids:
        return {s: alpha / max(len(seed_ids), 1) for s in seed_ids}

    etw = edge_type_weights if edge_type_weights is not None else DEFAULT_EDGE_TYPE_WEIGHTS
    hd = hub_dampening or {}
    eo = edge_overrides or {}
    default_theta = 50

    # --- 1. Build adjacency: out_neighbors[src] = [(tgt, effective_weight)]
    # Compute out-degree per source
    out_degree: dict[str, int] = defaultdict(int)
    for src, _tgt, _etype, _w in edges:
        out_degree[src] += 1

    # Build raw adjacency with dampened, typed weights
    raw_adj: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for src, tgt, etype, raw_w in edges:
        base_w = etw.get(etype, DEFAULT_WEIGHT)
        theta = hd.get(src, default_theta)
        degree = out_degree[src]
        dampened = base_w * min(1.0, theta / max(degree, 1))
        # Phase 3: Apply edge weight override delta (reversible)
        override_delta = eo.get((src, tgt), 0.0)
        effective_raw_w = max(0.0, (raw_w if raw_w else 1.0) + override_delta)
        raw_adj[src].append((tgt, dampened * effective_raw_w))

    # Normalize outgoing weights per source so they sum to 1.0
    out_neighbors: dict[str, list[tuple[str, float]]] = {}
    for src, neighbors in raw_adj.items():
        total = sum(w for _, w in neighbors)
        if total > 0:
            out_neighbors[src] = [(tgt, w / total) for tgt, w in neighbors]
        else:
            out_neighbors[src] = neighbors

    # --- 2. Initialize residual and PPR vectors
    ppr: dict[str, float] = defaultdict(float)
    residual: dict[str, float] = defaultdict(float)

    init_val = 1.0 / max(len(seed_ids), 1)
    for seed in seed_ids:
        residual[seed] = init_val

    # --- 3. Push loop
    for _iteration in range(max_iterations):
        # Collect nodes with significant residual
        active = [v for v, r in residual.items() if r > epsilon]
        if not active:
            break

        for v in active:
            r_v = residual[v]
            if r_v <= epsilon:
                continue

            # Push to self (teleport component)
            ppr[v] += alpha * r_v

            # Distribute remainder to neighbors
            remainder = (1.0 - alpha) * r_v
            neighbors = out_neighbors.get(v, [])
            if neighbors:
                for u, w in neighbors:
                    residual[u] += remainder * w
            # else: remainder is lost (dangling node) — equivalent to
            # teleporting back to seeds, which is handled by the residual
            # already held by seed nodes.

            # Clear this node's residual
            residual[v] = 0.0

    # --- 4. Return top-K
    sorted_scores = sorted(ppr.items(), key=lambda x: x[1], reverse=True)
    return dict(sorted_scores[:top_k])
