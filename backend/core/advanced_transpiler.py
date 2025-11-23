# backend/core/advanced_transpiler.py

from typing import Dict, List, Any, Tuple, Optional
from dataclasses import dataclass
import re
from enum import Enum

class PatternType(Enum):
    SIMPLE_PATH = "simple_path"
    COMPLEX_PATH = "complex_path"
    OPTIONAL_PATH = "optional_path"
    VARIABLE_LENGTH = "variable_length"

@dataclass
class PathComponent:
    nodes: List[Dict[str, Any]]
    edges: List[Dict[str, Any]]
    pattern_type: PatternType
    optional: bool = False

class AdvancedCypherTranspiler:
    """Advanced Cypher to SQL transpiler with complex pattern support"""
    
    def __init__(self):
        self.cte_counter = 0
        self.join_tracker = []
        self.alias_map = {}
        
    def transpile_complex_query(self, cypher_query: str) -> Dict[str, Any]:
        """Transpile complex Cypher with pattern analysis"""
        
        # Step 1: Tokenize and identify clauses
        clauses = self._extract_clauses(cypher_query)
        
        # Step 2: Parse patterns
        pattern_graph = self._build_pattern_graph(clauses['match'])
        
        # Step 3: Analyze path dependencies
        path_dependencies = self._analyze_path_dependencies(pattern_graph)
        
        # Step 4: Generate optimized SQL
        sql_query = self._generate_optimized_sql(
            pattern_graph,
            path_dependencies,
            clauses
        )
        
        return {
            'original_cypher': cypher_query,
            'transpiled_sql': sql_query,
            'pattern_graph': pattern_graph,
            'dependencies': path_dependencies,
            'metadata': {
                'num_ctes': self.cte_counter,
                'joins': self.join_tracker,
                'complexity': self._calculate_complexity(pattern_graph)
            }
        }
    
    def _extract_clauses(self, query: str) -> Dict[str, str]:
        """Extract all clauses from Cypher query"""
        clauses = {}
        
        # Extract MATCH clauses (including multiple consecutive ones)
        match_pattern = r'MATCH\s+([^W\n]+?)(?=\s+(?:WHERE|WITH|RETURN|$))'
        clauses['match'] = re.findall(match_pattern, query, re.IGNORECASE | re.DOTALL)
        
        # Extract WHERE
        where_match = re.search(r'WHERE\s+([^W\n]+?)(?=\s+(?:WITH|RETURN|$))', query, re.IGNORECASE)
        clauses['where'] = where_match.group(1) if where_match else None
        
        # Extract WITH (for intermediate results)
        with_matches = re.findall(r'WITH\s+([^M\n]+?)(?=\s+(?:MATCH|RETURN))', query, re.IGNORECASE)
        clauses['with'] = with_matches
        
        # Extract RETURN
        return_match = re.search(r'RETURN\s+([^\n]+?)(?:\s+(?:ORDER|LIMIT|$))', query, re.IGNORECASE)
        clauses['return'] = return_match.group(1) if return_match else None
        
        return clauses
    
    def _build_pattern_graph(self, match_clauses: List[str]) -> Dict[str, Any]:
        """Build a graph representing pattern relationships"""
        pattern_graph = {
            'nodes': {},
            'edges': {},
            'paths': []
        }
        
        for match in match_clauses:
            # Parse nodes in pattern
            node_pattern = r'\((\w+)(?::(\w+))?\s*(?:\{([^}]+)\})?\)'
            for node_match in re.finditer(node_pattern, match):
                node_name = node_match.group(1)
                node_label = node_match.group(2) or 'Node'
                node_props = self._parse_properties(node_match.group(3) or '')
                
                pattern_graph['nodes'][node_name] = {
                    'label': node_label,
                    'properties': node_props,
                    'position': node_match.start()
                }
            
            # Parse edges and their connections
            edge_pattern = r'-\[(\w+)?(?::(\w+))?\s*(?:\{([^}]+)\})?\]-([>|<]*)-'
            edges_in_match = []
            for edge_match in re.finditer(edge_pattern, match):
                edge_name = edge_match.group(1) or f'e{len(pattern_graph["edges"])}'
                edge_label = edge_match.group(2) or 'RELATED'
                edge_props = self._parse_properties(edge_match.group(3) or '')
                direction = edge_match.group(4) or '->'
                
                edges_in_match.append({
                    'name': edge_name,
                    'label': edge_label,
                    'properties': edge_props,
                    'direction': direction,
                    'position': edge_match.start()
                })
                
                pattern_graph['edges'][edge_name] = {
                    'label': edge_label,
                    'properties': edge_props,
                    'direction': direction
                }
            
            # Build path from parsed nodes and edges
            if edges_in_match:
                pattern_graph['paths'].append({
                    'nodes': list(pattern_graph['nodes'].keys()),
                    'edges': edges_in_match
                })
        
        return pattern_graph
    
    def _analyze_path_dependencies(self, pattern_graph: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze dependencies between paths for JOIN order"""
        dependencies = {
            'shared_nodes': {},
            'join_order': [],
            'cte_strategy': 'sequential'  # or 'parallel'
        }
        
        # Find shared nodes between paths
        if len(pattern_graph['paths']) > 1:
            for i, path1 in enumerate(pattern_graph['paths']):
                for j, path2 in enumerate(pattern_graph['paths'][i+1:], i+1):
                    shared = set(path1['nodes']) & set(path2['nodes'])
                    if shared:
                        key = f"path_{i}_path_{j}"
                        dependencies['shared_nodes'][key] = {
                            'nodes': list(shared),
                            'path1_index': i,
                            'path2_index': j
                        }
        
        # Determine optimal JOIN order
        dependencies['join_order'] = self._calculate_join_order(pattern_graph)
        
        return dependencies
    
    def _calculate_join_order(self, pattern_graph: Dict[str, Any]) -> List[str]:
        """Calculate optimal JOIN order using heuristics"""
        # Simple heuristic: join tables with fewer relationships first
        tables = list(pattern_graph['nodes'].keys())
        
        # Could implement cardinality estimation here
        return sorted(tables, key=lambda t: len(pattern_graph['edges']))
    
    def _generate_optimized_sql(self, pattern_graph: Dict[str, Any],
                               dependencies: Dict[str, Any],
                               clauses: Dict[str, str]) -> str:
        """Generate optimized SQL with CTEs"""
        sql_parts = []
        
        # Generate CTEs for each path
        cte_definitions = []
        for i, path in enumerate(pattern_graph['paths']):
            self.cte_counter += 1
            cte_name = f"path_{i}"
            cte_sql = self._generate_cte_for_path(path, cte_name, clauses.get('where'))
            cte_definitions.append(cte_sql)
        
        # Build main query
        if cte_definitions:
            sql_parts.append(f"WITH {','.join([f'\\n  {cte}' for cte in cte_definitions])}")
        
        # Build SELECT from CTEs
        if clauses['return']:
            select_sql = self._build_select_from_ctes(
                clauses['return'],
                pattern_graph,
                dependencies
            )
            sql_parts.append(select_sql)
        
        return "\\n".join(sql_parts)
    
    def _generate_cte_for_path(self, path: Dict[str, Any], cte_name: str, where_clause: Optional[str]) -> str:
        """Generate a single CTE for a path"""
        
        # Start with first node table
        first_node = path['nodes']
        sql = f"{cte_name} AS (\\n"
        sql += f"  SELECT "
        
        # Select all node/edge attributes
        select_items = []
        for node in path['nodes']:
            select_items.append(f"{node}.* AS {node}_data")
        
        sql += ",\\n         ".join(select_items)
        
        # Build FROM and JOINs
        sql += f"\\n  FROM {first_node.lower()} AS {first_node}\\n"
        
        # Add edge joins
        for edge in path['edges']:
            sql += self._build_join_for_edge(edge, path['nodes'])
        
        sql += "\\n)"
        
        return sql
    
    def _build_join_for_edge(self, edge: Dict[str, Any], nodes: List[str]) -> str:
        """Build JOIN clause for an edge"""
        edge_table = edge['label'].lower()
        direction = edge['direction']
        
        # Determine source and target nodes from edge position
        src_node = nodes
        tgt_node = nodes if len(nodes) > 1 else nodes
        
        join_type = "INNER JOIN" if 'LEFT' not in edge.get('type', '') else "LEFT JOIN"
        
        join = f"  {join_type} {edge_table} AS {edge['name']}\\n"
        join += f"    ON {src_node}.id = {edge['name']}.src_id\\n"
        join += f"    AND {edge['name']}.tgt_id = {tgt_node}.id"
        
        return join
    
    def _build_select_from_ctes(self, return_clause: str, pattern_graph: Dict[str, Any],
                               dependencies: Dict[str, Any]) -> str:
        """Build SELECT clause from CTEs"""
        
        # Parse return items
        return_items = [item.strip() for item in return_clause.split(',')]
        select_items = []
        group_by_items = []
        
        for item in return_items:
            # Check for aggregations
            if 'COUNT' in item.upper():
                select_items.append(item)
            elif 'SUM' in item.upper():
                select_items.append(item)
            elif 'AVG' in item.upper():
                select_items.append(item)
            elif 'MAX' in item.upper():
                select_items.append(item)
            elif 'MIN' in item.upper():
                select_items.append(item)
            elif 'AS' in item.upper():
                select_items.append(item)
                # Extract column for grouping
                parts = item.split('AS')
                if len(parts) == 2:
                    col = parts.strip()
                    if '.' in col:
                        group_by_items.append(col)
            else:
                select_items.append(item)
                if '.' in item:
                    group_by_items.append(item)
        
        select_clause = "SELECT \\n  " + ",\\n  ".join(select_items)
        
        # Build FROM clause using first CTE
        from_clause = f"\\nFROM path_0"
        
        # Add JOINs between CTEs if needed
        if len(pattern_graph['paths']) > 1:
            for i in range(1, len(pattern_graph['paths'])):
                shared = dependencies['shared_nodes'].get(f"path_0_path_{i}")
                if shared:
                    join_condition = " AND ".join([
                        f"path_0.{node}_data.id = path_{i}.{node}_data.id"
                        for node in shared['nodes']
                    ])
                    from_clause += f"\\nINNER JOIN path_{i} ON {join_condition}"
        
        # Add GROUP BY if there are aggregations
        group_by_clause = ""
        if any('COUNT' in item.upper() or 'SUM' in item.upper() for item in select_items):
            if group_by_items:
                group_by_clause = f"\\nGROUP BY " + ", ".join(group_by_items)
        
        return select_clause + from_clause + group_by_clause
    
    def _parse_properties(self, prop_str: str) -> Dict[str, Any]:
        """Parse property string into dictionary"""
        if not prop_str:
            return {}
        
        props = {}
        for prop in prop_str.split(','):
            if ':' in prop:
                key, value = prop.split(':', 1)
                props[key.strip()] = value.strip().strip("'\"")
        
        return props
    
    def _calculate_complexity(self, pattern_graph: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate query complexity metrics"""
        return {
            'num_nodes': len(pattern_graph['nodes']),
            'num_edges': len(pattern_graph['edges']),
            'num_paths': len(pattern_graph['paths']),
            'max_path_length': max([len(p['edges']) for p in pattern_graph['paths']]) if pattern_graph['paths'] else 0
        }
