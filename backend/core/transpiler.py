from typing import Dict, List, Any, Tuple
import re
from core.query_parser import CypherParser, SQLParser

class CypherToSQLTranspiler:
    """Transpiles Cypher queries to SQL queries"""
    
    def __init__(self, induced_schema: Dict[str, Any] = None):
        self.induced_schema = induced_schema or {}
        self.cypher_parser = CypherParser()
        self.cte_counter = 0
        
    def transpile(self, cypher_query: str) -> str:
        """Transpile a Cypher query to SQL"""
        parsed = self.cypher_parser.parse(cypher_query)
        
        # Convert any enum values to strings for JSON serialization
        if 'query_type' in parsed and hasattr(parsed['query_type'], 'value'):
            parsed['query_type'] = parsed['query_type'].value
        
        sql_parts = []
        cte_definitions = []
        
        # Process MATCH clauses
        for i, match in enumerate(parsed['match_clauses']):
            cte_name = f"t{i+1}"
            cte_sql = self._transpile_match(match, cte_name, i)
            cte_definitions.append(cte_sql)
        
        # Build final query
        if cte_definitions:
            with_clause = ",\n".join(cte_definitions)
            sql_parts.append(f"WITH {with_clause}")
        
        # Add SELECT/RETURN
        if parsed['return_clause']:
            select_sql = self._transpile_return(parsed['return_clause'], cte_definitions)
            sql_parts.append(select_sql)
        
        return "\n".join(sql_parts)
    
    def _transpile_match(self, match: Dict[str, Any], cte_name: str, index: int) -> str:
        """Transpile a MATCH clause to SQL CTE"""
        nodes = match['nodes']
        edges = match['edges']
        
        if not nodes:
            return f"{cte_name} AS (SELECT 1 WHERE 0)"
        
        # Start with first node/table
        first_node = nodes[0]
        from_table = first_node['label'].lower() if first_node['label'] else "nodes"
        
        # Build joins for edges
        joins = []
        for i, edge in enumerate(edges):
            edge_table = edge['label'].lower() if edge['label'] else f"edge_{i}"
            # Simplified join condition
            if i < len(nodes) - 1:
                next_node = nodes[i + 1]
                next_table = next_node['label'].lower() if next_node['label'] else "nodes"
                joins.append(f"  JOIN {edge_table} ON {first_node['name']}.id = {edge_table}.src")
                joins.append(f"  JOIN {next_table} AS {next_node['name']} ON {edge_table}.tgt = {next_node['name']}.id")
        
        join_clause = "\n".join(joins)
        
        # Build SELECT items
        select_items = [f"{node['name']}.*" for node in nodes]
        select_clause = ", ".join(select_items)
        
        where_clauses = []
        # Add property filters from nodes
        for node in nodes:
            if node.get('properties'):
                for prop, value in node['properties'].items():
                    # Handle string values with quotes
                    if isinstance(value, str):
                        where_clauses.append(f"{node['name']}.{prop} = '{value}'")
                    else:
                        where_clauses.append(f"{node['name']}.{prop} = {value}")
        
        sql = f"{cte_name} AS (\n"
        sql += f"  SELECT {select_clause}\n"
        sql += f"  FROM {from_table} AS {first_node['name']}\n"
        if join_clause:
            sql += join_clause + "\n"
        if where_clauses:
            sql += f"  WHERE " + " AND ".join(where_clauses) + "\n"
        sql += ")"
        
        return sql
        
    def _transpile_return(self, return_clause: Dict[str, Any], ctes: List[str]) -> str:
        """Transpile RETURN clause to SELECT"""
        items = return_clause['items']
        from_table = "t1"  # Default to first CTE
        
        select_items = []
        group_by_items = []
        
        for item in items:
            if item['type'] == 'aggregation':
                select_items.append(item['expression'])
            else:
                select_items.append(item['expression'])
                # Extract column name for GROUP BY - FIX THIS PART
                col_parts = item['expression'].split('.')
                if len(col_parts) > 1:
                    col = col_parts[-1].split(' ')[0]  # Get the column name
                    if col != '*':
                        group_by_items.append(col)
        
        select_clause = ",\n    ".join(select_items)
        
        sql = f"SELECT {select_clause}\nFROM {from_table}"
        
        if group_by_items:
            group_clause = ", ".join(group_by_items)  # Changed from ",\n    " to ", "
            sql += f"\nGROUP BY {group_clause}"
        
        return sql


class SchemaMapper:
    """Maps between graph schemas and induced relational schemas"""
    
    @staticmethod
    def create_induced_schema(graph_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Create induced relational schema from graph schema"""
        induced = {
            'tables': {},
            'relationships': {}
        }
        
        # Create tables for nodes
        if 'nodes' in graph_schema:
            for node_label, node_def in graph_schema['nodes'].items():
                table_name = node_label.lower()
                induced['tables'][table_name] = {
                    'columns': node_def.get('properties', {}),
                    'primary_key': node_def.get('id_property', 'id'),
                    'type': 'node'
                }
        
        # Create tables for edges
        if 'edges' in graph_schema:
            for edge_label, edge_def in graph_schema['edges'].items():
                table_name = edge_label.lower()
                induced['tables'][table_name] = {
                    'columns': {
                        'src': 'int',
                        'tgt': 'int',
                        **edge_def.get('properties', {})
                    },
                    'primary_key': 'id',
                    'foreign_keys': {
                        'src': edge_def.get('source_node'),
                        'tgt': edge_def.get('target_node')
                    },
                    'type': 'edge'
                }
        
        return induced
