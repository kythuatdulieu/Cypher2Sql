import re
from enum import Enum
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple

class QueryType(Enum):
    CYPHER = "cypher"
    SQL = "sql"

class ElementType(Enum):
    NODE = "node"
    EDGE = "edge"
    ATTRIBUTE = "attribute"

@dataclass
class Node:
    """Represents a Cypher node pattern"""
    name: str
    label: str
    properties: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.properties is None:
            self.properties = {}

@dataclass
class Edge:
    """Represents a Cypher edge pattern"""
    name: str
    label: str
    direction: str  # '->', '<-', or '<->'
    source: str
    target: str
    properties: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.properties is None:
            self.properties = {}

@dataclass
class PathPattern:
    """Represents a complete path pattern in Cypher"""
    nodes: List[Node]
    edges: List[Edge]

class CypherParser:
    """Parses Cypher queries into structured format"""
    
    def __init__(self):
        self.node_pattern = r'\((\w+)(?::(\w+))?\s*(?:\{([^}]+)\})?\)'
        self.edge_pattern = r'-\[(\w+)?(?::(\w+))?(?:\{([^}]+)\})?\]-([>|<]*)-'
        self.match_clause = r'MATCH\s+(.+?)\s+(?:WHERE|WITH|RETURN)'
        self.return_clause = r'RETURN\s+(.+?)(?:\s+(?:ORDER\s+BY|LIMIT|$))'
        self.where_clause = r'WHERE\s+(.+?)(?:\s+(?:WITH|RETURN|ORDER\s+BY|$))'
        
    def parse(self, query: str) -> Dict[str, Any]:
        """Parse a Cypher query"""
        query = query.strip()
        result = {
            'type': QueryType.CYPHER,
            'original': query,
            'match_clauses': [],
            'where': None,
            'return_clause': None,
            'order_by': None,
            'with_clause': None
        }
        
        # Extract MATCH clauses
        match_clauses = re.findall(r'MATCH\s+([^W\n]+?)(?=\s+(?:WHERE|WITH|RETURN))', 
                                   query, re.IGNORECASE)
        for match in match_clauses:
            result['match_clauses'].append(self._parse_match(match))
        
        # Extract WHERE clause
        where_match = re.search(self.where_clause, query, re.IGNORECASE)
        if where_match:
            result['where'] = where_match.group(1).strip()
        
        # Extract RETURN clause
        return_match = re.search(self.return_clause, query, re.IGNORECASE)
        if return_match:
            result['return_clause'] = self._parse_return(return_match.group(1))
        
        # Extract ORDER BY
        order_match = re.search(r'ORDER\s+BY\s+(.+?)(?:\s+(?:LIMIT|$))', query, re.IGNORECASE)
        if order_match:
            result['order_by'] = order_match.group(1).strip()
        
        # Extract WITH clause
        with_match = re.search(r'WITH\s+(.+?)(?=\s+(?:MATCH|RETURN))', query, re.IGNORECASE)
        if with_match:
            result['with_clause'] = with_match.group(1).strip()
        
        return result
    
    def _parse_match(self, match_str: str) -> Dict[str, Any]:
        """Parse a single MATCH clause"""
        nodes = []
        edges = []
        
        # Find all nodes
        for node_match in re.finditer(self.node_pattern, match_str):
            name = node_match.group(1)
            label = node_match.group(2) or ""
            props = node_match.group(3) or ""
            nodes.append({
                'name': name,
                'label': label,
                'properties': self._parse_properties(props)
            })
        
        # Find all edges
        for edge_match in re.finditer(self.edge_pattern, match_str):
            name = edge_match.group(1) or f"e{len(edges)}"
            label = edge_match.group(2) or ""
            props = edge_match.group(3) or ""
            direction = edge_match.group(4)
            
            edges.append({
                'name': name,
                'label': label,
                'direction': direction if direction else '->',
                'properties': self._parse_properties(props)
            })
        
        return {
            'nodes': nodes,
            'edges': edges,
            'raw': match_str
        }
    
    def _parse_return(self, return_str: str) -> Dict[str, Any]:
        """Parse RETURN clause"""
        items = [item.strip() for item in return_str.split(',')]
        return_items = []
        
        for item in items:
            # Check for aggregation functions
            if 'COUNT' in item.upper():
                return_items.append({'type': 'aggregation', 'function': 'COUNT', 'expression': item})
            elif 'SUM' in item.upper():
                return_items.append({'type': 'aggregation', 'function': 'SUM', 'expression': item})
            elif 'AVG' in item.upper():
                return_items.append({'type': 'aggregation', 'function': 'AVG', 'expression': item})
            elif 'MAX' in item.upper():
                return_items.append({'type': 'aggregation', 'function': 'MAX', 'expression': item})
            elif 'MIN' in item.upper():
                return_items.append({'type': 'aggregation', 'function': 'MIN', 'expression': item})
            else:
                return_items.append({'type': 'projection', 'expression': item})
        
        return {'items': return_items, 'raw': return_str}
    
    def _parse_properties(self, prop_str: str) -> Dict[str, str]:
        """Parse property key-value pairs"""
        if not prop_str:
            return {}
        
        properties = {}
        for prop in prop_str.split(','):
            if ':' in prop:
                key, value = prop.split(':', 1)
                properties[key.strip()] = value.strip()
        
        return properties


class SQLParser:
    """Parses SQL queries into structured format"""
    
    def __init__(self):
        self.select_pattern = r'SELECT\s+(.+?)\s+FROM'
        self.from_pattern = r'FROM\s+(.+?)(?:\s+(?:WHERE|GROUP|ORDER|$))'
        self.join_pattern = r'(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+)?JOIN\s+(\w+)'
        self.where_pattern = r'WHERE\s+(.+?)(?:\s+(?:GROUP|ORDER|$))'
        self.group_pattern = r'GROUP\s+BY\s+(.+?)(?:\s+(?:HAVING|ORDER|$))'
        
    def parse(self, query: str) -> Dict[str, Any]:
        """Parse a SQL query"""
        query = query.strip()
        result = {
            'type': QueryType.SQL,
            'original': query,
            'select': None,
            'from': None,
            'joins': [],
            'where': None,
            'group_by': None,
            'having': None,
            'order_by': None
        }
        
        # Extract SELECT clause
        select_match = re.search(self.select_pattern, query, re.IGNORECASE)
        if select_match:
            result['select'] = select_match.group(1).strip()
        
        # Extract FROM clause
        from_match = re.search(self.from_pattern, query, re.IGNORECASE)
        if from_match:
            from_part = from_match.group(1).strip()
            result['from'] = from_part.split()
        
        # Extract JOINs
        for join_match in re.finditer(self.join_pattern, query, re.IGNORECASE):
            result['joins'].append(join_match.group(1))
        
        # Extract WHERE clause
        where_match = re.search(self.where_pattern, query, re.IGNORECASE)
        if where_match:
            result['where'] = where_match.group(1).strip()
        
        # Extract GROUP BY
        group_match = re.search(self.group_pattern, query, re.IGNORECASE)
        if group_match:
            result['group_by'] = group_match.group(1).strip()
        
        # Extract ORDER BY
        order_match = re.search(r'ORDER\s+BY\s+(.+?)(?:\s+(?:LIMIT|$))', query, re.IGNORECASE)
        if order_match:
            result['order_by'] = order_match.group(1).strip()
        
        return result
