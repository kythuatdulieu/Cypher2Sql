# backend/core/transformer_engine.py

from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass
import re

@dataclass
class TransformationRule:
    """Represents a database transformation rule"""
    source_predicates: List[str]
    target_predicate: str
    condition: Optional[str] = None
    
class DatabaseTransformer:
    """Manages graph-to-relational database transformations"""
    
    def __init__(self):
        self.rules: List[TransformationRule] = []
        self.inverse_rules: List[TransformationRule] = []
        
    def create_standard_transformer(self, graph_schema: Dict[str, Any]) -> 'DatabaseTransformer':
        """Create Standard Database Transformer (SDT) from graph schema"""
        
        transformer = DatabaseTransformer()
        
        # Transform nodes
        if 'nodes' in graph_schema:
            for node_label, node_def in graph_schema['nodes'].items():
                rule = self._create_node_transformation_rule(node_label, node_def)
                transformer.rules.append(rule)
        
        # Transform edges
        if 'edges' in graph_schema:
            for edge_label, edge_def in graph_schema['edges'].items():
                rule = self._create_edge_transformation_rule(edge_label, edge_def)
                transformer.rules.append(rule)
        
        return transformer
    
    def _create_node_transformation_rule(self, label: str, definition: Dict[str, Any]) -> TransformationRule:
        """Create transformation rule for a node type"""
        
        properties = definition.get('properties', {})
        prop_list = ', '.join([f"{key}" for key in properties.keys()])
        
        source_pred = f"{label}({prop_list})"
        target_pred = f"R_{label}({prop_list})"
        
        return TransformationRule(
            source_predicates=[source_pred],
            target_predicate=target_pred
        )
    
    def _create_edge_transformation_rule(self, label: str, definition: Dict[str, Any]) -> TransformationRule:
        """Create transformation rule for an edge type"""
        
        source_node = definition.get('source_node', 'Node')
        target_node = definition.get('target_node', 'Node')
        properties = definition.get('properties', {})
        prop_list = ', '.join([f"{key}" for key in properties.keys()])
        
        source_pred = f"{label}({source_node}, {target_node}, {prop_list})"
        target_pred = f"R_{label}({source_node}, {target_node}, {prop_list})"
        
        return TransformationRule(
            source_predicates=[source_pred],
            target_predicate=target_pred
        )
    
    def transform_graph_to_relational(self, graph_instance: Dict[str, Any]) -> Dict[str, Any]:
        """Apply transformation to convert graph instance to relational"""
        
        relational_instance = {
            'tables': {},
            'constraints': {}
        }
        
        for rule in self.rules:
            # Apply each transformation rule
            transformed = self._apply_rule(rule, graph_instance)
            relational_instance['tables'].update(transformed)
        
        return relational_instance
    
    def _apply_rule(self, rule: TransformationRule, graph_instance: Dict[str, Any]) -> Dict[str, Any]:
        """Apply a single transformation rule"""
        
        # Extract data from graph matching source predicates
        result = {}
        
        # This would involve matching patterns and transforming data
        # Implementation depends on actual graph instance format
        
        return result
    
    def compose_transformers(self, transformer1: 'DatabaseTransformer',
                           transformer2: 'DatabaseTransformer') -> 'DatabaseTransformer':
        """Compose two transformers (S -> R and R -> R') into S -> R'"""
        
        composed = DatabaseTransformer()
        
        # For each rule in transformer1 (S -> R)
        for rule1 in transformer1.rules:
            # Find matching rules in transformer2
            for rule2 in transformer2.rules:
                if self._rules_compatible(rule1, rule2):
                    # Create composed rule
                    composed_rule = self._compose_rules(rule1, rule2)
                    composed.rules.append(composed_rule)
        
        return composed
    
    def _rules_compatible(self, rule1: TransformationRule, rule2: TransformationRule) -> bool:
        """Check if two rules can be composed"""
        # Rules are compatible if output of rule1 matches input of rule2
        return rule1.target_predicate == rule2.source_predicates
    
    def _compose_rules(self, rule1: TransformationRule, rule2: TransformationRule) -> TransformationRule:
        """Compose two transformation rules"""
        
        return TransformationRule(
            source_predicates=rule1.source_predicates,
            target_predicate=rule2.target_predicate,
            condition=self._combine_conditions(rule1.condition, rule2.condition)
        )
    
    def _combine_conditions(self, cond1: Optional[str], cond2: Optional[str]) -> Optional[str]:
        """Combine conditions from two rules"""
        
        if not cond1 and not cond2:
            return None
        elif cond1 and not cond2:
            return cond1
        elif not cond1 and cond2:
            return cond2
        else:
            return f"({cond1}) AND ({cond2})"
