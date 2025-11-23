# backend/tests/test_transpiler.py

import unittest
from core.advanced_transpiler import AdvancedCypherTranspiler
from core.query_parser import CypherParser, SQLParser

class TestCypherToSQLTranspilation(unittest.TestCase):
    
    def setUp(self):
        self.transpiler = AdvancedCypherTranspiler()
        self.cypher_parser = CypherParser()
        self.sql_parser = SQLParser()
    
    def test_simple_match_return(self):
        """Test simple MATCH-RETURN transpilation"""
        cypher = "MATCH (n:Person) RETURN n.name"
        result = self.transpiler.transpile_complex_query(cypher)
        
        self.assertIn('SELECT', result['transpiled_sql'].upper())
        self.assertIn('FROM', result['transpiled_sql'].upper())
    
    def test_match_with_edge(self):
        """Test MATCH with edge transpilation"""
        cypher = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name"
        result = self.transpiler.transpile_complex_query(cypher)
        
        self.assertIn('JOIN', result['transpiled_sql'].upper())
    
    def test_match_with_where(self):
        """Test MATCH with WHERE clause"""
        cypher = "MATCH (n:Person) WHERE n.age > 30 RETURN n.name"
        result = self.transpiler.transpile_complex_query(cypher)
        
        self.assertIn('WHERE', result['transpiled_sql'].upper())
    
    def test_match_with_aggregation(self):
        """Test MATCH with aggregation"""
        cypher = "MATCH (n:Person) RETURN COUNT(*) as count"
        result = self.transpiler.transpile_complex_query(cypher)
        
        self.assertIn('COUNT', result['transpiled_sql'].upper())
        self.assertIn('GROUP BY', result['transpiled_sql'].upper())
    
    def test_multiple_match_clauses(self):
        """Test multiple MATCH clauses"""
        cypher = """
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        WITH a, b
        MATCH (b:Person)-[:WORKS_AT]->(c:Company)
        RETURN a.name, b.name, c.name
        """
        result = self.transpiler.transpile_complex_query(cypher)
        
        self.assertGreaterEqual(result['metadata']['num_ctes'], 2)


class TestEquivalenceVerification(unittest.TestCase):
    
    def setUp(self):
        from core.verifier import EquivalenceVerifier
        self.verifier = EquivalenceVerifier()
    
    def test_equivalent_queries(self):
        """Test that equivalent queries are recognized"""
        cypher = "MATCH (n:Person) RETURN n.id, n.name"
        sql = "SELECT id, name FROM Person"
        
        # These are semantically equivalent
        result = self.verifier.verify(cypher, sql, {})
        self.assertEqual(result.result.value, 'equivalent')
    
    def test_non_equivalent_queries(self):
        """Test that non-equivalent queries are detected"""
        cypher = "MATCH (n:Person) WHERE n.age > 25 RETURN n.name"
        sql = "SELECT name FROM Person WHERE age > 30"
        
        # These are not equivalent (different WHERE conditions)
        result = self.verifier.verify(cypher, sql, {})
        self.assertEqual(result.result.value, 'not_equivalent')


if __name__ == '__main__':
    unittest.main()
