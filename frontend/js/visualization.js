class QueryVisualizer {
    constructor() {
        this.nodeCounter = 0;
        this.edgeCounter = 0;
    }
    
    visualizeCypherQuery(parsedQuery) {
        const container = document.getElementById('cypher-visualization');
        const nodes = [];
        const edges = [];
        
        // Add nodes
        if (parsedQuery.match_clauses) {
            parsedQuery.match_clauses.forEach((clause, idx) => {
                clause.nodes.forEach((node, nodeIdx) => {
                    nodes.push({
                        id: `node_${node.name}`,
                        label: `${node.name}\\n:${node.label}`,
                        title: JSON.stringify(node.properties),
                        shape: 'box',
                        color: { background: '#2563eb', border: '#1e40af' },
                        x: nodeIdx * 200,
                        y: idx * 200
                    });
                });
                
                // Add edges
                clause.edges.forEach((edge) => {
                    edges.push({
                        from: `node_${edge.source}`,
                        to: `node_${edge.target}`,
                        label: `:${edge.label}`,
                        title: JSON.stringify(edge.properties),
                        arrows: 'to',
                        color: '#64748b'
                    });
                });
            });
        }
        
        this._renderNetwork(container, nodes, edges);
    }
    
    visualizeSQLQuery(parsedQuery) {
        const container = document.getElementById('sql-visualization');
        const nodes = [];
        const edges = [];
        
        // Add table nodes
        if (parsedQuery.from) {
            const table = parsedQuery.from;
            nodes.push({
                id: `table_${table}`,
                label: table.toUpperCase(),
                shape: 'box',
                color: { background: '#10b981', border: '#059669' }
            });
        }
        
        // Add join nodes
        if (parsedQuery.joins && parsedQuery.joins.length > 0) {
            parsedQuery.joins.forEach((joinTable, idx) => {
                nodes.push({
                    id: `table_${joinTable}`,
                    label: joinTable.toUpperCase(),
                    shape: 'box',
                    color: { background: '#f59e0b', border: '#d97706' }
                });
                
                // Add join edge
                edges.push({
                    from: `table_${parsedQuery.from}`,
                    to: `table_${joinTable}`,
                    label: 'JOIN',
                    arrows: 'both',
                    color: '#64748b'
                });
            });
        }
        
        this._renderNetwork(container, nodes, edges);
    }
    
    _renderNetwork(container, nodes, edges) {
        const data = { nodes: new vis.DataSet(nodes), edges: new vis.DataSet(edges) };
        const options = {
            physics: {
                enabled: true,
                stabilization: { iterations: 200 }
            },
            layout: {
                hierarchical: { direction: 'UD', sortMethod: 'directed' }
            }
        };
        
        new vis.Network(container, data, options);
    }
    
    compareQueries(cypherParsed, sqlParsed) {
        // Create side-by-side comparison visualization
        const comparison = {
            cypher_elements: this._extractElements(cypherParsed),
            sql_elements: this._extractElements(sqlParsed),
            similarities: this._findSimilarities(cypherParsed, sqlParsed),
            differences: this._findDifferences(cypherParsed, sqlParsed)
        };
        
        return comparison;
    }
    
    _extractElements(parsed) {
        if (parsed.match_clauses) {
            // Cypher
            return {
                nodes: parsed.match_clauses.flatMap(c => c.nodes),
                edges: parsed.match_clauses.flatMap(c => c.edges),
                type: 'graph'
            };
        } else {
            // SQL
            return {
                tables: [parsed.from, ...parsed.joins],
                conditions: parsed.where,
                type: 'relational'
            };
        }
    }
    
    _findSimilarities(cypherParsed, sqlParsed) {
        // Find common elements/patterns
        const similarities = [];
        
        // Could implement sophisticated pattern matching here
        
        return similarities;
    }
    
    _findDifferences(cypherParsed, sqlParsed) {
        // Find differences between queries
        const differences = [];
        
        // Analyze structure, filters, aggregations, etc.
        
        return differences;
    }
}

// Export for use
const queryVisualizer = new QueryVisualizer();
