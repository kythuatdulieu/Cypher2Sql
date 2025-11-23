import enum
from flask import Flask, request, jsonify
from flask_cors import CORS
import json
import traceback
import time

# Import all core modules
from core.query_parser import CypherParser, SQLParser
from core.transpiler import CypherToSQLTranspiler, SchemaMapper
from core.verifier import EquivalenceVerifier

# Initialize Flask app
app = Flask(__name__)
CORS(app, resources={
    r"/api/*": {
        "origins": "*",
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type"]
    }
})  # Enable CORS for frontend

# Initialize parsers globally
cypher_parser = CypherParser()
sql_parser = SQLParser()
schema_mapper = SchemaMapper()


def serialize_enums(obj):
    if isinstance(obj, enum.Enum):
        return obj.value
    elif isinstance(obj, dict):
        return {k: serialize_enums(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [serialize_enums(i) for i in obj]
    else:
        return obj

@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'ok',
        'version': '1.0.0',
        'message': 'Graphiti backend is running'
    })

@app.route('/api/transpile', methods=['POST'])
def transpile_cypher():
    """Transpile Cypher to SQL"""
    try:
        data = request.json
        
        if not data:
            return jsonify({
                'success': False,
                'error': 'Request body is empty'
            }), 400
        
        cypher_query = data.get('cypher_query', '').strip()
        schema = data.get('schema', {})
        
        if not cypher_query:
            return jsonify({
                'success': False,
                'error': 'No Cypher query provided'
            }), 400
        
        # Transpile query
        transpiler = CypherToSQLTranspiler(schema)
        sql_result = transpiler.transpile(cypher_query)
        
        # Parse for visualization
        parsed_cypher = serialize_enums(cypher_parser.parse(cypher_query))
        
        return jsonify({
            'success': True,
            'original_cypher': cypher_query,
            'transpiled_sql': str(sql_result),
            'parsed_cypher': parsed_cypher,
            'timestamp': time.time()
        })
    
    except Exception as e:
        print(f"Error in transpile: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500

@app.route('/api/verify', methods=['POST'])
def verify_equivalence():
    """Verify equivalence between Cypher and SQL queries"""
    try:
        data = request.json
        
        if not data:
            return jsonify({'success': False, 'error': 'Request body is empty'}), 400
        
        cypher_query = data.get('cypher_query', '').strip()
        sql_query = data.get('sql_query', '').strip()
        schema = data.get('schema', {})
        backend = data.get('backend', 'verieql')
        timeout = data.get('timeout', 600)
        
        if not cypher_query or not sql_query:
            return jsonify({
                'success': False,
                'error': 'Both Cypher and SQL queries are required'
            }), 400
        
        verifier = EquivalenceVerifier(backend=backend)
        report = verifier.verify(cypher_query, sql_query, schema, timeout)
        
        return jsonify({
            'success': True,
            'result': report.result.value,
            'time_ms': report.time_ms,
            'checked_bound': report.checked_bound,
            'counterexample': report.counterexample,
            'details': report.details,
            'timestamp': time.time()
        })
    
    except Exception as e:
        print(f"Error in verify: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500

@app.route('/api/parse/cypher', methods=['POST'])
def parse_cypher():
    """Parse Cypher query"""
    try:
        data = request.json
        
        if not data:
            return jsonify({'success': False, 'error': 'Request body is empty'}), 400
        
        query = data.get('query', '').strip()
        
        if not query:
            return jsonify({'success': False, 'error': 'No query provided'}), 400
        
        parsed = cypher_parser.parse(query)
        parsed = serialize_enums(parsed)
        
        return jsonify({
            'success': True,
            'parsed': parsed
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/parse/sql', methods=['POST'])
def parse_sql():
    """Parse SQL query"""
    try:
        data = request.json
        
        if not data:
            return jsonify({'success': False, 'error': 'Request body is empty'}), 400
        
        query = data.get('query', '').strip()
        
        if not query:
            return jsonify({'success': False, 'error': 'No query provided'}), 400
        
        parsed = sql_parser.parse(query)
        parsed = serialize_enums(parsed)
        
        return jsonify({
            'success': True,
            'parsed': parsed
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/schema/create-induced', methods=['POST'])
def create_induced_schema():
    """Create induced relational schema from graph schema"""
    try:
        data = request.json
        
        if not data:
            return jsonify({'success': False, 'error': 'Request body is empty'}), 400
        
        graph_schema = data.get('graph_schema', {})
        
        if not graph_schema:
            return jsonify({
                'success': False,
                'error': 'Graph schema is required'
            }), 400
        
        induced = schema_mapper.create_induced_schema(graph_schema)
        
        return jsonify({
            'success': True,
            'induced_schema': induced
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.errorhandler(404)
def not_found(e):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def server_error(e):
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    print("Starting Graphiti backend...")
    print("Ensure CORS is enabled for http://localhost:8000")
    app.run(debug=True, port=5000, host='0.0.0.0')
