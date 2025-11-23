const API_BASE = 'http://localhost:5000/api';

let cypherEditor, sqlEditor;
let verifyCypherEditor, verifySqlEditor;
let compareCypherEditor, compareSqlEditor;
let graphSchemaEditor, sqlOutputEditor;

let cypherNetwork = null;
let sqlNetwork = null;

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    initializeAceEditors();
    setupTabNavigation();
    setupExamples();
});

function initializeAceEditors() {
    // Transpile tab
    cypherEditor = setupEditor('cypher-editor', 'cypher');
    sqlOutputEditor = setupEditor('sql-output', 'sql');
    
    // Verify tab
    verifyCypherEditor = setupEditor('verify-cypher-editor', 'cypher');
    verifySqlEditor = setupEditor('verify-sql-editor', 'sql');
    
    // Compare tab
    compareCypherEditor = setupEditor('compare-cypher-editor', 'cypher');
    compareSqlEditor = setupEditor('compare-sql-editor', 'sql');
    
    // Schema tab
    graphSchemaEditor = setupEditor('graph-schema-editor', 'json');

    window.editors = {
        cypher: cypherEditor,
        sql: sqlOutputEditor,
        verifyCypher: verifyCypherEditor,
        verifySql: verifySqlEditor,
        compareCypher: compareCypherEditor,
        compareSql: compareSqlEditor,
        graphSchema: graphSchemaEditor
    };
}

function setupEditor(elementId, language) {
    const editor = ace.edit(elementId);
    editor.setTheme('ace/theme/material-darker');
    editor.session.setMode(`ace/mode/${language}`);
    editor.setOptions({
        fontSize: 13,
        enableBasicAutocompletion: true,
        enableLiveAutocompletion: true,
        enableSnippets: true,
        showPrintMargin: false,
        wrap: true
    });
    return editor;
}

async function compareQueries() {
    const cypherQuery = editors.compareCypher.getValue().trim();
    const sqlQuery = editors.compareSql.getValue().trim();
    const resultBox = document.getElementById('comparison-tab') || document.getElementById('comparison-result');

    if (!cypherQuery || !sqlQuery) {
        showError('comparison-tab', 'Please enter both Cypher and SQL queries');
        return;
    }

    try {
        showSuccess('comparison-tab', 'Parsing queries...');

        const [cypherResponse, sqlResponse] = await Promise.all([
            fetch(`${API_BASE}/parse/cypher`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ query: cypherQuery })
            }),
            fetch(`${API_BASE}/parse/sql`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ query: sqlQuery })
            })
        ]);

        const cypherData = await cypherResponse.json();
        const sqlData = await sqlResponse.json();

        if (!cypherData.success) {
            showError('comparison-tab', `Cypher parse error: ${cypherData.error}`);
            return;
        }
        if (!sqlData.success) {
            showError('comparison-tab', `SQL parse error: ${sqlData.error}`);
            return;
        }

        // Visualize both queries
        visualizeParsedQueries(cypherData.parsed, sqlData.parsed);

        // Compute similarity summary
        const summary = computeQuerySimilaritySummary(cypherData.parsed, sqlData.parsed);

        showSuccess('comparison-tab', `Comparison complete: ${summary}`);

    } catch (error) {
        showError('comparison-tab', `Error during comparison: ${error.message}`);
    }
}

function visualizeParsedQueries(cypherParsed, sqlParsed) {
    const cypherContainer = document.getElementById('cypher-visualization');
    const sqlContainer = document.getElementById('sql-visualization');
    console.log('cypherContainer:', cypherContainer, 'sqlContainer:', sqlContainer);
    if (!cypherContainer || !sqlContainer) {
        showError('comparison-tab', 'Visualization containers not found in the DOM.');
        return;
    }

    // Destroy previous networks if they exist
    if (cypherNetwork) {
        cypherNetwork.destroy();
        cypherNetwork = null;
    }
    if (sqlNetwork) {
        sqlNetwork.destroy();
        sqlNetwork = null;
    }

    // Prepare Cypher nodes and edges
    const cypherNodes = [];
    const cypherEdges = [];
    (cypherParsed.match_clauses || []).forEach(clause => {
        clause.nodes.forEach(node => {
            cypherNodes.push({
                id: node.name,
                label: `${node.name}: ${node.label}`,
                shape: 'box',
                color: '#2563eb'
            });
        });
        clause.edges.forEach(edge => {
            cypherEdges.push({
                from: edge.source || clause.nodes[0].name,
                to: edge.target || clause.nodes[1]?.name,
                label: `:${edge.label}`,
                arrows: 'to',
                color: '#64748b'
            });
        });
    });

    // Prepare SQL nodes and edges
    const sqlNodes = [];
    const sqlEdges = [];

    if (sqlParsed.from && typeof sqlParsed.from === 'string') {
        sqlNodes.push({
            id: sqlParsed.from,
            label: sqlParsed.from.toUpperCase(),
            shape: 'ellipse',
            color: '#10b981'
        });
        
        (sqlParsed.joins || []).forEach(joinTbl => {
            if (typeof joinTbl === 'string') {
                sqlNodes.push({
                    id: joinTbl,
                    label: joinTbl.toUpperCase(),
                    shape: 'ellipse',
                    color: '#f59e0b'
                });
                sqlEdges.push({
                    from: sqlParsed.from,
                    to: joinTbl,
                    label: 'JOIN',
                    arrows: 'to',
                    color: '#64748b'
                });
            }
        });
    }
    (sqlParsed.joins || []).forEach(joinTbl => {
        sqlNodes.push({
            id: joinTbl,
            label: joinTbl.toUpperCase(),
            shape: 'ellipse',
            color: '#f59e0b'
        });
        sqlEdges.push({
            from: sqlParsed.from,
            to: joinTbl,
            label: 'JOIN',
            arrows: 'to',
            color: '#64748b'
        });
    });

    const options = {
        physics: false, 
        layout: { improvedLayout: true }, 
        interaction: { hover: true },
        autoResize: true,
        height: '100%',
        width: '100%'
    };

    // Create vis network instances
    cypherNetwork = new vis.Network(cypherContainer, {
        nodes: new vis.DataSet(cypherNodes),
        edges: new vis.DataSet(cypherEdges)
    }, options);

    sqlNetwork = new vis.Network(sqlContainer, {
        nodes: new vis.DataSet(sqlNodes),
        edges: new vis.DataSet(sqlEdges)
    }, options);

    // Optionally, force redraw after a short delay to ensure layout
    setTimeout(() => {
        cypherNetwork.redraw();
        sqlNetwork.redraw();
    }, 100);
}


function computeQuerySimilaritySummary(cypherParsed, sqlParsed) {
    // Extract projected columns from Cypher RETURN clause
    const cypherProjs = new Set();
    (cypherParsed.return_clause?.items || []).forEach(item => {
        if (item.type === 'projection') cypherProjs.add(item.expression.toLowerCase());
    });

    // Extract projected columns from SQL SELECT clause (split by comma)
    const sqlProjs = new Set();
    if (sqlParsed.select) {
        sqlParsed.select.toLowerCase().split(',').forEach(col => sqlProjs.add(col.trim()));
    }

    // Calculate overlap in projections
    let commonCount = 0;
    cypherProjs.forEach(p => { if (sqlProjs.has(p)) commonCount++; });
    const total = Math.max(cypherProjs.size, sqlProjs.size, 1);
    const projectionSim = Math.round((commonCount / total) * 100);

    // Compare number of edges (joins)
    const cypherEdgesCount = (cypherParsed.match_clauses || []).reduce((acc, c) => acc + (c.edges?.length || 0), 0);
    const sqlJoinsCount = sqlParsed.joins?.length || 0;

    const joinSim = Math.round(Math.min(cypherEdgesCount, sqlJoinsCount) / Math.max(cypherEdgesCount, sqlJoinsCount, 1) * 100);

    return `Projections similarity: ${projectionSim}%, Joins similarity: ${joinSim}%`;
}


// function visualizeParsedQueries(cypherParsed, sqlParsed) {
//     // Use Vis.js or similar to display graph structure of Cypher and SQL tables/joins side by side
//     // For example, show nodes and edges from parsedCypher.match_clauses
//     // and from parsedSql.from + parsedSql.joins.

//     // Here you would clear and redraw vis networks in #cypher-visualization and #sql-visualization divs
//     // Full visualization code depends on your existing vis.js setup
// }


document.addEventListener('DOMContentLoaded', () => {
    // Apply saved theme or default
    const savedTheme = localStorage.getItem('theme') || 'dark';
    setTheme(savedTheme);

    // Setup theme toggle button
    const themeToggleBtn = document.getElementById('theme-toggle');
    themeToggleBtn.addEventListener('click', () => {
        const current = document.documentElement.getAttribute('data-theme');
        const nextTheme = current === 'dark' ? 'light' : 'dark';
        setTheme(nextTheme);
        localStorage.setItem('theme', nextTheme);
    });

    // Rest of your initialization code ...
});

function setTheme(theme) {
    document.documentElement.setAttribute('data-theme', theme);
    const themeToggleBtn = document.getElementById('theme-toggle');
    if (!themeToggleBtn) return;
    if (theme === 'dark') {
        themeToggleBtn.textContent = '🌙'; // Moon icon
    } else {
        themeToggleBtn.textContent = '☀️'; // Sun icon
    }
}

function analyzeQuerySimilarity(cypherParsed, sqlParsed) {
    // Simple heuristic: compare projected columns and join counts
    const cypherProjections = new Set();
    (cypherParsed.return_clause?.items || []).forEach(item => {
        if(item.type === 'projection')
            cypherProjections.add(item.expression.toLowerCase());
    });

    const sqlProjections = new Set();
    if(sqlParsed.select){
        sqlParsed.select.toLowerCase().split(',').forEach(s => sqlProjections.add(s.trim()));
    }

    let commonProjectionsCount = 0;
    cypherProjections.forEach(p => {
        if(sqlProjections.has(p)){
            commonProjectionsCount++;
        }
    });

    const totalProjections = Math.max(cypherProjections.size, sqlProjections.size);

    const similarityPercent = totalProjections === 0 ? 100 : Math.round((commonProjectionsCount/totalProjections)*100);

    // Compare number of joins/edges as rough structural similarity
    const cypherJoinsCount = cypherParsed.match_clauses?.reduce((acc, m) => acc+(m.edges?.length||0), 0) || 0;
    const sqlJoinsCount = sqlParsed.joins?.length || 0;

    const joinSimilarity = Math.min(cypherJoinsCount, sqlJoinsCount)/Math.max(1, Math.max(cypherJoinsCount, sqlJoinsCount));

    const structuralSimilarityPercent = Math.round(joinSimilarity*100);

    return `Projection similarity: ${similarityPercent}%, Join similarity: ${structuralSimilarityPercent}%`;
}


function setupTabNavigation() {
    const tabButtons = document.querySelectorAll('.tab-button');
    const tabContents = document.querySelectorAll('.tab-content');
    
    tabButtons.forEach(button => {
        button.addEventListener('click', () => {
            const tabName = button.getAttribute('data-tab');
            
            // Hide all tabs
            tabContents.forEach(tab => tab.classList.remove('active'));
            tabButtons.forEach(btn => btn.classList.remove('active'));
            
            // Show selected tab
            document.getElementById(tabName + '-tab').classList.add('active');
            button.classList.add('active');
            
            // Resize editors for the visible tab
            setTimeout(() => {
                if (tabName === 'transpile') {
                    cypherEditor.resize();
                    sqlOutputEditor.resize();
                } else if (tabName === 'verify') {
                    verifyCypherEditor.resize();
                    verifySqlEditor.resize();
                } else if (tabName === 'comparison') {
                    compareCypherEditor.resize();
                    compareSqlEditor.resize();
                }
            }, 100);
        });
    });
}

function setupExamples() {
    // Add example Cypher queries
    const exampleCypher = `MATCH (e:Employee)-[:WORKS_AT]->(d:Department)
RETURN d.name AS department, COUNT(e.id) AS employee_count
ORDER BY employee_count DESC`;
    
    const exampleSQL = `SELECT d.name AS department, COUNT(e.id) AS employee_count
FROM Department d
LEFT JOIN Employee e ON e.dept_id = d.id
GROUP BY d.name
ORDER BY employee_count DESC`;
    
    cypherEditor.setValue(exampleCypher);
    verifySqlEditor.setValue(exampleSQL);
}

async function transpileCypher() {
    const cypherQuery = cypherEditor.getValue().trim();
    
    if (!cypherQuery) {
        showError('Please enter a Cypher query');
        return;
    }
    
    const resultBox = document.getElementById('transpile-result');
    
    try {
        const response = await fetch(`${API_BASE}/transpile`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                cypher_query: cypherQuery,
                schema: {}
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            sqlOutputEditor.setValue(data.transpiled_sql);
            showSuccess(resultBox, `✓ Transpiled successfully in ${data.timestamp}`);
        } else {
            showError(resultBox, `Error: ${data.error}`);
        }
    } catch (error) {
        showError(resultBox, `Network error: ${error.message}`);
    }
}

async function verifyEquivalence() {
    const cypherQuery = verifyCypherEditor.getValue().trim();
    const sqlQuery = verifySqlEditor.getValue().trim();
    const backend = document.getElementById('backend').value;
    const timeout = parseInt(document.getElementById('timeout').value);
    
    if (!cypherQuery || !sqlQuery) {
        showError('Please enter both Cypher and SQL queries');
        return;
    }
    
    const resultBox = document.getElementById('verify-result');
    
    try {
        const response = await fetch(`${API_BASE}/verify`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                cypher_query: cypherQuery,
                sql_query: sqlQuery,
                schema: {},
                backend: backend,
                timeout: timeout
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            const resultHTML = buildVerificationResult(data);
            resultBox.innerHTML = resultHTML;
            resultBox.classList.add('show');
            
            if (data.result === 'equivalent') {
                resultBox.classList.add('success');
            } else {
                resultBox.classList.add('error');
            }
        } else {
            showError(resultBox, `Error: ${data.error}`);
        }
    } catch (error) {
        showError(resultBox, `Network error: ${error.message}`);
    }
}

function buildVerificationResult(data) {
    const resultEmoji = data.result === 'equivalent' ? '✓' : '✗';
    const resultText = data.result === 'equivalent' ? 'EQUIVALENT' : 'NOT EQUIVALENT';
    const resultColor = data.result === 'equivalent' ? 'success-text' : 'error-text';
    
    let html = `
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <div>
                <h3 class="${resultColor}">${resultEmoji} Queries are ${resultText}</h3>
            </div>
            <div class="info-text">
                Time: ${data.time_ms.toFixed(2)}ms
            </div>
        </div>
    `;
    
    if (data.checked_bound) {
        html += `<p>Checked bound: ${data.checked_bound}</p>`;
    }
    
    if (data.counterexample) {
        html += `<details><summary>Counterexample</summary><pre><code>${escapeHtml(data.counterexample)}</code></pre></details>`;
    }
    
    if (data.details) {
        html += `<p>Method: ${data.details.method} | Backend: ${data.details.backend}</p>`;
    }
    
    return html;
}

async function generateInducedSchema() {
    const schemaText = graphSchemaEditor.getValue().trim();
    
    if (!schemaText) {
        showError('Please enter a graph schema');
        return;
    }
    
    try {
        const graphSchema = JSON.parse(schemaText);
        
        const response = await fetch(`${API_BASE}/schema/create-induced`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ graph_schema: graphSchema })
        });
        
        const data = await response.json();
        
        if (data.success) {
            const display = document.getElementById('induced-schema-display');
            display.innerHTML = `<pre><code>${JSON.stringify(data.induced_schema, null, 2)}</code></pre>`;
            showSuccess('Induced schema generated successfully');
        } else {
            showError(`Error: ${data.error}`);
        }
    } catch (error) {
        showError(`Error: ${error.message}`);
    }
}

function showSuccess(targetOrMessage, message) {
    if (typeof message === 'undefined') {
        // Only message provided
        console.log('✓', targetOrMessage);
    } else {
        // Target element and message provided
        const el = typeof targetOrMessage === 'string' 
            ? document.getElementById(targetOrMessage) 
            : targetOrMessage;
        
        if (el) {
            el.innerHTML = `<div class="success-text">${message}</div>`;
            el.classList.add('show', 'success');
            el.classList.remove('error');
        }
        console.log('✓', message);
    }
}

function showError(targetOrMessage, message) {
    if (typeof message === 'undefined') {
        // Only message provided
        console.error('✗', targetOrMessage);
    } else {
        // Target element and message provided
        const el = typeof targetOrMessage === 'string' 
            ? document.getElementById(targetOrMessage) 
            : targetOrMessage;
        
        if (el) {
            el.innerHTML = `<div class="error-text">${message}</div>`;
            el.classList.add('show', 'error');
            el.classList.remove('success');
        }
        console.error('✗', message);
    }
}

function copySQLToClipboard() {
    const sql = sqlOutputEditor.getValue();
    navigator.clipboard.writeText(sql).then(() => {
        showSuccess('SQL copied to clipboard');
    }).catch(err => {
        showError('Failed to copy: ' + err);
    });
}

function escapeHtml(text) {
    const map = {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#039;'
    };
    return text.replace(/[&<>"']/g, m => map[m]);
}
