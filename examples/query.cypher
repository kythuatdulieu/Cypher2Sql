MATCH (p:Person)-[w:WORKS_AT]->(c:Company)
WHERE c.cid = 10
RETURN p.pid AS pid, c.cid AS cid
