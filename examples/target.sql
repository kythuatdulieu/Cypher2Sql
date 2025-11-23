SELECT p.pid AS pid, c.cid AS cid
FROM person AS p
INNER JOIN works_at AS w ON p.pid = w.SRC
INNER JOIN company AS c ON w.TGT = c.cid
WHERE c.cid = 10
