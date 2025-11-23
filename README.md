## Cách chạy CLI

```bash
rm examples/data.db
  sqlite3 examples/data.db < examples/data.sql
  python -m graphiti.cli \
    --schema examples/schema.json \
    --cypher examples/query.cypher \
    --target examples/target.sql \
    --db examples/data.db
```

- `--schema`: JSON gồm danh sách `nodes` và `edges` (label + keys).
- `--cypher`: file chứa truy vấn Cypher.
- `--target` + `--db`: nếu cung cấp cả hai, CLI sẽ so sánh kết quả của SQL sinh ra với SQL đích trên cơ sở dữ liệu SQLite.

## Kiểm thử

Chạy toàn bộ kiểm thử đơn vị:

```bash
python -m unittest discover -s tests -p 'test_*.py'
```
