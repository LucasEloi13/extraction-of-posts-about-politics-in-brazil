# extraction-of-posts-about-politics-in-brazil

Pipeline de extração de posts políticos em Reddit, Bluesky e X.

## Extração com keywords + control plane

1. Defina as variáveis de banco no arquivo `.env`:
   - `POSTGRES_USER`
   - `POSTGRES_PASSWORD`
   - `POSTGRES_DB`
   - `POSTGRES_HOST`
   - `POSTGRES_PORT`
2. Preencha `keywords.txt` com uma keyword por linha.
3. Execute:

```bash
python src/extractor/orchestrator.py --keywords keywords.txt --limit 100 --cookies cookies.json
```

### Idempotência

- Control plane + persistência Postgres: `src/persist/postgres_control_plane.py`.
- Persistência raw no S3: `src/persist/s3_raw_posts.py`.
- O JSON bruto dos posts é salvo no S3 `s3://023546157022-posts/raw/source=.../year=.../month=.../day=...`.
- O control plane persiste jobs e tarefas por `source + keyword` nas tabelas:
  - `extraction_jobs`
  - `extraction_job_tasks`
