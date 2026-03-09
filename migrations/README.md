# Migrations

These are **upgrade-only** scripts for existing installs moving between versions.

**Fresh installs do not need these.** The database is created automatically from
the current models on first boot via `db.create_all()`.

## Running a migration on an existing install

```bash
docker exec fastchannels python /app/migrations/<script>.py
```

Each script is safe to re-run — it checks before altering anything.
