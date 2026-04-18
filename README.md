# batch-http-probe-scheduler

> ⚠️ **本项目仅供个人学习与功能测试使用，严禁用于任何商业、生产或非法用途。**  
> **FOR TESTING PURPOSES ONLY. NOT FOR PRODUCTION USE.**

Asynchronous HTTP metadata probe scheduler with WAL-mode SQLite backend and quota-aware retry logic.

---

## Required GitHub Secrets

| Secret | Description |
|--------|-------------|
| `PROBE_ENDPOINT` | API endpoint URL |
| `DATA_REPO_TOKEN` | PAT with `repo` scope for private data repo |
| `DATA_REPO_SLUG` | Private data repo slug, e.g. `yourname/private-data-store` |

## Usage

Trigger manually via **Actions → Data Processing Pipeline → Run workflow**, or wait for the daily schedule (02:00 UTC).

DB file is stored and versioned in a separate **private** repository. Each run pulls the latest DB, processes new records, and pushes the result back.
