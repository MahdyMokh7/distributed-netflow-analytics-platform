# GitHub publishing checklist

This folder is a report package, not a copy of the application repository. Nothing has been committed or pushed.

## Before publication

- [ ] Confirm permission to publish the project name, employer/team work, architectural details and selected screenshots.
- [ ] Review attribution in `CONTRIBUTIONS.md`; this was a team project.
- [ ] Review every public Markdown file for details your organization considers confidential.
- [ ] Keep `private-evidence/` local. Its contents include internal addresses and possibly operational data.
- [ ] Redact and approve any screenshots separately; only approved copies belong in `assets/`.
- [ ] Do not publish raw server inventories, SSH/kube configuration, session transcripts, mail/chat screenshots, source secrets, or the original project ZIP.
- [ ] Choose a license only after confirming ownership and redistribution permission. No license is assumed by this report.
- [ ] If publishing source too, audit configuration, Git history, deployment examples and secrets independently. This report's sanitization does not sanitize the source repository.
- [ ] Add the approved repository URL and convert source references into commit-pinned links when a suitable public repository exists.
- [ ] Check the final staged diff and screenshot contents before pushing.

`.gitignore` is a convenience, not encryption or a security boundary. It does not remove already tracked files and does not protect manual uploads or ZIP archives. **Do not upload or ZIP the entire local report folder**, because that would include private evidence.

## Suggested publication layout

```text
README.md
ENGINEERING-REPORT.md
CONTRIBUTIONS.md
EVIDENCE.md
SCREENSHOTS.md
PUBLISHING.md
.gitignore
assets/                 # approved, redacted figures only, when ready
```

The existing `private-evidence/` directory is intentionally absent from this publication list. Copy only the listed public files and approved assets into a clean publication location.

## Suggested repository description

> Engineering case study of a team-built network analytics platform: Spark aggregation, PostgreSQL write optimization, guarded natural-language analytics, Kubernetes deployment and observability.

Possible topics: `java`, `spring-boot`, `apache-spark`, `postgresql`, `kafka`, `hdfs`, `starrocks`, `kubernetes`, `helm`, `text-to-sql`, `prometheus`, `grafana`, `data-engineering`.

## Suggested introductory post

> This report documents my contributions to Nimbyte, a team-built network analytics platform. It covers Spark aggregation and PostgreSQL persistence, semantic REST queries, guarded natural-language analytics, and the deployment/testing/observability work around them. I have included implementation evidence, tradeoffs and known recovery limitations rather than presenting architecture diagrams as proof of guarantees.
