# Screenshot selection and captions

The original capture folder contains **48 viewport screenshots**. Not every browser tab was captured, and not every capture is suitable for a technical report. Personal chats, mail, research pages and browser error pages are excluded from this report's selected evidence.

Selected originals are stored locally in `private-evidence/screenshots/` and excluded by `.gitignore`. They have **not** been redacted and are intentionally not embedded in these public-facing pages. Use the local private gallery for review; do not publish that gallery.

## Suggested public figures after review

| Priority | Figure | Suggested caption and purpose |
| --- | --- | --- |
| 1 | Spark stages | “Named Spark actions expose aggregate preparation and parallel staging writes.” Show stages 276/278/281 and 288/290/293; do not label the whole operation as driver merge time. |
| 2 | Cube Maker dashboard | “Batch outcomes, processing phases, output counters and persistence indicators.” Explain that status labels do not establish crash-recovery guarantees. |
| 3 | Analytics UI | “Natural-language entry point for bounded network analytics.” Capture an actual sanitized result separately before claiming a successful query. |
| 4 | Swagger | “Documented and bounded traffic, connection, bandwidth and protocol contracts.” Prefer synthetic example addresses and a visible response. |
| 5 | Analytics dashboard | “Operational visibility into analytics execution and dependencies.” Explain the selected time range and metric meanings. |
| 6 | Current storage and nodes | “Storage and node headroom inform database tuning decisions.” This is a snapshot, not a peak-load capacity guarantee. |
| 7 | Prometheus targets | “Scrape discovery and health for platform components.” Remove target addresses and topology details as necessary. |
| 8 | Jenkins CI and SonarQube | “Automated tests and quality analysis.” State the visible build/analysis date and distinguish new-code from overall coverage. |
| 9 | Spark History / HDFS | “Persistent execution history and the batch input layout.” Remove internal paths, application identifiers and network details before sharing. |

## Redaction rules

Remove browser chrome, account/profile icons, unrelated tabs and bookmarks, internal URLs, hostnames, private IPs, credentials, tokens, staff names, job parameters, private repository URLs and sensitive flow records. Browser-address cropping alone is insufficient if the page body also exposes topology or data.

Retain the original image privately. Make a separately reviewed redacted copy, preserve the real result, and avoid edits that could misrepresent a failure as a success. Put approved public images in `assets/`, then embed them with a caption and date.

No screenshots were fabricated, recaptured or altered while assembling this report.
