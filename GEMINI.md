# groundwaterMonPortal — PR review guide
ArcGIS JS 4.21 + jQuery 3.5 / tablesorter / select2 / amCharts front-end (public/index.html) over
server-side PHP data endpoints (downloadData.php, getFlowData.php) that query Postgres. Older
maintenance app. Review ONLY the changed lines (general bug/security/quality assumed). Cite
file:line; group nits; this app is in maintenance — prefer minimal, in-style fixes over refactors.

## Match the existing code
- jQuery + Dojo-era ArcGIS-4 client + plain PHP. Match surrounding patterns; no new
  frameworks/build steps or rewrites of working code.

## Security (top priority — this public app has server-side code + a DB)
- SQL injection: the PHP endpoints concatenate `$_POST` values straight into SQL — e.g.
  downloadData.php:57/87/194 (`SiteID/OBJECTID/LOCATIONID IN (...)`) and getFlowData.php:52. Any
  changed query touching request input MUST use parameterized queries (pg_query_params), not string
  interpolation. This is the #1 thing to catch.
- NO DB credentials in committed code: the pg_connect() strings hold them inline
  (downloadData.php:18, getFlowData.php:14). Flag any new hardcoded cred/host/password, and any a PR
  relocates rather than removes.
- Client-side secrets: no ArcGIS tokens/keys in JS/HTML (a Firebase web `apiKey` is meant to be
  public — don't false-flag it).
- XSS / DOM injection: chart/table/popup HTML is built from query results via `innerHTML`
  (index.html ~1367+) — escape any newly rendered DB/feature/URL value.

## Correctness
- Fail loud on failed PHP responses / DB errors — return a real error; don't emit empty or partial
  JSON that silently blanks the grid.
