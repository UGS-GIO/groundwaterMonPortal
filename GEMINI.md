# groundwaterMonPortal — PR review guide
ArcGIS JS 4.21 + jQuery 3.5 / tablesorter / select2 / amCharts front-end (public/index.html) over
server-side PHP data endpoints that query Postgres. Older maintenance app. Review ONLY the changed
lines (general bug/security/quality assumed). Cite file:line; group nits; this app is in
maintenance — prefer minimal, in-style fixes over refactors.

## Match the existing code
- jQuery + Dojo-era ArcGIS-4 client + plain PHP. Match surrounding patterns; no new
  frameworks/build steps or rewrites of working code.

## Security (top priority — public app with server-side code + a DB)
- SQL injection is the #1 risk: any query built from request input (`$_GET`/`$_POST`) MUST use
  parameterized queries (`pg_query_params`), never string concatenation/interpolation. Flag any
  changed query that puts request data into SQL text.
- No DB credentials in committed code: flag any hardcoded host/user/password, and any that a PR
  relocates rather than removes — credentials belong in env / Secret Manager.
- Client-side secrets: no ArcGIS tokens/keys in JS/HTML (a Firebase web `apiKey` is meant to be
  public — don't false-flag it).
- XSS / DOM injection: chart/table/popup HTML is built from query results via `innerHTML` — escape
  any newly rendered DB/feature/URL value.

## Correctness
- Fail loud on failed PHP responses / DB errors — return a real error; don't emit empty or partial
  JSON that silently blanks the grid.
