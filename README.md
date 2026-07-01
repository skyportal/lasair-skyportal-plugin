# lasair-skyportal-plugin

A SkyPortal external micro-service that ingests **LSST objects from the
[Lasair](https://lasair.lsst.ac.uk/) broker** into a SkyPortal instance.

## How it's wired

SkyPortal's external-service framework clones this repo and runs `main.py` as a
supervised service when the block below is present in the SkyPortal config under
`services.external.lasair` (see `config.yaml.defaults`). Unlike a streaming
broker, Lasair is **poll-based**: each cycle the plugin runs one or more SQL
queries against the Lasair API, fetches each returned object's lightcurve +
cutouts, writes to the DB via SkyPortal's models + `add_external_photometry`,
then sleeps `poll_interval` seconds and repeats.

It writes **directly to the SkyPortal database** (same approach as the
[babamul plugin](https://github.com/skyportal/babamul-skyportal-plugin)), with
batched object creation and one aggregated photometry post per chunk.

## What it ingests

Per object returned by a Lasair query the plugin:

- creates/uses the **Obj** (`diaObjectId`, `ra`/`decl`);
- ingests **LSST photometry** from the object's `diaSourcesList`
  (`midpointMjdTai` → MJD, nJy `psfFlux`/`psfFluxErr` → SkyPortal flux with
  `zp = 31.4`, `band` → `lsst{u,g,r,i,z,y}`; rows without `psfFluxErr` skipped,
  exact-duplicate points deduped to satisfy SkyPortal's upsert key);
- registers it as a **Candidate** under each configured `filter_ids` (this is
  how it becomes scannable — leave empty to ingest Objs + photometry only);
- stores the extra query columns (e.g. TNS name/type, Sherlock classification)
  as an **Annotation** (`origin: lasair`);
- downloads the Science/Template/Difference FITS cutouts from
  `lasairData.imageUrls` and posts **new/ref/sub thumbnails** (off the DB hot
  path via a thread pool);
- for each **new** object, generates SkyPortal's external survey cutouts
  (**SDSS DR18**, **Legacy Survey DR10**, **PS1**) via `Obj.add_linked_thumbnails`
  — set by `ingest.survey_thumbnails` (default `["sdss", "ls", "ps1"]`; PS1 is a
  live external request, so drop it or set `[]` to disable).

## Configure

In the SkyPortal config, set your Lasair token, the LSST instrument name, ingest
targets, and the SQL queries to run. `fields` **must** select
`objects.diaObjectId`, `objects.ra`, and `objects.decl`; any other selected
columns are stored on the annotation.

```yaml
services:
    external:
        lasair:
            params:
                lasair:
                    token: <your Lasair API token>
                ingest:
                    poll_interval: 86400
                    group_ids: [1]
                    lsst_instrument_name: LSST
                    filter_ids: []
                    queries:
                        - name: nightly
                          fields: 'objects.diaObjectId, objects.ra, objects.decl'
                          tables: objects
                          conditions: 'objects.nDiaSources > 2 AND objects.firstDiaSourceMjdTai > (mjdnow() - 40)'
```

The configured `lsst_instrument_name` instrument must exist in SkyPortal and
have the `lsst{u,g,r,i,z,y}` filters, or photometry is skipped.

### Routing queries to different groups/filters

`group_ids`, `filter_ids` and `lsst_stream_ids` under `ingest` are the defaults
for every query. Set any of them **on a query** to route it separately (an
override wins even if `[]`; omitted keys fall back to the `ingest` default):

```yaml
queries:
    - name: nightly            # -> default group_ids / filter_ids
      fields: 'objects.diaObjectId, objects.ra, objects.decl'
      tables: objects
      conditions: 'objects.nDiaSources > 2'
    - name: bright-transients  # -> its own group + scanning filter
      group_ids: [2]
      filter_ids: [5]
      fields: 'objects.diaObjectId, objects.ra, objects.decl'
      tables: objects
      conditions: 'objects.nDiaSources > 2 AND objects.firstDiaSourceMjdTai > (mjdnow() - 5)'
```

If two queries return the same object, the Obj/photometry are shared and group
access is additive; only the single `origin: lasair` annotation is overwritten
by whichever query wrote it last.

See `config.yaml.defaults` for the full set of options and a worked
Sherlock/TNS-filtered query.

## Local testing

`main.py` reads config via `baselayer.app.env.load_env`, so a `config.yaml`
(git-ignored) in this directory is picked up when run standalone alongside a
SkyPortal checkout. It requires `skyportal`/`baselayer` to be importable and the
`lasair` client (`pip install lasair`).
