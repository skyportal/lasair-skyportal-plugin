"""SkyPortal micro-service ingesting Lasair (LSST) broker objects.

Lasair is a poll-based broker: we run one or more SQL queries against the
Lasair API (objects + sherlock_classifications + crossmatch_tns), then for each
returned object fetch its diaSources lightcurve and cutout image URLs. Per
object we create/use the Obj, ingest LSST photometry, optionally register it as
a Candidate under configured filters, store the TNS/Sherlock crossmatch as an
annotation, and post new/ref/sub cutout thumbnails.

Writes go straight to the SkyPortal DB via the models + ``add_external_photometry``
(direct-DB, batched — mirrors the babamul plugin). Runs as a supervised SkyPortal
external service, re-querying Lasair every ``poll_interval`` seconds.

The ``lasair`` client and ``confluent``-style deps are imported lazily so the
pure ingestion helpers can be imported/tested without them installed.
"""

import base64
import io
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import numpy as np
import requests
import sqlalchemy as sa
from astropy.io import fits
from astropy.visualization import (
    AsymmetricPercentileInterval,
    ImageNormalize,
    LinearStretch,
    LogStretch,
)
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
from scipy.ndimage import rotate
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.session import Session

from baselayer.app.env import load_env
from baselayer.app.models import init_db, session_context_id
from baselayer.log import make_log
from skyportal.handlers.api.photometry import add_external_photometry
from skyportal.handlers.api.thumbnail import post_thumbnail
from skyportal.models import (
    Annotation,
    Candidate,
    DBSession,
    Instrument,
    Obj,
    User,
)

log = make_log("lasair")

# Lasair LSST psfFlux/psfFluxErr are in nJy; SkyPortal computes
# mag = -2.5*log10(flux) + zp, and the AB zeropoint for nJy is 31.4
# (= 2.5*log10(3631e9)), so we upload raw nJy flux with zp=31.4. (Equivalent to
# the babamul convention of flux*1e-9 with zp=8.9.)
DEFAULT_ZP = 31.4
LSST_FILTER_MAP = {
    "u": "lsstu",
    "g": "lsstg",
    "r": "lsstr",
    "i": "lssti",
    "z": "lsstz",
    "y": "lssty",
}
# Lasair cutout image kind -> SkyPortal thumbnail type
THUMBNAIL_TYPES = [("Science", "new"), ("Template", "ref"), ("Difference", "sub")]
MAX_PHOT_ROWS_PER_POST = 8000  # keep each aggregated post under SkyPortal's cap


def _json_safe(o):
    """Recursively replace NaN/Inf (invalid JSON) with None, for JSONB storage."""
    if isinstance(o, float):
        return o if np.isfinite(o) else None
    if isinstance(o, dict):
        return {k: _json_safe(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_json_safe(v) for v in o]
    return o


# --- Lasair API ------------------------------------------------------------


def get_lasair_client(token, endpoint):
    import lasair

    return lasair.lasair_client(token=token, endpoint=endpoint)


def _lasair_call(fn, *args, max_retries=3, **kwargs):
    """Call a Lasair client method with retry/backoff on rate limits, mirroring
    the reference script (wait 1h on 'Request limit exceeded', 10m on
    'Max retries exceeded'). Returns None if it ultimately fails."""
    for attempt in range(max_retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            msg = str(e)
            if "Request limit exceeded" in msg:
                log("Lasair request limit exceeded; waiting 1h")
                time.sleep(3600)
            elif "Max retries exceeded" in msg:
                log("Lasair max retries exceeded; waiting 10m")
                time.sleep(600)
            else:
                log(f"Lasair call failed ({type(e).__name__}): {msg}")
                if attempt == max_retries - 1:
                    return None
                time.sleep(5)
    return None


def query_objects(L, query, limit_per_batch=10000, request_sleep=0.1):
    """Run a paginated Lasair SQL query (fields/tables/conditions) and return all
    rows. Each row is a dict of the selected columns (must include diaObjectId,
    ra, decl)."""
    fields = query["fields"]
    tables = query["tables"]
    conditions = query.get("conditions", "")
    rows, offset = [], 0
    while True:
        batch = _lasair_call(
            L.query, fields, tables, conditions, limit=limit_per_batch, offset=offset
        )
        if not batch:
            break
        rows.extend(batch)
        log(
            f"[{query.get('name', 'query')}] fetched {len(rows)} rows (offset {offset})"
        )
        if len(batch) < limit_per_batch:
            break
        offset += limit_per_batch
        time.sleep(request_sleep)
    return rows


def fetch_object(L, obj_id):
    """Fetch a single object's full record (diaSourcesList + lasairData)."""
    return _lasair_call(L.object, obj_id)


# --- Cutout thumbnails -----------------------------------------------------


def download_cutout(url, http_session, timeout=15):
    """Download a FITS cutout from a Lasair image URL; returns raw bytes."""
    resp = http_session.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.content


def make_thumbnail(obj_id, cutout_bytes, cutout_kind, thumbnail_type):
    """Render Lasair (uncompressed) FITS cutout bytes to a base64 PNG thumbnail.

    Uses the matplotlib OO API (not pyplot) so rendering is thread-safe and can
    run in a ThreadPoolExecutor off the ingestion hot path."""
    with fits.open(io.BytesIO(cutout_bytes), ignore_missing_simple=True) as hdul:
        data = hdul[0].data if hdul[0].data is not None else hdul[1].data
        rotpa = hdul[0].header.get("ROTPA", None)

    buff = io.BytesIO()
    fig = Figure(figsize=(4, 4))
    FigureCanvasAgg(fig)
    ax = fig.add_axes([0.0, 0.0, 1.0, 1.0])
    ax.set_axis_off()

    img = np.array(data, dtype=float)
    xl = np.greater(
        np.abs(img), 1e20, out=np.zeros(img.shape, dtype=bool), where=~np.isnan(img)
    )
    if img[xl].any():
        img[xl] = np.nan
    if np.isnan(img).any():
        img = np.nan_to_num(img, nan=float(np.nanmean(img.flatten())))

    stretch = LinearStretch() if cutout_kind == "Difference" else LogStretch()
    img_norm = ImageNormalize(img, stretch=stretch)(img)
    vmin, vmax = AsymmetricPercentileInterval(
        lower_percentile=1, upper_percentile=100
    ).get_limits(img_norm)

    if rotpa is not None:  # orient North up, West right when WCS angle is known
        try:
            img_norm = rotate(
                img_norm, -rotpa, reshape=True, order=1, mode="constant", cval=0.0
            )
        except Exception as e:
            log(f"Failed to rotate cutout for {obj_id}: {e}")

    ax.imshow(img_norm, cmap="bone", origin="lower", vmin=vmin, vmax=vmax)
    fig.savefig(buff, dpi=42)
    buff.seek(0)
    return {
        "obj_id": obj_id,
        "data": base64.b64encode(buff.read()).decode("utf-8"),
        "ttype": thumbnail_type,
    }


def add_cutout_thumbnails(obj_id, image_urls, http_session):
    """Download Lasair Science/Template/Difference FITS cutouts and post new/ref/sub
    thumbnails. Each thumbnail is posted in its own session so a bad cutout can't
    poison the caller's transaction."""
    if not isinstance(image_urls, dict):
        return
    for cutout_kind, thumbnail_type in THUMBNAIL_TYPES:
        url = image_urls.get(cutout_kind)
        if not url:
            continue
        try:
            cutout_bytes = download_cutout(url, http_session)
            thumb = make_thumbnail(obj_id, cutout_bytes, cutout_kind, thumbnail_type)
            with DBSession() as s:
                post_thumbnail(thumb, user_id=1, session=s)
                s.commit()
        except Exception as e:
            log(f"Failed to post {thumbnail_type} thumbnail for {obj_id}: {e}")


# --- Photometry ------------------------------------------------------------


def band_to_filter(band):
    if band is None:
        return None
    b = str(band).lower()
    if b.startswith("lsst"):
        return b
    return LSST_FILTER_MAP.get(b)


def collect_lasair_photometry(dia_sources, ra, dec):
    """Flatten an object's diaSources list into photometry points. Lasair LSST
    lightcurves use midpointMjdTai (already MJD) and nJy psfFlux/psfFluxErr."""
    points = []
    for p in dia_sources or []:
        points.append(
            {
                "mjd": p.get("midpointMjdTai"),
                "band": p.get("band"),
                "flux": p.get("psfFlux"),
                "flux_err": p.get("psfFluxErr"),
                "ra": ra,
                "dec": dec,
            }
        )
    return points


def _new_phot_acc():
    return {
        k: []
        for k in (
            "obj_id",
            "mjd",
            "flux",
            "fluxerr",
            "filter",
            "zp",
            "magsys",
            "ra",
            "dec",
        )
    }


def _accumulate_points(acc, seen, obj_id, points, zp):
    """Append valid/deduped points for obj_id into a cross-object accumulator.
    The dedup key includes obj_id so points for different objects in one batch
    never collide (matches SkyPortal's Photometry dedup: obj_id, instrument_id,
    origin, mjd, fluxerr, flux — instrument_id/origin fixed per post)."""
    for p in points:
        mjd, ferr, flux = p["mjd"], p["flux_err"], p["flux"]
        if mjd is None or ferr is None or not np.isfinite(ferr):
            continue
        filt = band_to_filter(p["band"])
        if filt is None:
            continue
        flux = flux if (flux is not None and np.isfinite(flux)) else None
        key = (obj_id, mjd, ferr, flux)
        if key in seen:
            continue
        seen.add(key)
        acc["obj_id"].append(obj_id)
        acc["mjd"].append(mjd)
        acc["flux"].append(flux)
        acc["fluxerr"].append(ferr)
        acc["filter"].append(filt)
        acc["zp"].append(zp)
        acc["magsys"].append("ab")
        acc["ra"].append(p["ra"])
        acc["dec"].append(p["dec"])


def _split_phot_data(acc, instrument_id, stream_ids, group_ids):
    """Split an accumulator into <=MAX_PHOT_ROWS_PER_POST posts, never splitting a
    single obj's points across two posts."""
    out, cur, count, cur_obj = [], _new_phot_acc(), 0, None
    keys = [k for k in acc if k != "obj_id"]
    for i, oid in enumerate(acc["obj_id"]):
        if count >= MAX_PHOT_ROWS_PER_POST and oid != cur_obj:
            out.append(cur)
            cur, count = _new_phot_acc(), 0
        cur["obj_id"].append(oid)
        for k in keys:
            cur[k].append(acc[k][i])
        count += 1
        cur_obj = oid
    if cur["mjd"]:
        out.append(cur)
    result = []
    for c in out:
        d = {**c, "instrument_id": instrument_id, "group_ids": group_ids}
        if stream_ids:
            d["stream_ids"] = stream_ids
        result.append(d)
    return result


def post_accumulated(acc, instrument_id, stream_ids, group_ids, user, session):
    if not acc["mjd"] or instrument_id is None:
        return
    if len(acc["mjd"]) <= MAX_PHOT_ROWS_PER_POST:
        data = {**acc, "instrument_id": instrument_id, "group_ids": group_ids}
        if stream_ids:
            data["stream_ids"] = stream_ids
        chunks = [data]
    else:
        chunks = _split_phot_data(acc, instrument_id, stream_ids, group_ids)
    for d in chunks:
        try:
            add_external_photometry(d, user, session)
        except Exception as e:
            log(
                f"batched photometry post failed ({len(d['mjd'])} pts): "
                f"{type(e).__name__}: {str(e).splitlines()[0][:200]}"
            )


# --- DB helpers ------------------------------------------------------------


def get_or_create_object(obj_id, ra, dec, session: Session) -> tuple[Obj, bool]:
    obj = session.scalar(sa.select(Obj).where(Obj.id == obj_id))
    if obj is not None:
        return obj, False
    try:
        with session.begin_nested():
            obj = Obj(id=obj_id, ra=ra, dec=dec, ra_dis=ra, dec_dis=dec)
            session.add(obj)
            session.flush()  # force the INSERT inside the savepoint
    except IntegrityError:
        obj = session.scalar(sa.select(Obj).where(Obj.id == obj_id))  # lost a race
        if obj is None:
            raise
        return obj, False
    return obj, True


def make_instrument_id(session, ingest_cfg):
    name = ingest_cfg.get("lsst_instrument_name", "LSST")
    iid = session.scalar(sa.select(Instrument.id).where(Instrument.name == name))
    if iid is None:
        log(f"Instrument '{name}' not found; photometry will be skipped")
    return iid


def _cutout_worker(obj_id, image_urls):
    """Run cutout download+render+post on a pool thread. The scoped DBSession is
    keyed by the session_context_id contextvar (default None in every thread), so
    set a unique id per task and dispose the session after."""
    session_context_id.set(uuid.uuid4().hex)
    http_session = requests.Session()
    try:
        add_cutout_thumbnails(obj_id, image_urls, http_session)
    finally:
        http_session.close()
        DBSession.remove()


# --- Ingestion -------------------------------------------------------------


def _annotation_data(row):
    """Build the annotation blob from the extra query columns (everything that
    isn't the object id / coordinates)."""
    skip = {"diaObjectId", "ra", "decl", "dec"}
    data = {k: v for k, v in row.items() if k not in skip and v is not None}
    return _json_safe(data) if data else None


def process_batch(
    fetched,
    session,
    *,
    ingest_cfg,
    instrument_id,
    user,
    cutout_pool=None,
):
    """Ingest a chunk of already-fetched Lasair objects.

    ``fetched`` is a list of dicts: {id, ra, dec, dia_sources, image_urls,
    annotation}. Does bulk obj get-or-create, ONE aggregated photometry post,
    bulk candidate/annotation upserts, and off-path cutout thumbnails."""
    if not fetched:
        return
    group_ids = ingest_cfg.get("group_ids") or [1]
    filter_ids = ingest_cfg.get("filter_ids") or []
    stream_ids = ingest_cfg.get("lsst_stream_ids", [])
    zp = float(ingest_cfg.get("zp", DEFAULT_ZP))
    now = datetime.now(timezone.utc)

    # 1. bulk get-or-create objs
    want = {f["id"]: (f["ra"], f["dec"]) for f in fetched}
    existing = set(
        session.scalars(sa.select(Obj.id).where(Obj.id.in_(list(want)))).all()
    )
    new_ids = [oid for oid in want if oid not in existing]
    if new_ids:
        session.add_all(
            [
                Obj(
                    id=oid,
                    ra=want[oid][0],
                    dec=want[oid][1],
                    ra_dis=want[oid][0],
                    dec_dis=want[oid][1],
                )
                for oid in new_ids
            ]
        )
    session.commit()

    # 2. dispatch cutouts for newly-created objs (off the DB hot path)
    if cutout_pool is not None and ingest_cfg.get("fetch_cutouts", True):
        new_set = set(new_ids)
        for f in fetched:
            if f["id"] in new_set and f.get("image_urls"):
                new_set.discard(f["id"])  # one fetch per obj
                cutout_pool.submit(_cutout_worker, f["id"], f["image_urls"])

    # 3. aggregate + post photometry in one call
    acc, seen = _new_phot_acc(), set()
    for f in fetched:
        _accumulate_points(
            acc,
            seen,
            f["id"],
            collect_lasair_photometry(f["dia_sources"], f["ra"], f["dec"]),
            zp,
        )
    post_accumulated(acc, instrument_id, stream_ids, group_ids, user, session)

    # 4. bulk candidates (unique index obj_id+filter_id+passed_at; stamp per obj)
    cand_rows = [
        {
            "obj_id": f["id"],
            "filter_id": fid,
            "passed_at": datetime.now(timezone.utc),
            "uploader_id": user.id,
            "created_at": now,
            "modified": now,
        }
        for f in fetched
        for fid in filter_ids
    ]
    if cand_rows:
        session.execute(
            pg_insert(Candidate).on_conflict_do_nothing(
                index_elements=["obj_id", "filter_id", "passed_at"]
            ),
            cand_rows,
        )

    # 5. bulk annotations (ON CONFLICT on obj_id+origin)
    ann_rows = [
        {
            "obj_id": f["id"],
            "origin": "lasair",
            "data": f["annotation"],
            "author_id": user.id,
            "created_at": now,
            "modified": now,
        }
        for f in fetched
        if f.get("annotation")
    ]
    if ann_rows:
        stmt = pg_insert(Annotation)
        session.execute(
            stmt.on_conflict_do_update(
                index_elements=["obj_id", "origin"],
                set_={"data": stmt.excluded.data, "modified": stmt.excluded.modified},
            ),
            ann_rows,
        )
    session.commit()


def _fetch_for_ingest(L, row, fetch_cutouts):
    """Fetch one object's lightcurve (+ cutout urls) and build its ingest dict.
    Returns None on missing coordinates or a failed fetch. Safe to run on a pool
    thread (only touches the Lasair client, no shared state)."""
    oid = row.get("diaObjectId")
    ra = row.get("ra")
    dec = row.get("decl", row.get("dec"))
    if oid is None or ra is None or dec is None:
        return None
    oid = str(oid)
    obj_data = fetch_object(L, oid)
    if not obj_data:
        return None
    image_urls = None
    if fetch_cutouts:
        urls = (obj_data.get("lasairData", {}) or {}).get("imageUrls")
        if isinstance(urls, list) and urls:
            image_urls = urls[0]
    return {
        "id": oid,
        "ra": float(ra),
        "dec": float(dec),
        "dia_sources": obj_data.get("diaSourcesList") or [],
        "image_urls": image_urls,
        "annotation": _annotation_data(row),
    }


def run_poll(L, *, lasair_cfg, ingest_cfg, instrument_id, user, cutout_pool):
    """One polling cycle: run each configured query, fetch object lightcurves +
    cutouts, and batch-ingest into the DB.

    The per-object ``L.object`` fetch is the dominant cost (~0.5s each serially)
    and parallelizes ~3x across a few workers, while DB writes stay batched
    (~9ms/obj). So we fetch concurrently and flush full batches to the DB."""
    queries = ingest_cfg.get("queries") or []
    if not queries:
        log("No queries configured under ingest.queries; nothing to do")
        return
    limit_per_batch = int(lasair_cfg.get("limit_per_batch", 10000))
    request_sleep = float(lasair_cfg.get("request_sleep", 0.1))
    batch_size = int(ingest_cfg.get("batch_size", 200))
    fetch_cutouts = bool(ingest_cfg.get("fetch_cutouts", True))
    fetch_workers = int(lasair_cfg.get("fetch_workers", 4))

    for query in queries:
        rows = query_objects(L, query, limit_per_batch, request_sleep)
        log(f"[{query.get('name', 'query')}] {len(rows)} objects to ingest")

        buffer = []
        with ThreadPoolExecutor(max_workers=fetch_workers) as fetch_pool:
            results = fetch_pool.map(
                lambda r: _fetch_for_ingest(L, r, fetch_cutouts), rows
            )
            for fetched in results:
                if fetched is None:
                    continue
                buffer.append(fetched)
                if len(buffer) >= batch_size:
                    _flush(buffer, ingest_cfg, instrument_id, user, cutout_pool)
                    buffer = []
        if buffer:
            _flush(buffer, ingest_cfg, instrument_id, user, cutout_pool)


def _flush(buffer, ingest_cfg, instrument_id, user, cutout_pool):
    try:
        with DBSession() as session:
            process_batch(
                buffer,
                session,
                ingest_cfg=ingest_cfg,
                instrument_id=instrument_id,
                user=user,
                cutout_pool=cutout_pool,
            )
    except Exception as e:
        traceback.print_exc()
        log(f"Error ingesting batch of {len(buffer)} objects: {e}")


def main():
    _, cfg = load_env()
    init_db(**cfg["database"])

    params = cfg.get("services.external.lasair.params", {})
    lasair_cfg = params.get("lasair", {})
    ingest_cfg = params.get("ingest", {})

    token = lasair_cfg.get("token")
    endpoint = lasair_cfg.get("endpoint", "https://api.lasair.lsst.ac.uk/api")
    if not token or token.startswith("your_lasair"):
        log("Lasair token not configured (lasair.token); aborting")
        return

    with DBSession() as session:
        user = session.scalar(sa.select(User).where(User.id == 1))
        if user is None:
            log("User id 1 not found; aborting")
            return
        instrument_id = make_instrument_id(session, ingest_cfg)

    L = get_lasair_client(token, endpoint)
    cutout_pool = ThreadPoolExecutor(
        max_workers=int(ingest_cfg.get("cutout_workers", 4))
    )
    poll_interval = float(ingest_cfg.get("poll_interval", 86400))

    log(f"Lasair plugin started (endpoint {endpoint}); poll interval {poll_interval}s")
    while True:
        start = datetime.now()
        try:
            run_poll(
                L,
                lasair_cfg=lasair_cfg,
                ingest_cfg=ingest_cfg,
                instrument_id=instrument_id,
                user=user,
                cutout_pool=cutout_pool,
            )
        except Exception as e:
            traceback.print_exc()
            log(f"Poll cycle failed: {e}")
        elapsed = (datetime.now() - start).total_seconds()
        sleep_for = max(0.0, poll_interval - elapsed)
        log(f"Poll cycle done in {elapsed:.0f}s; sleeping {sleep_for:.0f}s")
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
