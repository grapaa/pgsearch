import logging
import re
import unicodedata
from datetime import date
from pathlib import Path
from urllib.parse import urlparse

import httpx
from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, SpinnerColumn, TextColumn

from .models import ByggesakDokument, Vedlegg
from .scraper import BASE_URL

console = Console()
log = logging.getLogger("pgsearch.downloader")

HTTP_TIMEOUT = 120


def _safe_print(msg: str, con: Console | None = None) -> None:
    """Print with ASCII-safe fallback for Windows console encoding issues."""
    target = con or console
    try:
        target.print(msg)
    except UnicodeEncodeError:
        safe = unicodedata.normalize("NFKD", msg).encode("ascii", "replace").decode()
        print(safe)


def download_documents(
    docs: list[ByggesakDokument], target_date: date, data_dir: Path, con: Console | None = None
) -> tuple[int, int, int]:
    """Download main documents and attachments for a list of byggesaker."""
    date_folder = data_dir / "raw" / target_date.strftime("%Y-%m-%d")
    date_folder.mkdir(parents=True, exist_ok=True)

    downloaded = 0
    skipped = 0
    failed = 0

    # Count total files to download for accurate progress (only vedlegg with a URL)
    total_files = sum(
        (1 if doc.er_tilgjengelig and doc.dokument_url else 0)
        + sum(1 for v in doc.vedlegg if v.url)
        for doc in docs
    )

    with httpx.Client(
        timeout=HTTP_TIMEOUT,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        follow_redirects=True,
    ) as client:
        with Progress(
            SpinnerColumn(),
            TextColumn("[cyan]{task.description}[/]"),
            BarColumn(),
            MofNCompleteColumn(),
            console=con or console,
        ) as progress:
            task = progress.add_task("Laster ned...", total=total_files)
            for doc in docs:
                sak_folder = date_folder / _sanitize_filename(doc.saksnr)
                sak_folder.mkdir(parents=True, exist_ok=True)

                if doc.er_tilgjengelig and doc.dokument_url:
                    filename = _get_main_document_filename(doc)
                    progress.update(task, description=filename[:60])
                    d, s, f = _download_main(client, doc, sak_folder)
                    downloaded += d
                    skipped += s
                    failed += f
                    progress.advance(task)

                    for vedlegg in doc.vedlegg:
                        if vedlegg.url:
                            vfilename = _get_vedlegg_filename(vedlegg)
                            progress.update(task, description=vfilename[:60])
                            d, s, f = _download_vedlegg(client, vedlegg, sak_folder)
                            downloaded += d
                            skipped += s
                            failed += f
                            progress.advance(task)

    _safe_print(
        f"[green]Lastet ned {downloaded} filer, "
        f"hoppet over {skipped}, feilet {failed}[/green]",
        con or console,
    )
    return downloaded, skipped, failed


def _download_main(
    client: httpx.Client, doc: ByggesakDokument, sak_folder: Path
) -> tuple[int, int, int]:
    filename = _get_main_document_filename(doc)
    filepath = sak_folder / filename
    if filepath.exists():
        log.debug("Allerede lastet ned, hopper over: %s", filepath)
        return 0, 1, 0

    url = _build_full_url(doc.dokument_url)
    return _download_file(client, url, filepath)


def _download_vedlegg(
    client: httpx.Client, vedlegg: Vedlegg, sak_folder: Path
) -> tuple[int, int, int]:
    filename = _get_vedlegg_filename(vedlegg)
    filepath = sak_folder / filename
    if filepath.exists():
        log.debug("Allerede lastet ned, hopper over: %s", filepath)
        return 0, 1, 0

    url = _build_full_url(vedlegg.url)
    return _download_file(client, url, filepath)


def _download_file(client: httpx.Client, url: str, filepath: Path) -> tuple[int, int, int]:
    tmp = filepath.with_suffix(".tmp")
    try:
        with client.stream("GET", url) as resp:
            resp.raise_for_status()
            size = 0
            with open(tmp, "wb") as f:
                for chunk in resp.iter_bytes():
                    f.write(chunk)
                    size += len(chunk)
        tmp.rename(filepath)  # atomic on all major OS
        log.info("Lastet ned: %s  [%d bytes]  url=%s", filepath, size, url)
        return 1, 0, 0
    except Exception as e:
        log.error("Nedlasting feilet: %s  url=%s  feil=%s", filepath, url, e)
        _safe_print(f"  [red]Feil ved nedlasting {filepath.name}: {e}[/red]")
        if tmp.exists():
            tmp.unlink()
        return 0, 0, 1


def _build_full_url(url: str) -> str:
    if url.startswith(("http://", "https://")):
        return url
    base = BASE_URL.rstrip("/")
    return f"{base}/{url.lstrip('/')}"


def _get_extension_from_url(url: str, default: str = ".pdf") -> str:
    try:
        path = url.split("?")[0]
        parsed = urlparse(path) if path.startswith("http") else None
        if parsed:
            path = parsed.path
        ext = Path(path).suffix
        return ext if ext else default
    except Exception:
        return default


def _get_main_document_filename(doc: ByggesakDokument) -> str:
    ext = _get_extension_from_url(doc.dokument_url)
    filename = f"{doc.dato}_{doc.beskrivelse}{ext}"
    return _sanitize_filename(filename)


def _get_vedlegg_filename(vedlegg: Vedlegg) -> str:
    ext = _get_extension_from_url(vedlegg.url)
    filename = f"Vedlegg_{vedlegg.nummer}_{vedlegg.navn}{ext}"
    return _sanitize_filename(filename)


def _sanitize_filename(name: str) -> str:
    name = unicodedata.normalize("NFKC", name)
    name = re.sub(r"[\x00-\x1f\x7f]", " ", name)  # strip control chars incl. \n \r \t
    sanitized = re.sub(r'[<>:"/\\|?*]', "_", name)
    sanitized = re.sub(r"\s+", " ", sanitized)
    sanitized = re.sub(r"_+", "_", sanitized)
    return sanitized.strip("_.").strip()
