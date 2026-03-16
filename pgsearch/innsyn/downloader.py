import re
import unicodedata
from datetime import date
from pathlib import Path
from urllib.parse import urlparse

import httpx
from rich.console import Console
from rich.progress import Progress

from .models import ByggesakDokument, Vedlegg
from .scraper import BASE_URL

console = Console()

HTTP_TIMEOUT = 120


def _safe_print(msg: str) -> None:
    """Print with ASCII-safe fallback for Windows console encoding issues."""
    try:
        console.print(msg)
    except UnicodeEncodeError:
        safe = unicodedata.normalize("NFKD", msg).encode("ascii", "replace").decode()
        print(safe)


def download_documents(
    docs: list[ByggesakDokument], target_date: date, data_dir: Path
) -> tuple[int, int, int]:
    """Download main documents and attachments for a list of byggesaker."""
    date_folder = data_dir / "raw" / target_date.strftime("%Y-%m-%d")
    date_folder.mkdir(parents=True, exist_ok=True)

    downloaded = 0
    skipped = 0
    failed = 0

    with httpx.Client(
        timeout=HTTP_TIMEOUT,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        follow_redirects=True,
    ) as client:
        with Progress() as progress:
            task = progress.add_task("Laster ned...", total=len(docs))
            for doc in docs:
                sak_folder = date_folder / _sanitize_filename(doc.saksnr)
                sak_folder.mkdir(parents=True, exist_ok=True)

                if doc.er_tilgjengelig and doc.dokument_url:
                    d, s, f = _download_main(client, doc, sak_folder)
                    downloaded += d
                    skipped += s
                    failed += f

                    for vedlegg in doc.vedlegg:
                        if vedlegg.url:
                            d, s, f = _download_vedlegg(client, vedlegg, sak_folder)
                            downloaded += d
                            skipped += s
                            failed += f

                progress.advance(task)

    _safe_print(
        f"[green]Lastet ned {downloaded} filer, "
        f"hoppet over {skipped}, feilet {failed}[/green]"
    )
    return downloaded, skipped, failed


def _download_main(
    client: httpx.Client, doc: ByggesakDokument, sak_folder: Path
) -> tuple[int, int, int]:
    filename = _get_main_document_filename(doc)
    filepath = sak_folder / filename
    if filepath.exists():
        return 0, 1, 0

    url = _build_full_url(doc.dokument_url)
    return _download_file(client, url, filepath)


def _download_vedlegg(
    client: httpx.Client, vedlegg: Vedlegg, sak_folder: Path
) -> tuple[int, int, int]:
    filename = _get_vedlegg_filename(vedlegg)
    filepath = sak_folder / filename
    if filepath.exists():
        return 0, 1, 0

    url = _build_full_url(vedlegg.url)
    return _download_file(client, url, filepath)


def _download_file(client: httpx.Client, url: str, filepath: Path) -> tuple[int, int, int]:
    try:
        resp = client.get(url)
        resp.raise_for_status()
        filepath.write_bytes(resp.content)
        return 1, 0, 0
    except Exception as e:
        _safe_print(f"  [red]Feil ved nedlasting {filepath.name}: {e}[/red]")
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
    sanitized = re.sub(r'[<>:"/\\|?*]', "_", name)
    sanitized = re.sub(r"_+", "_", sanitized)
    return sanitized.strip("_.").strip()
