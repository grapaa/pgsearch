import logging
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path

from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
)

from pgsearch.chunker import chunk_text
from pgsearch.database import DatabaseService
from pgsearch.embedding import EmbeddingService
from pgsearch.extractor import extract_text

from . import scraper
from .downloader import download_documents

from .models import ByggesakDokument

console = Console()
log = logging.getLogger("pgsearch.pipeline")

_LOG_DIR = Path(__file__).parent.parent.parent / "logs"
_LOG_FORMATTER = logging.Formatter(
    "%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


@contextmanager
def _date_log(date_str: str):
    """Add a per-date FileHandler to the pgsearch logger for the duration of a pipeline run."""
    _LOG_DIR.mkdir(exist_ok=True)
    log_path = _LOG_DIR / f"{date_str}.log"
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(_LOG_FORMATTER)
    root = logging.getLogger("pgsearch")
    root.addHandler(handler)
    try:
        yield log_path
    finally:
        handler.close()
        root.removeHandler(handler)


def run_daily_pipeline(
    db: DatabaseService,
    embedding: EmbeddingService,
    target_date: date,
    data_dir: Path,
) -> None:
    """Run the full innsyn pipeline for a single date."""
    date_str = target_date.strftime("%Y-%m-%d")
    with _date_log(date_str):
        _run_daily_pipeline(date_str, target_date, db, embedding, data_dir)


def _run_daily_pipeline(
    date_str: str,
    target_date: date,
    db: DatabaseService,
    embedding: EmbeddingService,
    data_dir: Path,
) -> None:
    log.info("=== Pipeline start: %s ===", date_str)
    console.print(f"\n[bold cyan]Henter byggesaker for {target_date.strftime('%d.%m.%Y')}...[/]")

    # 1. Scrape from GraphQL API
    docs = scraper.fetch_byggesaker(target_date)
    log.info("Hentet %d journalposter for %s", len(docs), date_str)
    if not docs:
        console.print("[yellow]Ingen dokumenter funnet for denne datoen.[/]")
        return

    # 2. Upsert byggesak metadata
    saker = _docs_to_byggesaker(docs)
    if saker:
        console.print(f"Lagrer metadata for {len(saker)} saker...")
        db.upsert_byggesaker(saker)
        console.print(f"[green]Lagret metadata for {len(saker)} saker.[/]")
        log.info("Upsert %d saker: %s", len(saker), [s["saksnr"] for s in saker])

    # 3. Download PDFs to disk
    console.print(f"\n[bold cyan]Laster ned {len(docs)} dokumenter...[/]")
    downloaded, skipped, failed = download_documents(docs, target_date, data_dir, console)
    log.info("Nedlasting ferdig: %d lastet ned, %d hoppet over, %d feilet", downloaded, skipped, failed)

    # 4. Index downloaded files
    date_folder = data_dir / "raw" / target_date.strftime("%Y-%m-%d")
    if not date_folder.exists():
        log.warning("Mappe finnes ikke etter nedlasting: %s", date_folder)
        return

    _index_downloaded_files(db, embedding, date_folder)
    log.info("=== Pipeline ferdig: %s ===", date_str)


def run_range_pipeline(
    db: DatabaseService,
    embedding: EmbeddingService,
    from_date: date,
    to_date: date,
    data_dir: Path,
) -> None:
    """Run the innsyn pipeline for each day in a date range."""
    current = from_date
    while current <= to_date:
        run_daily_pipeline(db, embedding, current, data_dir)
        current += timedelta(days=1)


def _docs_to_byggesaker(docs: list) -> list[dict]:
    """Convert ByggesakDokument list to unique byggesaker dicts for upsert."""
    seen: dict[str, dict] = {}
    for doc in docs:
        if not doc.saksnr:
            continue
        if doc.saksnr not in seen:
            seen[doc.saksnr] = {
                "saksnr": doc.saksnr,
                "metadata": {
                    "gnr_bnr": doc.gnr_bnr,
                    "sakstype": doc.sakstype,
                    "beskrivelse": doc.beskrivelse,
                    "avsender_mottaker": doc.avsender_mottaker,
                    "saksbehandler": doc.saksbehandler,
                    "dato": doc.dato,
                },
            }
    return list(seen.values())


def _index_downloaded_files(
    db: DatabaseService, embedding: EmbeddingService, date_folder: Path
) -> None:
    """Index all PDF/TXT files under a date folder."""
    files = [
        f
        for f in date_folder.rglob("*")
        if f.suffix.lower() in (".pdf", ".txt") and not f.name.endswith(".ocr.txt")
    ]

    if not files:
        console.print("[yellow]Ingen filer å indeksere.[/]")
        log.warning("Ingen filer funnet i %s", date_folder)
        return

    indexed_ids = db.get_indexed_document_ids()

    new_files = []
    for f in files:
        doc_id = f"{f.parent.name}/{f.name}"
        if doc_id in indexed_ids:
            log.debug("Allerede indeksert, hopper over: %s", f)
            continue
        new_files.append((f, doc_id))

    already_indexed = len(files) - len(new_files)
    console.print(
        f"Fant [green]{len(files)}[/] fil(er), [green]{len(new_files)}[/] nye "
        f"(hopper over {already_indexed} allerede indekserte)."
    )

    log.info(
        "Indeksering: %d filer totalt, %d nye, %d allerede indeksert",
        len(files), len(new_files), already_indexed,
    )

    if not new_files:
        console.print("[yellow]Alle filer er allerede indeksert.[/]")
        return

    with Progress(
        SpinnerColumn(),
        TextColumn("[cyan]{task.description}[/]"),
        BarColumn(),
        MofNCompleteColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Indekserer", total=len(new_files))

        for file_path, doc_id in new_files:
            progress.update(task, description=file_path.name)
            try:
                _index_file(db, embedding, file_path, doc_id)
            except Exception as e:
                log.error("Feil ved indeksering av %s: %s", file_path, e, exc_info=True)
                console.print(f"[red]Feil ved {file_path.name}: {e}[/]")
            progress.advance(task)

    console.print("[green]Indeksering fullført![/]")


def _index_file(
    db: DatabaseService,
    embedding: EmbeddingService,
    file_path: Path,
    document_id: str,
) -> None:
    """Extract, chunk, embed, and store a single file."""
    file_size = file_path.stat().st_size
    text = extract_text(file_path).replace("\0", "")

    if not text.strip():
        log.warning("Tom fil — ingen tekst ekstrahert: %s  [%d bytes]", file_path, file_size)
        console.print(f"[yellow]Tom fil: {file_path.name}[/]")
        return

    chunks = chunk_text(text)
    embeddings = embedding.get_embeddings(chunks)

    # Derive saksnr from the sak folder name (e.g. "24_1234" -> "24/1234")
    sak_folder_name = file_path.parent.name
    saksnr = sak_folder_name.replace("_", "/") if "_" in sak_folder_name else None

    db_chunks = [
        {
            "document_id": document_id,
            "chunk_index": i,
            "content": content,
            "embedding": emb,
            "saksnr": saksnr,
            "metadata": {
                "source_path": str(file_path),
                "file_type": file_path.suffix.lstrip("."),
                "chunk_count": len(chunks),
                "source": "innsyn",
            },
        }
        for i, (content, emb) in enumerate(zip(chunks, embeddings))
    ]

    db.insert_chunks(db_chunks)
    log.info(
        "Indeksert: %s  [saksnr=%s, %d chunks, %d bytes]",
        file_path, saksnr, len(chunks), file_size,
    )
