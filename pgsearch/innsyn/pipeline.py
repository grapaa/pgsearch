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
from .filter import should_index
from .models import ByggesakDokument

console = Console()


def run_daily_pipeline(
    db: DatabaseService,
    embedding: EmbeddingService,
    target_date: date,
    data_dir: Path,
) -> None:
    """Run the full innsyn pipeline for a single date."""
    console.print(f"\n[bold cyan]Henter byggesaker for {target_date.strftime('%d.%m.%Y')}...[/]")

    # 1. Scrape from GraphQL API
    docs = scraper.fetch_byggesaker(target_date)
    if not docs:
        console.print("[yellow]Ingen dokumenter funnet for denne datoen.[/]")
        return

    # 2. Upsert byggesak metadata (alle saker, også filtrerte)
    saker = _docs_to_byggesaker(docs)
    if saker:
        db.upsert_byggesaker(saker)
        console.print(f"[green]Lagret metadata for {len(saker)} saker.[/]")

    # 3. Filtrer — fjern tegninger/bilder
    filtered_docs = _filter_docs(docs)
    skipped = len(docs) - len(filtered_docs)
    if skipped:
        console.print(f"[yellow]Filtrert bort {skipped} tegninger/bilder.[/]")

    # 4. Download PDFs to disk (kun filtrerte)
    console.print(f"\n[bold cyan]Laster ned {len(filtered_docs)} dokumenter...[/]")
    download_documents(filtered_docs, target_date, data_dir)

    # 5. Index downloaded files
    date_folder = data_dir / "raw" / target_date.strftime("%Y-%m-%d")
    if not date_folder.exists():
        return

    _index_downloaded_files(db, embedding, date_folder)


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


def _filter_docs(docs: list[ByggesakDokument]) -> list[ByggesakDokument]:
    """Remove documents with drawing/sketch/photo titles, and filter vedlegg."""
    filtered = []
    for doc in docs:
        if not should_index(doc.beskrivelse):
            continue
        # Filter vedlegg on the kept documents
        kept_vedlegg = [v for v in doc.vedlegg if should_index(v.navn)]
        if len(kept_vedlegg) != len(doc.vedlegg):
            doc = doc.model_copy(update={"vedlegg": kept_vedlegg})
        filtered.append(doc)
    return filtered


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
        return

    indexed_ids = db.get_indexed_document_ids()

    # document_id = "{saksnr_folder}/{filename}" for uniqueness across saker
    new_files = []
    for f in files:
        doc_id = f"{f.parent.name}/{f.name}"
        if doc_id not in indexed_ids:
            new_files.append((f, doc_id))

    console.print(
        f"Fant [green]{len(files)}[/] fil(er), [green]{len(new_files)}[/] nye "
        f"(hopper over {len(files) - len(new_files)} allerede indekserte)."
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
    text = extract_text(file_path).replace("\0", "")

    if not text.strip():
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
