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
from pgsearch.utils import extract_saksnr

console = Console()


def index_directory(
    db: DatabaseService, embedding: EmbeddingService, directory: str
) -> None:
    dir_path = Path(directory)
    files = [
        f
        for f in dir_path.rglob("*")
        if f.suffix.lower() in (".txt", ".pdf") and not f.name.endswith(".ocr.txt")
    ]

    if not files:
        console.print("[yellow]Ingen .txt eller .pdf filer funnet.[/]")
        return

    indexed_ids = db.get_indexed_document_ids()
    new_files = [f for f in files if f.name not in indexed_ids]

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

        for file_path in new_files:
            progress.update(task, description=file_path.name)
            try:
                _index_file(db, embedding, file_path)
            except Exception as e:
                console.print(f"[red]Feil ved {file_path.name}: {e}[/]")
            progress.advance(task)

    console.print("[green]Indeksering fullført![/]")


def _index_file(
    db: DatabaseService, embedding: EmbeddingService, file_path: Path
) -> None:
    text = extract_text(file_path).replace("\0", "")

    if not text.strip():
        console.print(f"[yellow]Tom fil: {file_path.name}[/]")
        return

    chunks = chunk_text(text)
    saksnr = extract_saksnr(file_path)
    embeddings = embedding.get_embeddings(chunks)

    db_chunks = [
        {
            "document_id": file_path.name,
            "chunk_index": i,
            "content": content,
            "embedding": emb,
            "saksnr": saksnr,
            "metadata": {
                "source_path": str(file_path),
                "file_type": file_path.suffix.lstrip("."),
                "chunk_count": len(chunks),
            },
        }
        for i, (content, emb) in enumerate(zip(chunks, embeddings))
    ]

    db.insert_chunks(db_chunks)
