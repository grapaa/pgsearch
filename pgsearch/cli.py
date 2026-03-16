from datetime import datetime, date
from pathlib import Path

from rich.console import Console
from rich.prompt import IntPrompt, Prompt
from rich.table import Table
from rich import box

from pgsearch.config import get_config
from pgsearch.database import DatabaseService
from pgsearch.embedding import EmbeddingService
from pgsearch.indexer import index_directory
from pgsearch.innsyn.pipeline import run_daily_pipeline, run_range_pipeline
from pgsearch.metadata_loader import load_metadata
from pgsearch.searcher import search, display_results

console = Console()

MENU_CHOICES = [
    "1. Sett opp database",
    "2. Indekser dokumenter",
    "3. Hybrid-søk",
    "4. Vis statistikk",
    "5. Last inn byggesak-metadata",
    "6. Oppdater saksnr på eksisterende chunks",
    "7. Indekser byggesaker fra Innsyn",
    "8. Avslutt",
]


def main():
    config = get_config()
    db = DatabaseService(config.db_connection)
    embedding = EmbeddingService(
        endpoint=config.azure_openai_endpoint,
        api_key=config.azure_openai_api_key,
        deployment=config.azure_openai_deployment,
    )

    while True:
        console.print()
        console.print("[bold blue]PgSearch — Hybrid RAG med pgvector[/]")
        for c in MENU_CHOICES:
            console.print(f"  {c}")

        choice = Prompt.ask(
            "Velg", choices=["1", "2", "3", "4", "5", "6", "7", "8"], default="3"
        )

        match choice:
            case "1":
                _setup_database(db)
            case "2":
                _index_documents(db, embedding)
            case "3":
                _search(db, embedding)
            case "4":
                _show_statistics(db)
            case "5":
                _load_metadata(db)
            case "6":
                _backfill_saksnr(db)
            case "7":
                _index_byggesaker(db, embedding)
            case "8":
                break


def _setup_database(db: DatabaseService) -> None:
    with console.status("Setter opp database..."):
        db.setup()
    console.print("[green]Database satt opp med pgvector extension, tabell og indekser.[/]")


def _index_documents(db: DatabaseService, embedding: EmbeddingService) -> None:
    default_path = str(Path.cwd() / "SampleData")
    directory = Prompt.ask("Sti til mappe med dokumenter", default=default_path)

    if not Path(directory).is_dir():
        console.print(f"[red]Mappen finnes ikke: {directory}[/]")
        return

    index_directory(db, embedding, directory)


def _search(db: DatabaseService, embedding: EmbeddingService) -> None:
    query = Prompt.ask("Søketekst")
    if not query or not query.strip():
        return

    top_k = IntPrompt.ask("Antall resultater", default=5)

    with console.status("Søker..."):
        results = search(db, embedding, query, top_k)

    display_results(results)


def _load_metadata(db: DatabaseService) -> None:
    default_path = str(Path.cwd() / "SampleData" / "processed")
    directory = Prompt.ask("Sti til mappe med JSONL-filer", default=default_path)

    if not Path(directory).is_dir():
        console.print(f"[red]Mappen finnes ikke: {directory}[/]")
        return

    load_metadata(db, directory)


def _backfill_saksnr(db: DatabaseService) -> None:
    with console.status("Oppdaterer saksnr på eksisterende chunks..."):
        count = db.backfill_saksnr()
    console.print(f"[green]Oppdaterte {count} chunks med saksnr.[/]")


def _index_byggesaker(db: DatabaseService, embedding: EmbeddingService) -> None:
    console.print("Skriv dato (dd.MM.yyyy) eller datoperiode (dd.MM.yyyy-dd.MM.yyyy):")
    date_input = Prompt.ask("Dato").strip()
    data_dir = Path.cwd() / "data"

    if "-" in date_input:
        parts = date_input.split("-", 1)
        try:
            from_date = datetime.strptime(parts[0].strip(), "%d.%m.%Y").date()
            to_date = datetime.strptime(parts[1].strip(), "%d.%m.%Y").date()
        except ValueError:
            console.print("[red]Ugyldig datoformat. Bruk dd.MM.yyyy-dd.MM.yyyy[/]")
            return
        run_range_pipeline(db, embedding, from_date, to_date, data_dir)
    else:
        try:
            target_date = datetime.strptime(date_input, "%d.%m.%Y").date()
        except ValueError:
            console.print("[red]Ugyldig datoformat. Bruk dd.MM.yyyy[/]")
            return
        run_daily_pipeline(db, embedding, target_date, data_dir)


def _show_statistics(db: DatabaseService) -> None:
    stats = db.get_statistics()

    table = Table(box=box.ROUNDED)
    table.add_column("Metrikk")
    table.add_column("Verdi")
    table.add_row("Dokumenter", str(stats["documents"]))
    table.add_row("Chunks", str(stats["chunks"]))

    console.print(table)


if __name__ == "__main__":
    main()
