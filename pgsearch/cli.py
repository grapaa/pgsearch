import logging
from datetime import datetime, date
from pathlib import Path

from rich.console import Console
from rich.prompt import IntPrompt, Prompt
from rich.table import Table
from rich import box

from pgsearch.config import get_config
from pgsearch.database import DatabaseService
from pgsearch.embedding import EmbeddingService
from pgsearch.innsyn.pipeline import run_daily_pipeline, run_range_pipeline
from pgsearch.searcher import search, display_results

console = Console()

logging.getLogger("pgsearch").setLevel(logging.INFO)
log = logging.getLogger("pgsearch")

MENU_CHOICES = [
    "1. Sett opp database",
    "2. Hybrid-søk",
    "3. Vis statistikk",
    "4. Indekser byggesaker fra Innsyn",
    "5. Avslutt",
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

        try:
            choice = Prompt.ask(
                "Velg", choices=["1", "2", "3", "4", "5"], default="2"
            )
        except (KeyboardInterrupt, EOFError):
            console.print("\n[yellow]Avslutter.[/]")
            break

        try:
            match choice:
                case "1":
                    _setup_database(db)
                case "2":
                    _search(db, embedding)
                case "3":
                    _show_statistics(db)
                case "4":
                    _index_byggesaker(db, embedding)
                case "5":
                    break
        except KeyboardInterrupt:
            console.print("\n[yellow]Avbrutt.[/]")
            log.info("Avbrutt av bruker (Ctrl-C)")
        except Exception as e:
            log.exception("Uventet feil")
            console.print(f"[red]Feil: {e}[/]")


def _setup_database(db: DatabaseService) -> None:
    with console.status("Setter opp database..."):
        db.setup()
    console.print("[green]Database satt opp med pgvector extension, tabell og indekser.[/]")


def _search(db: DatabaseService, embedding: EmbeddingService) -> None:
    query = Prompt.ask("Søketekst")
    if not query or not query.strip():
        return

    top_k = IntPrompt.ask("Antall resultater", default=5)

    with console.status("Søker..."):
        results = search(db, embedding, query, top_k)

    display_results(results)


def _show_statistics(db: DatabaseService) -> None:
    stats = db.get_statistics()

    table = Table(box=box.ROUNDED)
    table.add_column("Metrikk")
    table.add_column("Verdi")
    table.add_row("Dokumenter", str(stats["documents"]))
    table.add_row("Chunks", str(stats["chunks"]))

    console.print(table)


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
        if from_date > to_date:
            console.print("[red]Fra-dato må være før eller lik til-dato.[/]")
            return
        run_range_pipeline(db, embedding, from_date, to_date, data_dir)
    else:
        try:
            target_date = datetime.strptime(date_input, "%d.%m.%Y").date()
        except ValueError:
            console.print("[red]Ugyldig datoformat. Bruk dd.MM.yyyy[/]")
            return
        run_daily_pipeline(db, embedding, target_date, data_dir)


if __name__ == "__main__":
    main()
