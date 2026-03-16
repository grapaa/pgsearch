import json
from pathlib import Path

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn

from pgsearch.database import DatabaseService

console = Console()


def load_metadata(db: DatabaseService, directory: str):
    dir_path = Path(directory)
    jsonl_files = list(dir_path.rglob("*.jsonl"))

    if not jsonl_files:
        console.print("[yellow]Ingen .jsonl filer funnet.[/]")
        return

    saker: dict[str, dict] = {}

    for jsonl_file in jsonl_files:
        for line in jsonl_file.open(encoding="utf-8-sig"):
            if not line.strip():
                continue
            record = json.loads(line)
            saksnr = record["saksnr"]
            if saksnr not in saker:
                metadata = {k: v for k, v in record.items() if k != "saksnr"}
                saker[saksnr] = {
                    "saksnr": saksnr,
                    "metadata": metadata,
                }

    console.print(f"Fant [green]{len(saker)}[/] unike saker fra {len(jsonl_files)} JSONL-fil(er).")

    with Progress(
        SpinnerColumn(),
        TextColumn("[cyan]{task.description}[/]"),
        BarColumn(),
        MofNCompleteColumn(),
        console=console,
    ) as progress:
        batch = list(saker.values())
        task = progress.add_task("Laster byggesaker", total=len(batch))
        chunk_size = 100
        for i in range(0, len(batch), chunk_size):
            db.upsert_byggesaker(batch[i:i + chunk_size])
            progress.advance(task, advance=min(chunk_size, len(batch) - i))

    console.print(f"[green]Lastet {len(saker)} byggesaker inn i databasen.[/]")
