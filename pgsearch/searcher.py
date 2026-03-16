from rich.console import Console
from rich.table import Table
from rich import box

from pgsearch.database import DatabaseService
from pgsearch.embedding import EmbeddingService

console = Console()


def search(
    db: DatabaseService, embedding: EmbeddingService, query: str, top_k: int = 10
) -> list[dict]:
    query_embedding = embedding.get_embedding(query)
    return db.hybrid_search(query_embedding, query, top_k)


def display_results(results: list[dict]) -> None:
    if not results:
        console.print("[yellow]Ingen resultater funnet.[/]")
        return

    table = Table(box=box.ROUNDED)
    table.add_column("#")
    table.add_column("Saksnr")
    table.add_column("Dokument")
    table.add_column("Chunk")
    table.add_column("Vektor")
    table.add_column("FTS")
    table.add_column("Meta")
    table.add_column("RRF")
    table.add_column("Innhold", max_width=60)

    for i, r in enumerate(results):
        content = r["content"].replace("\n", " ")
        preview = content[:120] + "..." if len(content) > 120 else content

        table.add_row(
            str(i + 1),
            r.get("saksnr") or "",
            r["document_id"],
            str(r["chunk_index"]),
            f"{r['vector_score']:.4f}",
            f"{r['fts_score']:.4f}",
            f"{r.get('meta_score', 0):.4f}",
            f"[green]{r['rrf_score']:.4f}[/]",
            preview,
        )

    console.print(table)
