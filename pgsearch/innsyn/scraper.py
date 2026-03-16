import re
from datetime import date, datetime, timedelta

import httpx
from rich.console import Console

from .models import ByggesakDokument, MerInfo, Vedlegg

console = Console()

BASE_URL = "https://innsyn2020.drammen.kommune.no"
BYGGSAK_LIST_ID = "450d153d-62f7-4564-ab1b-60370477c471"
PAGE_SIZE = 50
HTTP_TIMEOUT = 120

_SEARCH_QUERY = (
    "query FetchMoreJournals($journalsLimit: Int!, $journalsOffset: Int, "
    "$journalsWhere: SearchJournalsWhere!, "
    "$journalProceedingWhere: JournalProceedingWhere, "
    "$journalDocumentsWhere: JournalDocumentsWhere!, "
    "$journalsOrderBy: SearchJournalsOrderBy) {\n"
    "  journals: searchJournals(\n"
    "    limit: $journalsLimit\n"
    "    offset: $journalsOffset\n"
    "    where: $journalsWhere\n"
    "    proceedingWhere: $journalProceedingWhere\n"
    "    orderBy: $journalsOrderBy\n"
    "  ) {\n"
    "    nodes {\n"
    "      ...JournalResult\n"
    "      __typename\n"
    "    }\n"
    "    __typename\n"
    "  }\n"
    "}\n"
    "\n"
    "fragment JournalResult on Journal {\n"
    "  id\n"
    "  archiveId\n"
    "  journalDate\n"
    "  classified\n"
    "  documentDate\n"
    "  title\n"
    "  sequenceNumber\n"
    "  caseworkers\n"
    "  senders\n"
    "  unpublished\n"
    "  recipients\n"
    "  mainDocumentNotPublishedReason\n"
    "  archiveSystem {\n"
    "    id\n"
    "    name\n"
    "    __typename\n"
    "  }\n"
    "  department {\n"
    "    id\n"
    "    name\n"
    "    __typename\n"
    "  }\n"
    "  status {\n"
    "    id\n"
    "    description\n"
    "    name\n"
    "    __typename\n"
    "  }\n"
    "  subArchive {\n"
    "    id\n"
    "    name\n"
    "    __typename\n"
    "  }\n"
    "  type {\n"
    "    id\n"
    "    name\n"
    "    description\n"
    "    __typename\n"
    "  }\n"
    "  documents(where: $journalDocumentsWhere) {\n"
    "    id\n"
    "    classified\n"
    "    title\n"
    "    order\n"
    "    type {\n"
    "      id\n"
    "      name\n"
    "      __typename\n"
    "    }\n"
    "    __typename\n"
    "  }\n"
    "  proceeding {\n"
    "    id\n"
    "    sequenceNumber\n"
    "    type {\n"
    "      id\n"
    "      name\n"
    "      __typename\n"
    "    }\n"
    "    subArchive {\n"
    "      id\n"
    "      name\n"
    "      __typename\n"
    "    }\n"
    "    propertyIdentifications {\n"
    "      id\n"
    "      useNr\n"
    "      propertyNr\n"
    "      __typename\n"
    "    }\n"
    "    __typename\n"
    "  }\n"
    "  __typename\n"
    "}"
)


def fetch_byggesaker(target_date: date) -> list[ByggesakDokument]:
    """Fetch all byggesaker for a single date via GraphQL API."""
    date_str = target_date.strftime("%Y-%m-%d")
    graphql_url = f"{BASE_URL}/graphql"
    all_docs: list[ByggesakDokument] = []
    offset = 0

    with httpx.Client(timeout=HTTP_TIMEOUT) as client:
        while True:
            console.print(f"  Henter offset {offset} for {date_str}...")
            variables = {
                "journalsLimit": PAGE_SIZE,
                "journalsOffset": offset,
                "journalsWhere": {
                    "listId": BYGGSAK_LIST_ID,
                    "typeIdIn": [],
                    "search": "",
                    "journalFromDate": date_str,
                    "journalToDate": date_str,
                    "subArchiveId": "",
                    "departmentIdIn": None,
                    "includeUnpublished": False,
                },
                "journalDocumentsWhere": {
                    "includeUnpublished": False,
                    "listId": BYGGSAK_LIST_ID,
                },
                "journalProceedingWhere": {"typeIdIn": []},
                "journalsOrderBy": "journalDate_DESC",
            }

            resp = client.post(
                graphql_url,
                json={
                    "operationName": "FetchMoreJournals",
                    "query": _SEARCH_QUERY.strip(),
                    "variables": variables,
                },
                headers={
                    "Content-Type": "application/json",
                    "Apollo-Require-Preflight": "true",
                },
            )
            resp.raise_for_status()
            data = resp.json()

            if "errors" in data:
                errors = data["errors"]
                error_msg = errors[0].get("message", "Unknown error") if errors else "Unknown error"
                raise RuntimeError(f"GraphQL feil ved offset {offset}: {error_msg}")

            journals = data.get("data", {}).get("journals", {})
            nodes = journals.get("nodes", [])

            if not nodes:
                break

            docs = [_journal_to_dokument(node, target_date) for node in nodes]
            all_docs.extend(docs)

            if len(nodes) < PAGE_SIZE:
                break

            offset += PAGE_SIZE

    console.print(f"  Fant {len(all_docs)} journalposter")
    return all_docs


def fetch_byggesaker_range(from_date: date, to_date: date) -> list[ByggesakDokument]:
    """Fetch byggesaker for each date in a range."""
    all_docs: list[ByggesakDokument] = []
    current = from_date
    while current <= to_date:
        console.print(f"[cyan]Dato: {current.strftime('%d.%m.%Y')}[/cyan]")
        docs = fetch_byggesaker(current)
        all_docs.extend(docs)
        current += timedelta(days=1)
    return all_docs


def _journal_to_dokument(node: dict, target_date: date) -> ByggesakDokument:
    """Convert a GraphQL journal node to a ByggesakDokument."""
    proceeding = node.get("proceeding") or {}
    property_ids = proceeding.get("propertyIdentifications") or []
    gnr_bnr = ""
    if property_ids:
        pi = property_ids[0]
        gnr_bnr = f"{pi.get('propertyNr', '')}/{pi.get('useNr', '')}"

    sakstype = ""
    proc_type = proceeding.get("type") or {}
    if proc_type:
        sakstype = proc_type.get("name", "")

    journal_type = node.get("type") or {}
    status = node.get("status") or {}

    senders = node.get("senders") or []
    recipients = node.get("recipients") or []
    avsender_mottaker = ", ".join(senders + recipients)

    caseworkers = node.get("caseworkers") or []
    saksbehandler = ", ".join(caseworkers)

    not_published_reason = node.get("mainDocumentNotPublishedReason")
    er_tilgjengelig = not_published_reason is None or not_published_reason == ""

    raw_docs = node.get("documents") or []
    vedlegg = []
    dokument_url = ""
    for i, raw_doc in enumerate(raw_docs):
        doc_id = raw_doc.get("id", "")
        if raw_doc.get("order", 0) == 0 or i == 0:
            dokument_url = f"/file/{doc_id}" if doc_id else ""
        else:
            vedlegg.append(Vedlegg(
                nummer=i,
                navn=raw_doc.get("title", ""),
                dokument_id=doc_id,
                url=f"/file/{doc_id}" if doc_id else "",
            ))

    saksnr = proceeding.get("sequenceNumber", "") or ""
    dato_raw = node.get("journalDate") or node.get("documentDate") or ""
    dato = target_date.strftime("%d.%m.%Y")
    if dato_raw:
        try:
            dt = datetime.fromisoformat(dato_raw.replace("Z", "+00:00"))
            dato = dt.strftime("%d.%m.%Y")
        except (ValueError, TypeError):
            pass

    dokument_status = status.get("name", "")

    return ByggesakDokument(
        saksnr=str(saksnr),
        dato=dato,
        gnr_bnr=gnr_bnr,
        sakstype=sakstype,
        beskrivelse=node.get("title", ""),
        avsender_mottaker=avsender_mottaker,
        saksbehandler=saksbehandler,
        dokument_status=dokument_status,
        dokument_url=dokument_url,
        er_tilgjengelig=er_tilgjengelig,
        vedlegg=vedlegg,
        mer_info=MerInfo(
            status=dokument_status,
            enhet=(node.get("department") or {}).get("name", ""),
            brevdato=dato,
            type=journal_type.get("name", ""),
        ),
    )
