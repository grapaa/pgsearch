import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()

_REQUIRED = ["PGSEARCH_DB_CONNECTION", "AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_API_KEY"]


@dataclass(frozen=True)
class Config:
    db_connection: str
    azure_openai_endpoint: str
    azure_openai_api_key: str
    azure_openai_deployment: str = "text-embedding-3-small"


def get_config() -> Config:
    missing = [v for v in _REQUIRED if not os.environ.get(v)]
    if missing:
        raise SystemExit(
            f"Mangler påkrevde miljøvariabler: {', '.join(missing)}\n"
            "Sjekk at .env-filen finnes og inneholder disse variablene."
        )
    return Config(
        db_connection=os.environ["PGSEARCH_DB_CONNECTION"],
        azure_openai_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
        azure_openai_api_key=os.environ["AZURE_OPENAI_API_KEY"],
        azure_openai_deployment=os.environ.get(
            "AZURE_OPENAI_DEPLOYMENT", "text-embedding-3-small"
        ),
    )
