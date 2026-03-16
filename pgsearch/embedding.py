from openai import AzureOpenAI

BATCH_SIZE = 16


class EmbeddingService:
    def __init__(self, endpoint: str, api_key: str, deployment: str):
        self._client = AzureOpenAI(
            azure_endpoint=endpoint,
            api_key=api_key,
            api_version="2024-02-01",
            max_retries=5,  # exponential backoff on 429/503
        )
        self._deployment = deployment

    def get_embedding(self, text: str) -> list[float]:
        response = self._client.embeddings.create(
            input=text, model=self._deployment
        )
        return response.data[0].embedding

    def get_embeddings(self, texts: list[str]) -> list[list[float]]:
        results: list[list[float]] = []
        for i in range(0, len(texts), BATCH_SIZE):
            batch = texts[i : i + BATCH_SIZE]
            response = self._client.embeddings.create(
                input=batch, model=self._deployment
            )
            results.extend(item.embedding for item in response.data)
        return results
