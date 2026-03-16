import re


def chunk_text(text: str, max_chunk_size: int = 1000, overlap: int = 200) -> list[str]:
    if not text or not text.strip():
        return []

    paragraphs = re.split(r"\r?\n\r?\n", text)
    paragraphs = [p.strip() for p in paragraphs if p.strip()]

    chunks = []
    current = ""

    for para in paragraphs:
        if current and len(current) + len(para) + 1 > max_chunk_size:
            chunks.append(current.strip())
            if overlap > 0 and len(current) > overlap:
                current = current[-overlap:].strip() + "\n\n" + para
            else:
                current = para
        else:
            current = current + "\n\n" + para if current else para

    if current and current.strip():
        chunks.append(current.strip())

    # Splitt chunks som er for lange
    result = []
    for chunk in chunks:
        if len(chunk) <= max_chunk_size:
            result.append(chunk)
        else:
            i = 0
            while i < len(chunk):
                end = min(i + max_chunk_size, len(chunk))
                result.append(chunk[i:end].strip())
                i += max_chunk_size - overlap

    return result
