from pathlib import Path

import fitz  # PyMuPDF
import numpy as np

_ocr_reader = None

# Max number of image-only pages to OCR per document. Larger documents
# (e.g. multi-page drawing packages) are skipped to avoid very long runtimes.
_MAX_OCR_PAGES = 25

_TEXT_ENCODINGS = ("utf-8", "windows-1252", "latin-1")


def _get_ocr_reader():
    global _ocr_reader
    if _ocr_reader is None:
        import easyocr

        _ocr_reader = easyocr.Reader(["no", "en"], gpu=True)
    return _ocr_reader


def extract_text(file_path: str | Path) -> str:
    file_path = Path(file_path)
    if file_path.suffix.lower() == ".pdf":
        return _extract_pdf(file_path)
    return _read_text_file(file_path)


def _read_text_file(path: Path) -> str:
    for encoding in _TEXT_ENCODINGS:
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Klarte ikke å lese {path.name} med kjente enkodinger (utf-8, windows-1252, latin-1)")


def _extract_pdf(pdf_path: Path) -> str:
    with open(pdf_path, "rb") as f:
        if f.read(4) != b"%PDF":
            raise ValueError(f"Not a valid PDF (wrong magic bytes): {pdf_path.name}")
    with fitz.open(pdf_path) as doc:
        texts: list[str | None] = []
        needs_ocr = False

        for page in doc:
            text = page.get_text().strip()
            if text:
                texts.append(text)
            else:
                needs_ocr = True
                texts.append(None)

        if needs_ocr:
            ocr_pages = texts.count(None)
            if ocr_pages > _MAX_OCR_PAGES:
                raise ValueError(
                    f"For mange sider for OCR: {ocr_pages} sider uten tekst (maks {_MAX_OCR_PAGES})"
                )
            reader = _get_ocr_reader()
            for i, page in enumerate(doc):
                if texts[i] is not None:
                    continue
                mat = fitz.Matrix(300 / 72, 300 / 72)
                pix = page.get_pixmap(matrix=mat)
                # Ensure RGB (3 channels) — pixmap may be RGBA or grayscale
                if pix.n != 3:
                    pix = fitz.Pixmap(fitz.csRGB, pix)
                img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(
                    pix.height, pix.width, 3
                )
                results = reader.readtext(img, paragraph=True)
                ocr_text = "\n".join(r[1] for r in results)
                texts[i] = ocr_text if ocr_text.strip() else ""

    return "\n\n".join(t for t in texts if t)
