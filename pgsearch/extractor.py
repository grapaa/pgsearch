from pathlib import Path

import fitz  # PyMuPDF
import numpy as np

_ocr_reader = None


def _get_ocr_reader():
    global _ocr_reader
    if _ocr_reader is None:
        import easyocr

        _ocr_reader = easyocr.Reader(["no", "en"], gpu=False)
    return _ocr_reader


def extract_text(file_path: str | Path) -> str:
    file_path = Path(file_path)
    if file_path.suffix.lower() == ".pdf":
        return _extract_pdf(file_path)
    return file_path.read_text(encoding="utf-8")


def _extract_pdf(pdf_path: Path) -> str:
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
            reader = _get_ocr_reader()
            for i, page in enumerate(doc):
                if texts[i] is not None:
                    continue
                mat = fitz.Matrix(300 / 72, 300 / 72)
                pix = page.get_pixmap(matrix=mat)
                img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(
                    pix.height, pix.width, 3
                )
                results = reader.readtext(img, paragraph=True)
                ocr_text = "\n".join(r[1] for r in results)
                texts[i] = ocr_text if ocr_text.strip() else ""

    return "\n\n".join(t for t in texts if t)
