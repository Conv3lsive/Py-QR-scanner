import os
import tempfile
from typing import List


def pdf_to_images(pdf_path: str) -> List[str]:
    try:
        from pdf2image import convert_from_path
    except Exception as exc:
        raise RuntimeError(
            'Для обработки PDF установите pdf2image и poppler (на macOS: brew install poppler).'
        ) from exc

    output_dir = tempfile.mkdtemp(prefix='qr_pdf_')
    images = convert_from_path(pdf_path)
    result_paths = []

    for idx, img in enumerate(images, start=1):
        path = os.path.join(output_dir, f'{os.path.basename(pdf_path)}_page_{idx}.jpg')
        img.save(path, 'JPEG')
        result_paths.append(path)

    return result_paths
