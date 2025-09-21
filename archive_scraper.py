import requests
from urllib.parse import urlparse

ARCHIVE_METADATA_URL = "https://archive.org/metadata/{identifier}"

def parse_archive_url(url: str):
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split('/') if p]
    if len(parts) >= 2 and parts[0] == 'details':
        return parts[1]
    return parts[-1] if parts else None

def fetch_metadata(identifier: str):
    resp = requests.get(ARCHIVE_METADATA_URL.format(identifier=identifier), timeout=20)
    resp.raise_for_status()
    return resp.json()

def list_files_from_metadata(meta: dict):
    files = meta.get('files', [])
    out = []
    for f in files:
        name = f.get('name')
        if not name:
            continue
        size = f.get('size') or f.get('original') or ''
        format_ = f.get('format') or f.get('format-label') or ''
        out.append({
            'name': name,
            'format': format_,
            'size': size,
            'source': f.get('source'),
            'md5': f.get('md5')
        })
    return out
