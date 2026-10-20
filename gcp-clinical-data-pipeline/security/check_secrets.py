"""Conservative basic secret/data leak scan; run from repo root."""
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
EXCLUDE = {'.git', '.venv', 'venv', '__pycache__', '.pytest_cache'}
PATTERNS = {
    'private key': re.compile(r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----'),
    'Google API key': re.compile(r'AIza[0-9A-Za-z_-]{35}'),
    'password assignment': re.compile(r'(?i)\b(?:password|passwd|client_secret|api_token|access_token)\s*[:=]\s*[\"\'][^\"\']{8,}[\"\']'),
}
FLAG_EXT = {'.csv', '.xlsx', '.xls', '.parquet', '.pem', '.p12', '.pfx', '.key'}
issues=[]
for path in ROOT.rglob('*'):
    if not path.is_file() or any(part in EXCLUDE for part in path.relative_to(ROOT).parts):
        continue
    if path.suffix.lower() in FLAG_EXT:
        issues.append(f'Data/credential-like file: {path.relative_to(ROOT)}')
        continue
    if path.name == '.env' or (path.name.startswith('.env.') and path.name != '.env.example'):
        issues.append(f'Private environment file: {path.relative_to(ROOT)}')
        continue
    if path.suffix not in {'.py', '.md', '.txt', '.yaml', '.yml', '.json', '.toml', '.sh'}:
        continue
    content=path.read_text(encoding='utf-8',errors='replace')
    if path.name == 'check_secrets.py':
        continue
    for label,pattern in PATTERNS.items():
        if pattern.search(content):
            issues.append(f'Potential {label}: {path.relative_to(ROOT)}')
for issue in issues:
    print('REVIEW:',issue)
print(f'Scan finished: {len(issues)} item(s) flagged. Manual review still required.')
sys.exit(bool(issues))
