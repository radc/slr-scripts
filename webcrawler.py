#!/usr/bin/env python3
"""
Web crawler to download PDFs from a website, restricted to specified domains.
Usage:
    python papers_crawler.py \
        --start-url https://papers.nips.cc \
        --domains papers.nips.cc arxiv.org \
        --exclude 'logout|login' \
        --output-dir pdfs \
        --max-depth 3 \
        --delay 1.0 \
        --overwrite
"""
import argparse
import os
import re
import time
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup


def parse_args():
    parser = argparse.ArgumentParser(
        description='Crawler to download PDFs from a website, restricted to specified domains.'
    )
    parser.add_argument(
        '--start-url', required=True,
        help='Initial URL to start crawling'
    )
    parser.add_argument(
        '--domains', nargs='+', required=True,
        help='List of allowed domains (e.g., papers.nips.cc arxiv.org)'
    )
    parser.add_argument(
        '--exclude', default=None,
        help='Regex pattern to exclude URLs that match'
    )
    parser.add_argument(
        '--output-dir', default='pdfs',
        help='Directory where PDFs will be saved'
    )
    parser.add_argument(
        '--max-depth', type=int, default=3,
        help='Maximum depth for following links'
    )
    parser.add_argument(
        '--delay', type=float, default=1.0,
        help='Delay (in seconds) between requests'
    )
    parser.add_argument(
        '--overwrite', action='store_true',
        help='Overwrite existing files if present'
    )
    return parser.parse_args()


def ensure_output_dir(path):
    os.makedirs(path, exist_ok=True)


def is_allowed_domain(url, domains):
    parsed = urlparse(url)
    return parsed.netloc in domains


def log_warning(output_dir, url, error):
    warning_file = os.path.join(output_dir, 'WARNINGS.txt')
    with open(warning_file, 'a') as wf:
        wf.write(f"Failed to download {url}: {error}\n")


def download_pdf(url, output_dir, overwrite):
    filename = os.path.basename(urlparse(url).path)
    filepath = os.path.join(output_dir, filename)

    # Skip if exists and not overwrite
    if os.path.exists(filepath) and not overwrite:
        print(f"[~] Skipping existing file: {filepath}")
        return

    for attempt in range(1, 3):
        try:
            print(f"[+] Downloading (attempt {attempt}): {url}")
            resp = requests.get(url, stream=True)
            resp.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            return
        except Exception as e:
            print(f"[!] Error on attempt {attempt} for {url}: {e}")
            if attempt == 2:
                log_warning(output_dir, url, e)
            else:
                time.sleep(1)


def crawl(start_url, domains, exclude_pattern, output_dir, max_depth, delay, overwrite):
    visited = set()
    queue = [(start_url, 0)]
    exclude_regex = re.compile(exclude_pattern) if exclude_pattern else None

    while queue:
        url, depth = queue.pop(0)
        if url in visited or depth > max_depth:
            continue
        visited.add(url)

        print(f"[*] Visiting (depth={depth}): {url}")
        try:
            resp = requests.get(url)
            resp.raise_for_status()
        except Exception as e:
            print(f"[!] Error accessing {url}: {e}")
            log_warning(output_dir, url, e)
            continue

        content_type = resp.headers.get('Content-Type', '')
        if 'application/pdf' in content_type or url.lower().endswith('.pdf'):
            download_pdf(url, output_dir, overwrite)
            time.sleep(delay)
            continue

        if 'text/html' not in content_type:
            continue

        soup = BeautifulSoup(resp.text, 'html.parser')
        for tag in soup.find_all('a', href=True):
            href = tag['href']
            full_url = urljoin(url, href)
            normalized = urlparse(full_url)._replace(fragment='').geturl()

            if not is_allowed_domain(normalized, domains):
                continue
            if exclude_regex and exclude_regex.search(normalized):
                print(f"[-] Excluding link by regex pattern (depth={depth}): {normalized}")
                continue

            if normalized.lower().endswith('.pdf'):
                if normalized not in visited:
                    download_pdf(normalized, output_dir, overwrite)
            else:
                if normalized not in visited:
                    queue.append((normalized, depth + 1))

        time.sleep(delay)


def main():
    args = parse_args()
    ensure_output_dir(args.output_dir)
    crawl(
        start_url=args.start_url,
        domains=args.domains,
        exclude_pattern=args.exclude,
        output_dir=args.output_dir,
        max_depth=args.max_depth,
        delay=args.delay,
        overwrite=args.overwrite
    )


if __name__ == '__main__':
    main()