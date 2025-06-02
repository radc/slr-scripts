
#!/usr/bin/env python3
"""
search_pdf.py: Search for terms in PDF files within a directory, supporting Boolean expressions, parallel execution via multiprocessing, and exclusion by filename regex.

Usage:
  python search_pdf.py /path/to/folder -s "EXPR" [-p PROCESSES] [-e EXCLUDE_REGEX]
  python search_pdf.py /path/to/folder -f terms.txt [-p PROCESSES] [-e EXCLUDE_REGEX]

Single-term searches without AND/OR/parentheses are treated as a phrase.
Use -e/--exclude to skip PDF files whose filenames match the given regex.
Use -p/--processes to set number of worker processes (default: CPU count).
"""
import argparse
import os
import glob
import re
import json
import sys
from typing import List, Union, Dict, Tuple
from PyPDF2 import PdfReader
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

# Node types for Boolean expression tree
tree = Union['OpNode', 'TermNode']

class TermNode:
    def __init__(self, term: str):
        self.term = term.strip('"')
        escaped = re.escape(self.term)
        self.pattern = re.compile(escaped, re.IGNORECASE)
    def evaluate(self, text: str) -> bool:
        return bool(self.pattern.search(text))

class OpNode:
    def __init__(self, op: str, children: List[tree]):
        self.op = op
        self.children = children
    def evaluate(self, text: str) -> bool:
        if self.op == 'AND':
            return all(child.evaluate(text) for child in self.children)
        return any(child.evaluate(text) for child in self.children)

# Boolean expression parsing and query loading
def tokenize(expr: str) -> List[str]:
    tokens = []
    i = 0
    while i < len(expr):
        c = expr[i]
        if c.isspace():
            i += 1
            continue
        if c in '()':
            tokens.append(c)
            i += 1
            continue
        if expr[i:i+3].upper() == 'AND' and (i+3 == len(expr) or not expr[i+3].isalpha()):
            tokens.append('AND')
            i += 3
            continue
        if expr[i:i+2].upper() == 'OR' and (i+2 == len(expr) or not expr[i+2].isalpha()):
            tokens.append('OR')
            i += 2
            continue
        if c == '"':
            j = expr.find('"', i+1)
            if j == -1:
                raise ValueError('Unmatched quote in expression')
            tokens.append(expr[i:j+1])
            i = j + 1
            continue
        j = i
        while j < len(expr) and not expr[j].isspace() and expr[j] not in '()':
            j += 1
        tokens.append(expr[i:j])
        i = j
    return tokens

class Parser:
    def __init__(self, tokens: List[str]):
        self.tokens = tokens
        self.pos = 0
    def parse(self) -> tree:
        node = self.parse_or()
        if self.pos < len(self.tokens):
            raise ValueError(f"Unexpected token '{self.tokens[self.pos]}' at position {self.pos}")
        return node
    def parse_or(self) -> tree:
        nodes = [self.parse_and()]
        while self.pos < len(self.tokens) and self.tokens[self.pos] == 'OR':
            self.pos += 1
            nodes.append(self.parse_and())
        return OpNode('OR', nodes) if len(nodes) > 1 else nodes[0]
    def parse_and(self) -> tree:
        nodes = [self.parse_term()]
        while self.pos < len(self.tokens) and self.tokens[self.pos] == 'AND':
            self.pos += 1
            nodes.append(self.parse_term())
        return OpNode('AND', nodes) if len(nodes) > 1 else nodes[0]
    def parse_term(self) -> tree:
        if self.pos >= len(self.tokens):
            raise ValueError("Incomplete expression; expected term or '('")
        token = self.tokens[self.pos]
        if token == '(':  
            self.pos += 1
            node = self.parse_or()
            if self.pos >= len(self.tokens) or self.tokens[self.pos] != ')':
                raise ValueError("Expected ')' to close parentheses")
            self.pos += 1
            return node
        self.pos += 1
        return TermNode(token)

def load_queries(search: str, search_file) -> List[str]:
    # Load search queries, wrap single terms as phrase
    if search_file:
        content = search_file.read().strip()
        try:
            data = json.loads(content)
            queries = data.get('queries', [])
        except json.JSONDecodeError:
            lines = [l.strip() for l in content.splitlines() if l.strip()]
            if len(lines) > 1 and content.startswith('(') and content.endswith(')'):
                queries = [' '.join(lines)]
            else:
                queries = lines
    else:
        txt = search.strip()
        up = txt.upper()
        if not any(op in up for op in (' AND ', ' OR ', '(', ')')):
            queries = [f'"{txt}"']
        else:
            queries = [txt]
    if not queries:
        print('No search terms found.', file=sys.stderr)
        sys.exit(1)
    return queries

def build_trees(queries: List[str]) -> List[tree]:
    trees = []
    for q in queries:
        try:
            tokens = tokenize(q)
            parser = Parser(tokens)
            trees.append(parser.parse())
        except ValueError as e:
            print(f"Error parsing '{q}': {e}", file=sys.stderr)
    return trees

def search_in_pdf(path: str, trees: List[tree], queries: List[str]) -> Tuple[str, List[str]]:
    """Read and search a PDF, return filename and matched queries list."""
    try:
        reader = PdfReader(path)
        text = ''.join(page.extract_text() or '' for page in reader.pages)
        matched = [queries[i] for i, node in enumerate(trees) if node.evaluate(text)]
        return (path, matched)
    except Exception:
        return (path, [])


def main():
    parser = argparse.ArgumentParser(description='Search PDFs with Boolean queries and exclusion')
    parser.add_argument('folder', type=str, help='Folder with PDF files')
    parser.add_argument('-p', '--processes', type=int, default=multiprocessing.cpu_count(),
                        help='Number of processes for parallel execution')
    parser.add_argument('-e', '--exclude', type=str, help='Regex to exclude filenames')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-s', '--search', type=str, help='Single Boolean expression')
    group.add_argument('-f', '--search_file', type=argparse.FileType('r', encoding='utf-8'),
                       help='File with expressions or JSON list')
    args = parser.parse_args()

    if not os.path.isdir(args.folder):
        print(f"'{args.folder}' is not a valid directory", file=sys.stderr)
        sys.exit(1)
    queries = load_queries(args.search, args.search_file)
    trees = build_trees(queries)
    pdfs = glob.glob(os.path.join(args.folder, '*.pdf'))
    if args.exclude:
        pattern = re.compile(args.exclude)
        pdfs = [p for p in pdfs if not pattern.search(os.path.basename(p))]
    total = len(pdfs)
    if total == 0:
        print('No PDF files to process.')
        return

    results: Dict[str, List[str]] = {}
    processed = 0

    with ProcessPoolExecutor(max_workers=args.processes) as executor:
        future_to_pdf = {executor.submit(search_in_pdf, path, trees, queries): path for path in pdfs}
        for future in as_completed(future_to_pdf):
            path, matched = future.result()
            processed += 1
            pct = processed / total * 100
            if matched:
                results[path] = matched
            print(f"Processed {os.path.basename(path)} ({processed}/{total}, {pct:.1f}%): {len(matched)} matches")

    print("\nFinal Results:")
    if results:
        for path, terms in results.items():
            print(f"\nFile: {path}")
            for term in terms:
                print(f"  - {term}")
    else:
        print('No matching terms in PDFs.')

if __name__ == '__main__':
    main()
