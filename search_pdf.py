#!/usr/bin/env python3
"""
search_pdf.py: Search for terms in PDF files within a directory, supporting complex Boolean expressions (AND, OR, parentheses), with optional parallel execution.

Usage:
  python search_pdf.py /path/to/folder -s "EXPR" [-t THREADS]
  python search_pdf.py /path/to/folder -f terms.txt [-t THREADS]

Single-term searches without AND/OR/parentheses will be treated as a phrase.
"""
import argparse
import os
import glob
import re
import json
import sys
from typing import List, Union, Dict
from PyPDF2 import PdfReader
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

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

# Boolean expression parsing functions

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
        elif expr[i:i+3].upper() == 'AND' and (i+3 == len(expr) or not expr[i+3].isalpha()):
            tokens.append('AND')
            i += 3
        elif expr[i:i+2].upper() == 'OR' and (i+2 == len(expr) or not expr[i+2].isalpha()):
            tokens.append('OR')
            i += 2
        elif c == '"':
            j = expr.find('"', i+1)
            if j == -1:
                raise ValueError('Unmatched quote in expression')
            tokens.append(expr[i:j+1])
            i = j + 1
        else:
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
    if search_file:
        content = search_file.read().strip()
        try:
            data = json.loads(content)
            queries = data.get('queries', [])
        except json.JSONDecodeError:
            lines = [line.strip() for line in content.splitlines() if line.strip()]
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
            print(f"Error parsing query '{q}': {e}", file=sys.stderr)
    return trees


def search_in_pdf(path: str, trees: List[tree], queries: List[str]) -> Dict[str, List[str]]:
    try:
        reader = PdfReader(path)
    except Exception as e:
        print(f"Error reading {path}: {e}", file=sys.stderr)
        return {}
    text = ''.join(page.extract_text() or '' for page in reader.pages)
    matched = [queries[i] for i, node in enumerate(trees) if node.evaluate(text)]
    return {path: matched} if matched else {}


def main():
    parser = argparse.ArgumentParser(description='Search PDFs with Boolean queries')
    parser.add_argument('folder', type=str, help='Folder with PDF files')
    parser.add_argument('-t', '--threads', type=int, default=1, help='Number of threads')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-s', '--search', type=str, help='Single Boolean expression')
    group.add_argument('-f', '--search_file', type=argparse.FileType('r', encoding='utf-8'), help='File with expressions or JSON list')
    args = parser.parse_args()

    if not os.path.isdir(args.folder):
        print(f"'{args.folder}' is not a valid directory", file=sys.stderr)
        sys.exit(1)
    queries = load_queries(args.search, args.search_file)
    trees = build_trees(queries)
    pdfs = glob.glob(os.path.join(args.folder, '*.pdf'))
    total = len(pdfs)
    if total == 0:
        print('No PDF files found.')
        return

    results: Dict[str, List[str]] = {}
    lock = threading.Lock()
    processed = 0

    def worker(path: str):
        nonlocal processed
        res = search_in_pdf(path, trees, queries)
        with lock:
            processed += 1
            pct = (processed / total) * 100
            cur_matches = len(results)
            new_matches = len(res)
            print(f"Processing: {path} ({processed}/{total}, {pct:.1f}% complete) - Matches so far: {cur_matches + new_matches}")
            if res:
                results.update(res)

    if args.threads > 1:
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            futures = [executor.submit(worker, p) for p in pdfs]
            for _ in as_completed(futures):
                pass
    else:
        for p in pdfs:
            worker(p)

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
