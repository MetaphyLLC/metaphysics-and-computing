#!/usr/bin/env python3
"""
TextTransform - Universal Text Transformation & Encoding Toolkit

A zero-dependency CLI tool for encoding, decoding, hashing, and transforming text
from the command line. Supports 30+ transformations including Base64, URL encoding,
hex conversion, hashing (MD5/SHA-1/SHA-256/SHA-512), case conversion, line
operations, whitespace manipulation, and JSON/string formatting.

Designed for developers, AI agents, and system administrators who need fast, reliable
text transformations without installing extra dependencies or opening a browser.

Works with piped input, file input, or direct arguments. Chainable via pipes.

Author: ATLAS (Team Brain)
For: Logan Smith / Metaphy LLC
Version: 1.0.0
Date: 2026-03-10
License: MIT
"""

import argparse
import base64
import hashlib
import html
import json
import re
import string
import sys
import urllib.parse
from pathlib import Path
from typing import Optional


# ============================================================================
# VERSION
# ============================================================================

__version__ = "1.0.0"


# ============================================================================
# CORE TRANSFORMATION ENGINE
# ============================================================================

class TextTransformer:
    """
    Core text transformation engine.

    Provides 30+ transformation methods organized into categories:
    - Encoding: base64, hex, URL, HTML, binary
    - Hashing: md5, sha1, sha256, sha512
    - Case: upper, lower, title, swap, camel, snake, kebab, pascal
    - Lines: sort, unique, reverse, count, number, grep, head, tail
    - Format: wrap, indent, trim, squeeze, strip, truncate
    - Escape: json-escape, json-unescape, backslash-escape
    - Misc: rot13, reverse, repeat, pad-left, pad-right, length

    Example:
        >>> t = TextTransformer()
        >>> t.transform("hello world", "upper")
        'HELLO WORLD'
        >>> t.transform("aGVsbG8=", "base64-decode")
        'hello'
    """

    def __init__(self):
        self._commands = self._build_command_map()

    def _build_command_map(self) -> dict:
        return {
            # Encoding
            "base64-encode": self._base64_encode,
            "base64-decode": self._base64_decode,
            "hex-encode": self._hex_encode,
            "hex-decode": self._hex_decode,
            "url-encode": self._url_encode,
            "url-decode": self._url_decode,
            "html-encode": self._html_encode,
            "html-decode": self._html_decode,
            "binary-encode": self._binary_encode,
            "binary-decode": self._binary_decode,
            # Hashing
            "md5": self._md5,
            "sha1": self._sha1,
            "sha256": self._sha256,
            "sha512": self._sha512,
            # Case
            "upper": lambda t, _: t.upper(),
            "lower": lambda t, _: t.lower(),
            "title": lambda t, _: t.title(),
            "swapcase": lambda t, _: t.swapcase(),
            "camel": self._to_camel,
            "snake": self._to_snake,
            "kebab": self._to_kebab,
            "pascal": self._to_pascal,
            # Lines
            "sort": self._sort_lines,
            "sort-reverse": self._sort_reverse,
            "unique": self._unique_lines,
            "reverse": self._reverse_lines,
            "count": self._count_lines,
            "number": self._number_lines,
            "grep": self._grep_lines,
            "grep-v": self._grep_v_lines,
            "head": self._head_lines,
            "tail": self._tail_lines,
            "trim-lines": self._trim_lines,
            "remove-empty": self._remove_empty_lines,
            # Format
            "trim": lambda t, _: t.strip(),
            "squeeze": self._squeeze_whitespace,
            "wrap": self._wrap_text,
            "indent": self._indent_text,
            "dedent": self._dedent_text,
            "truncate": self._truncate,
            "pad-left": self._pad_left,
            "pad-right": self._pad_right,
            # Escape
            "json-escape": self._json_escape,
            "json-unescape": self._json_unescape,
            "backslash-escape": self._backslash_escape,
            "backslash-unescape": self._backslash_unescape,
            # JSON
            "json-pretty": self._json_pretty,
            "json-minify": self._json_minify,
            "json-keys": self._json_keys,
            # Misc
            "rot13": self._rot13,
            "reverse-text": self._reverse_text,
            "repeat": self._repeat,
            "length": self._length,
            "words": self._word_count,
            "chars": self._char_count,
            "lines-count": self._lines_count,
            "slug": self._to_slug,
            "remove-punct": self._remove_punctuation,
            "remove-digits": self._remove_digits,
            "remove-whitespace": self._remove_whitespace,
            "extract-emails": self._extract_emails,
            "extract-urls": self._extract_urls,
            "extract-ips": self._extract_ips,
            "extract-digits": self._extract_digits,
        }

    @property
    def commands(self) -> list:
        """Return sorted list of all available command names."""
        return sorted(self._commands.keys())

    def transform(self, text: str, command: str, arg: Optional[str] = None) -> str:
        """
        Apply a named transformation to text.

        Args:
            text: Input text to transform
            command: Transformation name (e.g., 'upper', 'base64-encode')
            arg: Optional argument for parameterized transforms

        Returns:
            Transformed text string

        Raises:
            ValueError: If command is unknown
            RuntimeError: If transformation fails (e.g., invalid base64)

        Example:
            >>> t = TextTransformer()
            >>> t.transform("Hello World", "snake")
            'hello_world'
        """
        if not isinstance(text, str):
            raise TypeError(f"text must be str, got {type(text).__name__}")
        if command not in self._commands:
            raise ValueError(
                f"Unknown command: '{command}'. Use 'list' to see available commands."
            )
        try:
            return self._commands[command](text, arg)
        except (UnicodeDecodeError, Exception) as e:
            raise RuntimeError(f"Transform '{command}' failed: {e}") from e

    # --- ENCODING ---

    def _base64_encode(self, text: str, _) -> str:
        return base64.b64encode(text.encode("utf-8")).decode("ascii")

    def _base64_decode(self, text: str, _) -> str:
        text = text.strip()
        padding = 4 - (len(text) % 4)
        if padding != 4:
            text += "=" * padding
        return base64.b64decode(text).decode("utf-8")

    def _hex_encode(self, text: str, _) -> str:
        return text.encode("utf-8").hex()

    def _hex_decode(self, text: str, _) -> str:
        text = text.strip().replace(" ", "")
        return bytes.fromhex(text).decode("utf-8")

    def _url_encode(self, text: str, _) -> str:
        return urllib.parse.quote(text, safe="")

    def _url_decode(self, text: str, _) -> str:
        return urllib.parse.unquote(text)

    def _html_encode(self, text: str, _) -> str:
        return html.escape(text)

    def _html_decode(self, text: str, _) -> str:
        return html.unescape(text)

    def _binary_encode(self, text: str, _) -> str:
        return " ".join(format(ord(c), "08b") for c in text)

    def _binary_decode(self, text: str, _) -> str:
        text = text.strip()
        bits = text.split()
        return "".join(chr(int(b, 2)) for b in bits)

    # --- HASHING ---

    def _md5(self, text: str, _) -> str:
        return hashlib.md5(text.encode("utf-8")).hexdigest()

    def _sha1(self, text: str, _) -> str:
        return hashlib.sha1(text.encode("utf-8")).hexdigest()

    def _sha256(self, text: str, _) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    def _sha512(self, text: str, _) -> str:
        return hashlib.sha512(text.encode("utf-8")).hexdigest()

    # --- CASE ---

    def _to_camel(self, text: str, _) -> str:
        words = re.split(r"[\s_\-]+", text.strip())
        if not words:
            return text
        result = words[0].lower()
        for w in words[1:]:
            result += w.capitalize() if w else ""
        return result

    def _to_snake(self, text: str, _) -> str:
        text = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", text)
        text = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", text)
        text = re.sub(r"[\s\-]+", "_", text)
        return text.lower()

    def _to_kebab(self, text: str, _) -> str:
        return self._to_snake(text, None).replace("_", "-")

    def _to_pascal(self, text: str, _) -> str:
        words = re.split(r"[\s_\-]+", text.strip())
        return "".join(w.capitalize() for w in words if w)

    def _to_slug(self, text: str, _) -> str:
        text = text.lower().strip()
        text = re.sub(r"[^\w\s-]", "", text)
        text = re.sub(r"[\s_-]+", "-", text)
        return text.strip("-")

    # --- LINES ---

    def _sort_lines(self, text: str, _) -> str:
        return "\n".join(sorted(text.splitlines()))

    def _sort_reverse(self, text: str, _) -> str:
        return "\n".join(sorted(text.splitlines(), reverse=True))

    def _unique_lines(self, text: str, _) -> str:
        seen = []
        for line in text.splitlines():
            if line not in seen:
                seen.append(line)
        return "\n".join(seen)

    def _reverse_lines(self, text: str, _) -> str:
        return "\n".join(reversed(text.splitlines()))

    def _count_lines(self, text: str, _) -> str:
        return str(len(text.splitlines()))

    def _number_lines(self, text: str, _) -> str:
        lines = text.splitlines()
        width = len(str(len(lines)))
        return "\n".join(f"{i + 1:>{width}}  {line}" for i, line in enumerate(lines))

    def _grep_lines(self, text: str, pattern: Optional[str]) -> str:
        if not pattern:
            raise ValueError("grep requires --arg PATTERN")
        return "\n".join(line for line in text.splitlines() if re.search(pattern, line))

    def _grep_v_lines(self, text: str, pattern: Optional[str]) -> str:
        if not pattern:
            raise ValueError("grep-v requires --arg PATTERN")
        return "\n".join(
            line for line in text.splitlines() if not re.search(pattern, line)
        )

    def _head_lines(self, text: str, arg: Optional[str]) -> str:
        n = int(arg) if arg and arg.isdigit() else 10
        return "\n".join(text.splitlines()[:n])

    def _tail_lines(self, text: str, arg: Optional[str]) -> str:
        n = int(arg) if arg and arg.isdigit() else 10
        return "\n".join(text.splitlines()[-n:])

    def _trim_lines(self, text: str, _) -> str:
        return "\n".join(line.strip() for line in text.splitlines())

    def _remove_empty_lines(self, text: str, _) -> str:
        return "\n".join(line for line in text.splitlines() if line.strip())

    # --- FORMAT ---

    def _squeeze_whitespace(self, text: str, _) -> str:
        return re.sub(r"[ \t]+", " ", text).strip()

    def _wrap_text(self, text: str, arg: Optional[str]) -> str:
        import textwrap
        width = int(arg) if arg and arg.isdigit() else 80
        paragraphs = text.split("\n\n")
        wrapped = []
        for para in paragraphs:
            para = para.replace("\n", " ").strip()
            if para:
                wrapped.append(textwrap.fill(para, width=width))
        return "\n\n".join(wrapped)

    def _indent_text(self, text: str, arg: Optional[str]) -> str:
        prefix = arg if arg else "    "
        return "\n".join(prefix + line for line in text.splitlines())

    def _dedent_text(self, text: str, _) -> str:
        import textwrap
        return textwrap.dedent(text)

    def _truncate(self, text: str, arg: Optional[str]) -> str:
        n = int(arg) if arg and arg.isdigit() else 100
        if len(text) <= n:
            return text
        return text[:n - 3] + "..."

    def _pad_left(self, text: str, arg: Optional[str]) -> str:
        n = int(arg) if arg and arg.isdigit() else 10
        return text.rjust(n)

    def _pad_right(self, text: str, arg: Optional[str]) -> str:
        n = int(arg) if arg and arg.isdigit() else 10
        return text.ljust(n)

    # --- ESCAPE ---

    def _json_escape(self, text: str, _) -> str:
        return json.dumps(text)[1:-1]

    def _json_unescape(self, text: str, _) -> str:
        return json.loads('"' + text + '"')

    def _backslash_escape(self, text: str, _) -> str:
        return text.replace("\\", "\\\\").replace("\n", "\\n").replace("\t", "\\t").replace("\r", "\\r")

    def _backslash_unescape(self, text: str, _) -> str:
        return (
            text.replace("\\n", "\n")
            .replace("\\t", "\t")
            .replace("\\r", "\r")
            .replace("\\\\", "\\")
        )

    # --- JSON ---

    def _json_pretty(self, text: str, _) -> str:
        data = json.loads(text.strip())
        return json.dumps(data, indent=2, ensure_ascii=False)

    def _json_minify(self, text: str, _) -> str:
        data = json.loads(text.strip())
        return json.dumps(data, separators=(",", ":"), ensure_ascii=False)

    def _json_keys(self, text: str, _) -> str:
        data = json.loads(text.strip())
        if isinstance(data, dict):
            return "\n".join(sorted(data.keys()))
        if isinstance(data, list) and data and isinstance(data[0], dict):
            keys: set = set()
            for item in data:
                if isinstance(item, dict):
                    keys.update(item.keys())
            return "\n".join(sorted(keys))
        raise ValueError("json-keys requires a JSON object or array of objects")

    # --- MISC ---

    def _rot13(self, text: str, _) -> str:
        return text.translate(str.maketrans(
            string.ascii_letters,
            string.ascii_letters[13:26] + string.ascii_letters[:13] +
            string.ascii_letters[39:52] + string.ascii_letters[26:39]
        ))

    def _reverse_text(self, text: str, _) -> str:
        return text[::-1]

    def _repeat(self, text: str, arg: Optional[str]) -> str:
        n = int(arg) if arg and arg.isdigit() else 2
        return text * n

    def _length(self, text: str, _) -> str:
        return str(len(text))

    def _word_count(self, text: str, _) -> str:
        return str(len(text.split()))

    def _char_count(self, text: str, _) -> str:
        return str(len(text.replace(" ", "").replace("\n", "")))

    def _lines_count(self, text: str, _) -> str:
        return str(len(text.splitlines()))

    def _remove_punctuation(self, text: str, _) -> str:
        return text.translate(str.maketrans("", "", string.punctuation))

    def _remove_digits(self, text: str, _) -> str:
        return re.sub(r"\d", "", text)

    def _remove_whitespace(self, text: str, _) -> str:
        return re.sub(r"\s+", "", text)

    def _extract_emails(self, text: str, _) -> str:
        emails = re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", text)
        return "\n".join(emails)

    def _extract_urls(self, text: str, _) -> str:
        urls = re.findall(r"https?://[^\s\"'>]+", text)
        return "\n".join(urls)

    def _extract_ips(self, text: str, _) -> str:
        ips = re.findall(
            r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\b",
            text
        )
        return "\n".join(ips)

    def _extract_digits(self, text: str, _) -> str:
        digits = re.findall(r"\d+", text)
        return "\n".join(digits)


# ============================================================================
# INPUT HELPERS
# ============================================================================

def read_input(text_arg: Optional[str], file_arg: Optional[str]) -> str:
    """
    Read input text from argument, file, or stdin.

    Priority: --text arg > --file arg > stdin

    Args:
        text_arg: Direct text string from CLI
        file_arg: Path to input file

    Returns:
        Input text as string

    Raises:
        FileNotFoundError: If --file path does not exist
        ValueError: If no input is provided and stdin is empty
    """
    if text_arg is not None:
        return text_arg

    if file_arg is not None:
        path = Path(file_arg)
        if not path.exists():
            raise FileNotFoundError(f"Input file not found: {path}")
        return path.read_text(encoding="utf-8")

    if not sys.stdin.isatty():
        return sys.stdin.read()

    raise ValueError(
        "No input provided. Use --text TEXT, --file PATH, or pipe input via stdin."
    )


# ============================================================================
# CLI COMMANDS
# ============================================================================

def cmd_transform(args: argparse.Namespace) -> int:
    """Execute a single named transformation."""
    try:
        text = read_input(args.text, getattr(args, "file", None))
        transformer = TextTransformer()
        result = transformer.transform(text, args.command, getattr(args, "arg", None))
        if not args.no_newline:
            print(result)
        else:
            sys.stdout.write(result)
        return 0
    except (ValueError, RuntimeError, FileNotFoundError) as e:
        print(f"[X] Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"[X] Unexpected error: {e}", file=sys.stderr)
        return 1


def cmd_list(args: argparse.Namespace) -> int:
    """List all available transformations."""
    transformer = TextTransformer()
    commands = transformer.commands

    categories = {
        "Encoding": ["base64-encode", "base64-decode", "hex-encode", "hex-decode",
                     "url-encode", "url-decode", "html-encode", "html-decode",
                     "binary-encode", "binary-decode"],
        "Hashing": ["md5", "sha1", "sha256", "sha512"],
        "Case Conversion": ["upper", "lower", "title", "swapcase",
                            "camel", "snake", "kebab", "pascal", "slug"],
        "Line Operations": ["sort", "sort-reverse", "unique", "reverse",
                            "count", "number", "grep", "grep-v", "head",
                            "tail", "trim-lines", "remove-empty"],
        "Text Format": ["trim", "squeeze", "wrap", "indent", "dedent",
                        "truncate", "pad-left", "pad-right"],
        "Escape/Unescape": ["json-escape", "json-unescape",
                            "backslash-escape", "backslash-unescape"],
        "JSON": ["json-pretty", "json-minify", "json-keys"],
        "Extract": ["extract-emails", "extract-urls", "extract-ips", "extract-digits"],
        "Misc": ["rot13", "reverse-text", "repeat", "length",
                 "words", "chars", "lines-count",
                 "remove-punct", "remove-digits", "remove-whitespace"],
    }

    if getattr(args, "flat", False):
        for cmd in commands:
            print(cmd)
        return 0

    print(f"TextTransform v{__version__} - Available Transformations")
    print("=" * 60)
    for cat, cmds in categories.items():
        available = [c for c in cmds if c in transformer._commands]
        if available:
            print(f"\n  {cat}:")
            for cmd in available:
                print(f"    {cmd}")

    uncategorized = set(commands)
    for cmds in categories.values():
        uncategorized -= set(cmds)
    if uncategorized:
        print("\n  Other:")
        for cmd in sorted(uncategorized):
            print(f"    {cmd}")

    print(f"\nTotal: {len(commands)} transformations")
    return 0


def cmd_version(args: argparse.Namespace) -> int:
    """Print version."""
    print(f"TextTransform v{__version__}")
    return 0


def cmd_chain(args: argparse.Namespace) -> int:
    """
    Chain multiple transformations in sequence.

    Example: texttransform chain --commands "trim,lower,slug" --text "  Hello World!  "
    Result: "hello-world"
    """
    try:
        text = read_input(args.text, getattr(args, "file", None))
        if not args.commands:
            raise ValueError("--commands is required for chain mode")

        transformer = TextTransformer()
        pipeline = [c.strip() for c in args.commands.split(",") if c.strip()]

        if not pipeline:
            raise ValueError("No valid commands in pipeline")

        result = text
        for cmd in pipeline:
            parts = cmd.split(":", 1)
            command = parts[0]
            arg = parts[1] if len(parts) > 1 else None
            result = transformer.transform(result, command, arg)

        if not args.no_newline:
            print(result)
        else:
            sys.stdout.write(result)
        return 0
    except (ValueError, RuntimeError, FileNotFoundError) as e:
        print(f"[X] Error: {e}", file=sys.stderr)
        return 1


def cmd_info(args: argparse.Namespace) -> int:
    """Print detailed info/stats about the input text."""
    try:
        text = read_input(args.text, getattr(args, "file", None))
        transformer = TextTransformer()

        lines = text.splitlines()
        words = text.split()
        chars_with_spaces = len(text)
        chars_no_spaces = len(text.replace(" ", "").replace("\n", ""))

        print("TextTransform - Text Analysis")
        print("=" * 40)
        print(f"  Characters (total):    {chars_with_spaces}")
        print(f"  Characters (no space): {chars_no_spaces}")
        print(f"  Words:                 {len(words)}")
        print(f"  Lines:                 {len(lines)}")
        print(f"  Paragraphs:            {len([p for p in text.split(chr(10)+chr(10)) if p.strip()])}")
        print(f"  Unique words:          {len(set(w.lower() for w in words))}")
        print(f"  Avg word length:       {sum(len(w) for w in words) / max(len(words), 1):.1f}")
        print(f"  Avg line length:       {sum(len(l) for l in lines) / max(len(lines), 1):.1f}")

        print("\n  Encoding hashes:")
        print(f"    MD5:    {transformer.transform(text, 'md5')}")
        print(f"    SHA256: {transformer.transform(text, 'sha256')}")

        emails = transformer.transform(text, "extract-emails")
        urls = transformer.transform(text, "extract-urls")
        ips = transformer.transform(text, "extract-ips")
        digits_found = transformer.transform(text, "extract-digits")

        if emails:
            print(f"\n  Emails found: {len(emails.splitlines())}")
        if urls:
            print(f"  URLs found: {len(urls.splitlines())}")
        if ips:
            print(f"  IPs found: {len(ips.splitlines())}")
        if digits_found:
            print(f"  Number sequences: {len(digits_found.splitlines())}")

        return 0
    except (ValueError, FileNotFoundError) as e:
        print(f"[X] Error: {e}", file=sys.stderr)
        return 1


# ============================================================================
# CLI ARGUMENT PARSER
# ============================================================================

def build_parser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="texttransform",
        description="TextTransform v{} - Universal Text Transformation & Encoding Toolkit".format(__version__),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  texttransform upper --text "hello world"
  texttransform base64-encode --text "secret message"
  texttransform sha256 --text "password"
  texttransform snake --text "MyVariableName"
  texttransform json-pretty --file data.json
  texttransform sort --text "banana\\napple\\ncherry"
  texttransform chain --commands "trim,lower,slug" --text "  Hello World!  "
  texttransform grep --arg "@" --file emails.txt
  echo "hello world" | texttransform upper
  texttransform info --text "Analyze this text"
  texttransform list

For full documentation: https://github.com/DonkRonk17/TextTransform
"""
    )

    parser.add_argument("--version", "-V", action="store_true",
                        help="Show version and exit")

    subparsers = parser.add_subparsers(dest="subcommand", help="Command to run")

    # --- list ---
    list_p = subparsers.add_parser("list", help="List all available transformations")
    list_p.add_argument("--flat", action="store_true",
                        help="Output one command per line (for scripting)")

    # --- version (subcommand) ---
    subparsers.add_parser("version", help="Show version")

    # --- info ---
    info_p = subparsers.add_parser("info", help="Analyze text and print stats")
    _add_input_args(info_p)

    # --- chain ---
    chain_p = subparsers.add_parser(
        "chain",
        help="Chain multiple transformations: --commands 'trim,lower,slug'"
    )
    chain_p.add_argument("--commands", required=True,
                         help="Comma-separated list of transforms (e.g., 'trim,lower,slug'). "
                              "Use colon for args: 'head:5,sort'")
    _add_input_args(chain_p)

    # --- All transformation commands ---
    transformer = TextTransformer()
    for cmd in transformer.commands:
        sub_p = subparsers.add_parser(cmd, help=f"Apply '{cmd}' transformation")
        sub_p.add_argument("--arg", "-a",
                           help="Optional argument (e.g., line count for head/tail, pattern for grep)")
        _add_input_args(sub_p)

    return parser


def _add_input_args(parser: argparse.ArgumentParser) -> None:
    """Add standard input arguments to a subparser."""
    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument("--text", "-t",
                             help="Input text string")
    input_group.add_argument("--file", "-f",
                             help="Input file path")
    parser.add_argument("--no-newline", "-n", action="store_true",
                        help="Do not print trailing newline")


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main() -> int:
    """
    CLI entry point.

    Returns:
        Exit code (0 = success, 1 = error)
    """
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except AttributeError:
            pass

    parser = build_parser()
    args = parser.parse_args()

    if args.version:
        print(f"TextTransform v{__version__}")
        return 0

    if args.subcommand is None:
        parser.print_help()
        return 0

    transformer_obj = TextTransformer()
    dispatch = {
        "list": cmd_list,
        "version": cmd_version,
        "info": cmd_info,
        "chain": cmd_chain,
    }

    if args.subcommand in dispatch:
        return dispatch[args.subcommand](args)

    if args.subcommand in transformer_obj.commands:
        args.command = args.subcommand
        return cmd_transform(args)

    print(f"[X] Unknown command: {args.subcommand}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
