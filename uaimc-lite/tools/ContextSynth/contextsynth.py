#!/usr/bin/env python3
"""
ContextSynth - Instant Context Summarizer for Any File or Project

Generate instant context summaries for any file, folder, or project.
AI-powered extraction of key points, changes, dependencies, and blockers.
Outputs in markdown or JSON for easy sharing.

Problem Solved:
AI agents often need quick context when switching between tasks, reviewing
code, or starting new sessions. Manual context gathering is time-consuming
and error-prone. ContextSynth provides one-click summaries.

Author: ATLAS (Team Brain)
For: Logan Smith / Metaphy LLC
Version: 1.0
Date: January 24, 2026
License: MIT
Requested by: Copilot VSCode (Tool Request #25)
"""

import os
import re
import json
import hashlib
from datetime import datetime
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Tuple
from enum import Enum
from pathlib import Path
import ast
import logging

__version__ = "1.0.0"
__author__ = "ATLAS (Team Brain)"


class DetailLevel(Enum):
    """Summary detail level."""
    BRIEF = "brief"      # One-liners
    STANDARD = "standard"  # Key points
    DETAILED = "detailed"  # Full analysis


class OutputFormat(Enum):
    """Output format."""
    MARKDOWN = "markdown"
    JSON = "json"
    TEXT = "text"


class FileType(Enum):
    """Detected file type."""
    PYTHON = "python"
    JAVASCRIPT = "javascript"
    TYPESCRIPT = "typescript"
    JSON_FILE = "json"
    MARKDOWN = "markdown"
    YAML = "yaml"
    HTML = "html"
    CSS = "css"
    RUST = "rust"
    GO = "go"
    JAVA = "java"
    C = "c"
    CPP = "cpp"
    CONFIG = "config"
    TEXT = "text"
    UNKNOWN = "unknown"


class ProjectType(Enum):
    """Detected project type."""
    PYTHON = "python"
    NODE = "node"
    REACT = "react"
    REACT_NATIVE = "react_native"
    RUST = "rust"
    TAURI = "tauri"
    ELECTRON = "electron"
    GENERIC = "generic"


@dataclass
class CodeElement:
    """A code element (function, class, etc.)."""
    name: str
    element_type: str  # "function", "class", "variable", "import"
    line_number: int
    docstring: Optional[str] = None
    parameters: Optional[List[str]] = None


@dataclass
class FileSummary:
    """Summary of a single file."""
    path: str
    file_type: FileType
    size_bytes: int
    line_count: int
    description: str
    key_elements: List[CodeElement]
    imports: List[str]
    todos: List[str]
    blockers: List[str]
    dependencies: List[str]
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            "path": self.path,
            "file_type": self.file_type.value,
            "size_bytes": self.size_bytes,
            "line_count": self.line_count,
            "description": self.description,
            "key_elements": [
                {
                    "name": e.name,
                    "type": e.element_type,
                    "line": e.line_number,
                    "docstring": e.docstring
                }
                for e in self.key_elements
            ],
            "imports": self.imports,
            "todos": self.todos,
            "blockers": self.blockers,
            "dependencies": self.dependencies
        }


@dataclass
class FolderSummary:
    """Summary of a folder."""
    path: str
    file_count: int
    total_lines: int
    file_types: Dict[str, int]
    files: List[FileSummary]
    description: str
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            "path": self.path,
            "file_count": self.file_count,
            "total_lines": self.total_lines,
            "file_types": self.file_types,
            "files": [f.to_dict() for f in self.files],
            "description": self.description
        }


@dataclass
class ProjectSummary:
    """Summary of a project."""
    path: str
    project_type: ProjectType
    name: str
    description: str
    version: Optional[str]
    file_count: int
    total_lines: int
    main_technologies: List[str]
    dependencies: List[str]
    dev_dependencies: List[str]
    entry_points: List[str]
    key_files: List[FileSummary]
    todos: List[str]
    blockers: List[str]
    recent_changes: List[str]
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            "path": self.path,
            "project_type": self.project_type.value,
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "file_count": self.file_count,
            "total_lines": self.total_lines,
            "main_technologies": self.main_technologies,
            "dependencies": self.dependencies,
            "dev_dependencies": self.dev_dependencies,
            "entry_points": self.entry_points,
            "key_files": [f.to_dict() for f in self.key_files],
            "todos": self.todos,
            "blockers": self.blockers,
            "recent_changes": self.recent_changes
        }


class FileAnalyzer:
    """Analyzes individual files."""
    
    # File extension to type mapping
    EXTENSION_MAP = {
        ".py": FileType.PYTHON,
        ".js": FileType.JAVASCRIPT,
        ".jsx": FileType.JAVASCRIPT,
        ".ts": FileType.TYPESCRIPT,
        ".tsx": FileType.TYPESCRIPT,
        ".json": FileType.JSON_FILE,
        ".md": FileType.MARKDOWN,
        ".yaml": FileType.YAML,
        ".yml": FileType.YAML,
        ".html": FileType.HTML,
        ".htm": FileType.HTML,
        ".css": FileType.CSS,
        ".scss": FileType.CSS,
        ".rs": FileType.RUST,
        ".go": FileType.GO,
        ".java": FileType.JAVA,
        ".c": FileType.C,
        ".cpp": FileType.CPP,
        ".h": FileType.C,
        ".hpp": FileType.CPP,
        ".toml": FileType.CONFIG,
        ".ini": FileType.CONFIG,
        ".cfg": FileType.CONFIG,
        ".txt": FileType.TEXT,
    }
    
    @classmethod
    def detect_file_type(cls, filepath: Path) -> FileType:
        """Detect file type from extension."""
        ext = filepath.suffix.lower()
        return cls.EXTENSION_MAP.get(ext, FileType.UNKNOWN)
    
    @classmethod
    def analyze_file(cls, filepath: Path, detail_level: DetailLevel = DetailLevel.STANDARD) -> FileSummary:
        """Analyze a single file."""
        file_type = cls.detect_file_type(filepath)
        
        try:
            content = filepath.read_text(encoding='utf-8', errors='ignore')
        except Exception:
            content = ""
        
        lines = content.split('\n')
        line_count = len(lines)
        
        # Extract elements based on file type
        if file_type == FileType.PYTHON:
            key_elements, imports = cls._analyze_python(content)
        elif file_type in [FileType.JAVASCRIPT, FileType.TYPESCRIPT]:
            key_elements, imports = cls._analyze_javascript(content)
        else:
            key_elements, imports = [], []
        
        # Extract TODOs and blockers
        todos = cls._extract_todos(content)
        blockers = cls._extract_blockers(content)
        
        # Generate description
        description = cls._generate_description(filepath, file_type, key_elements, line_count)
        
        # Extract dependencies from content
        dependencies = cls._extract_dependencies(content, file_type)
        
        # Get file size safely
        try:
            size_bytes = filepath.stat().st_size
        except Exception:
            size_bytes = 0
        
        return FileSummary(
            path=str(filepath),
            file_type=file_type,
            size_bytes=size_bytes,
            line_count=line_count,
            description=description,
            key_elements=key_elements if detail_level != DetailLevel.BRIEF else [],
            imports=imports,
            todos=todos,
            blockers=blockers,
            dependencies=dependencies
        )
    
    @classmethod
    def _analyze_python(cls, content: str) -> Tuple[List[CodeElement], List[str]]:
        """Analyze Python code."""
        elements = []
        imports = []
        
        try:
            tree = ast.parse(content)
            
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    docstring = ast.get_docstring(node)
                    params = [arg.arg for arg in node.args.args]
                    elements.append(CodeElement(
                        name=node.name,
                        element_type="function",
                        line_number=node.lineno,
                        docstring=docstring[:100] if docstring else None,
                        parameters=params
                    ))
                elif isinstance(node, ast.ClassDef):
                    docstring = ast.get_docstring(node)
                    elements.append(CodeElement(
                        name=node.name,
                        element_type="class",
                        line_number=node.lineno,
                        docstring=docstring[:100] if docstring else None
                    ))
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        imports.append(alias.name)
                elif isinstance(node, ast.ImportFrom):
                    if node.module:
                        imports.append(node.module)
        except SyntaxError:
            # Fall back to regex-based analysis
            elements, imports = cls._analyze_python_regex(content)
        
        return elements, imports
    
    @classmethod
    def _analyze_python_regex(cls, content: str) -> Tuple[List[CodeElement], List[str]]:
        """Analyze Python code using regex (fallback)."""
        elements = []
        imports = []
        
        # Find functions
        for match in re.finditer(r'^def\s+(\w+)\s*\(', content, re.MULTILINE):
            elements.append(CodeElement(
                name=match.group(1),
                element_type="function",
                line_number=content[:match.start()].count('\n') + 1
            ))
        
        # Find classes
        for match in re.finditer(r'^class\s+(\w+)\s*[:\(]', content, re.MULTILINE):
            elements.append(CodeElement(
                name=match.group(1),
                element_type="class",
                line_number=content[:match.start()].count('\n') + 1
            ))
        
        # Find imports
        for match in re.finditer(r'^(?:from\s+(\S+)\s+)?import\s+(\S+)', content, re.MULTILINE):
            module = match.group(1) or match.group(2)
            if module:
                imports.append(module.split('.')[0])
        
        return elements, list(set(imports))
    
    @classmethod
    def _analyze_javascript(cls, content: str) -> Tuple[List[CodeElement], List[str]]:
        """Analyze JavaScript/TypeScript code."""
        elements = []
        imports = []
        
        # Find functions
        patterns = [
            r'function\s+(\w+)\s*\(',
            r'const\s+(\w+)\s*=\s*(?:async\s*)?\([^)]*\)\s*=>',
            r'const\s+(\w+)\s*=\s*function',
            r'(?:export\s+)?(?:default\s+)?(?:async\s+)?function\s+(\w+)',
        ]
        
        for pattern in patterns:
            for match in re.finditer(pattern, content):
                name = match.group(1)
                if name:
                    elements.append(CodeElement(
                        name=name,
                        element_type="function",
                        line_number=content[:match.start()].count('\n') + 1
                    ))
        
        # Find classes
        for match in re.finditer(r'class\s+(\w+)', content):
            elements.append(CodeElement(
                name=match.group(1),
                element_type="class",
                line_number=content[:match.start()].count('\n') + 1
            ))
        
        # Find imports
        for match in re.finditer(r'import\s+.*?from\s+[\'"]([^\'"]+)[\'"]', content):
            imports.append(match.group(1))
        for match in re.finditer(r'require\s*\(\s*[\'"]([^\'"]+)[\'"]\s*\)', content):
            imports.append(match.group(1))
        
        return elements, imports
    
    @classmethod
    def _extract_todos(cls, content: str) -> List[str]:
        """Extract TODO comments."""
        todos = []
        for match in re.finditer(r'(?:#|//|/\*)\s*TODO[:\s]*(.+?)(?:\*/|\n|$)', content, re.IGNORECASE):
            todos.append(match.group(1).strip()[:100])
        return todos[:10]  # Limit to 10
    
    @classmethod
    def _extract_blockers(cls, content: str) -> List[str]:
        """Extract FIXME/HACK/BUG comments."""
        blockers = []
        for match in re.finditer(r'(?:#|//|/\*)\s*(?:FIXME|HACK|BUG|XXX)[:\s]*(.+?)(?:\*/|\n|$)', content, re.IGNORECASE):
            blockers.append(match.group(1).strip()[:100])
        return blockers[:10]
    
    @classmethod
    def _extract_dependencies(cls, content: str, file_type: FileType) -> List[str]:
        """Extract dependencies from file content."""
        if file_type == FileType.JSON_FILE:
            try:
                data = json.loads(content)
                deps = []
                if "dependencies" in data:
                    deps.extend(data["dependencies"].keys())
                if "devDependencies" in data:
                    deps.extend(data["devDependencies"].keys())
                return deps[:20]
            except Exception:
                pass
        return []
    
    @classmethod
    def _generate_description(cls, filepath: Path, file_type: FileType, 
                             elements: List[CodeElement], line_count: int) -> str:
        """Generate a brief description."""
        name = filepath.name
        
        classes = [e for e in elements if e.element_type == "class"]
        functions = [e for e in elements if e.element_type == "function"]
        
        parts = [f"{name}"]
        
        if classes:
            parts.append(f"{len(classes)} class(es)")
        if functions:
            parts.append(f"{len(functions)} function(s)")
        
        parts.append(f"{line_count} lines")
        
        return " - ".join(parts)


class ProjectAnalyzer:
    """Analyzes projects."""
    
    # Files that indicate project type
    PROJECT_INDICATORS = {
        "package.json": ProjectType.NODE,
        "requirements.txt": ProjectType.PYTHON,
        "pyproject.toml": ProjectType.PYTHON,
        "Cargo.toml": ProjectType.RUST,
        "src-tauri/tauri.conf.json": ProjectType.TAURI,
    }
    
    # Files to prioritize in analysis
    KEY_FILES = [
        "README.md",
        "package.json",
        "requirements.txt",
        "pyproject.toml",
        "Cargo.toml",
        "main.py",
        "app.py",
        "index.js",
        "index.ts",
        "App.tsx",
        "App.jsx",
        "main.rs",
    ]
    
    # Directories to skip
    SKIP_DIRS = {
        "node_modules",
        ".git",
        "__pycache__",
        ".venv",
        "venv",
        ".env",
        "dist",
        "build",
        "target",
        ".next",
        ".nuxt",
        "coverage",
    }
    
    @classmethod
    def detect_project_type(cls, project_path: Path) -> ProjectType:
        """Detect project type."""
        for indicator, ptype in cls.PROJECT_INDICATORS.items():
            if (project_path / indicator).exists():
                return ptype
        return ProjectType.GENERIC
    
    @classmethod
    def analyze_project(cls, project_path: Path, 
                       detail_level: DetailLevel = DetailLevel.STANDARD) -> ProjectSummary:
        """Analyze a project."""
        project_type = cls.detect_project_type(project_path)
        
        # Get project metadata
        name, description, version = cls._extract_metadata(project_path, project_type)
        
        # Collect all files
        all_files = cls._collect_files(project_path)
        
        # Analyze key files
        key_files = []
        for filename in cls.KEY_FILES:
            filepath = project_path / filename
            if filepath.exists():
                key_files.append(FileAnalyzer.analyze_file(filepath, detail_level))
        
        # Count lines
        total_lines = 0
        for filepath in all_files:
            try:
                content = filepath.read_text(encoding='utf-8', errors='ignore')
                total_lines += len(content.split('\n'))
            except Exception:
                pass
        
        # Extract dependencies
        deps, dev_deps = cls._extract_all_dependencies(project_path, project_type)
        
        # Extract technologies
        technologies = cls._detect_technologies(project_path, project_type)
        
        # Find entry points
        entry_points = cls._find_entry_points(project_path, project_type)
        
        # Collect all TODOs and blockers
        todos = []
        blockers = []
        for file_summary in key_files:
            todos.extend(file_summary.todos)
            blockers.extend(file_summary.blockers)
        
        # Get recent changes (if git available)
        recent_changes = cls._get_recent_changes(project_path)
        
        return ProjectSummary(
            path=str(project_path),
            project_type=project_type,
            name=name,
            description=description,
            version=version,
            file_count=len(all_files),
            total_lines=total_lines,
            main_technologies=technologies,
            dependencies=deps,
            dev_dependencies=dev_deps,
            entry_points=entry_points,
            key_files=key_files,
            todos=todos[:20],
            blockers=blockers[:10],
            recent_changes=recent_changes
        )
    
    @classmethod
    def _collect_files(cls, project_path: Path) -> List[Path]:
        """Collect all relevant files."""
        files = []
        
        for root, dirs, filenames in os.walk(project_path):
            # Skip excluded directories
            dirs[:] = [d for d in dirs if d not in cls.SKIP_DIRS]
            
            for filename in filenames:
                filepath = Path(root) / filename
                if filepath.suffix.lower() in FileAnalyzer.EXTENSION_MAP:
                    files.append(filepath)
        
        return files
    
    @classmethod
    def _extract_metadata(cls, project_path: Path, 
                         project_type: ProjectType) -> Tuple[str, str, Optional[str]]:
        """Extract project name, description, and version."""
        name = project_path.name
        description = ""
        version = None
        
        # Try package.json for Node projects
        pkg_json = project_path / "package.json"
        if pkg_json.exists():
            try:
                data = json.loads(pkg_json.read_text())
                name = data.get("name", name)
                description = data.get("description", "")
                version = data.get("version")
            except Exception:
                pass
        
        # Try pyproject.toml for Python projects
        pyproject = project_path / "pyproject.toml"
        if pyproject.exists():
            try:
                content = pyproject.read_text()
                # Simple TOML parsing for name/version
                name_match = re.search(r'name\s*=\s*["\']([^"\']+)["\']', content)
                version_match = re.search(r'version\s*=\s*["\']([^"\']+)["\']', content)
                desc_match = re.search(r'description\s*=\s*["\']([^"\']+)["\']', content)
                
                if name_match:
                    name = name_match.group(1)
                if version_match:
                    version = version_match.group(1)
                if desc_match:
                    description = desc_match.group(1)
            except Exception:
                pass
        
        # Try README for description if not found
        if not description:
            readme = project_path / "README.md"
            if readme.exists():
                try:
                    content = readme.read_text(encoding='utf-8', errors='ignore')
                    lines = content.split('\n')
                    for line in lines[1:10]:  # Skip title, look in first 10 lines
                        line = line.strip()
                        if line and not line.startswith('#') and not line.startswith('!'):
                            description = line[:200]
                            break
                except Exception:
                    pass
        
        return name, description, version
    
    @classmethod
    def _extract_all_dependencies(cls, project_path: Path, 
                                  project_type: ProjectType) -> Tuple[List[str], List[str]]:
        """Extract all dependencies."""
        deps = []
        dev_deps = []
        
        # Node.js
        pkg_json = project_path / "package.json"
        if pkg_json.exists():
            try:
                data = json.loads(pkg_json.read_text())
                deps = list(data.get("dependencies", {}).keys())
                dev_deps = list(data.get("devDependencies", {}).keys())
            except Exception:
                pass
        
        # Python
        requirements = project_path / "requirements.txt"
        if requirements.exists():
            try:
                content = requirements.read_text()
                for line in content.split('\n'):
                    line = line.strip()
                    if line and not line.startswith('#'):
                        # Remove version specifier
                        pkg = re.split(r'[<>=!]', line)[0].strip()
                        if pkg:
                            deps.append(pkg)
            except Exception:
                pass
        
        return deps[:30], dev_deps[:20]
    
    @classmethod
    def _detect_technologies(cls, project_path: Path, 
                            project_type: ProjectType) -> List[str]:
        """Detect main technologies used."""
        technologies = []
        
        # Check for common tech indicators
        indicators = {
            "package.json": ["Node.js"],
            "requirements.txt": ["Python"],
            "Cargo.toml": ["Rust"],
            "tsconfig.json": ["TypeScript"],
            "tailwind.config.js": ["Tailwind CSS"],
            "next.config.js": ["Next.js"],
            "vite.config.js": ["Vite"],
            "webpack.config.js": ["Webpack"],
            ".eslintrc": ["ESLint"],
            "jest.config.js": ["Jest"],
            "pytest.ini": ["pytest"],
        }
        
        for indicator, techs in indicators.items():
            if (project_path / indicator).exists():
                technologies.extend(techs)
        
        # Check package.json dependencies
        pkg_json = project_path / "package.json"
        if pkg_json.exists():
            try:
                data = json.loads(pkg_json.read_text())
                all_deps = list(data.get("dependencies", {}).keys())
                all_deps += list(data.get("devDependencies", {}).keys())
                
                tech_mapping = {
                    "react": "React",
                    "react-native": "React Native",
                    "vue": "Vue.js",
                    "angular": "Angular",
                    "express": "Express.js",
                    "fastify": "Fastify",
                    "socket.io": "Socket.IO",
                    "tailwindcss": "Tailwind CSS",
                    "prisma": "Prisma",
                    "mongodb": "MongoDB",
                }
                
                for dep in all_deps:
                    if dep in tech_mapping:
                        technologies.append(tech_mapping[dep])
            except Exception:
                pass
        
        return list(set(technologies))[:10]
    
    @classmethod
    def _find_entry_points(cls, project_path: Path, 
                          project_type: ProjectType) -> List[str]:
        """Find main entry points."""
        entry_points = []
        
        common_entries = [
            "main.py", "app.py", "__main__.py",
            "index.js", "index.ts", "main.js", "main.ts",
            "App.tsx", "App.jsx", "App.js",
            "main.rs", "lib.rs",
        ]
        
        for entry in common_entries:
            if (project_path / entry).exists():
                entry_points.append(entry)
            # Check src directory
            if (project_path / "src" / entry).exists():
                entry_points.append(f"src/{entry}")
        
        return entry_points[:5]
    
    @classmethod
    def _get_recent_changes(cls, project_path: Path) -> List[str]:
        """Get recent git changes if available."""
        import subprocess
        
        try:
            result = subprocess.run(
                ["git", "log", "--oneline", "-n", "5"],
                cwd=project_path,
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0:
                changes = []
                for line in result.stdout.strip().split('\n'):
                    if line:
                        changes.append(line)
                return changes
        except Exception:
            pass
        
        return []


class ContextSynth:
    """
    Main class for context synthesis.
    
    Generates instant context summaries for files, folders, and projects.
    """
    
    def __init__(self, detail_level: DetailLevel = DetailLevel.STANDARD):
        """
        Initialize ContextSynth.
        
        Args:
            detail_level: Level of detail for summaries
        """
        self.detail_level = detail_level
        self.logger = logging.getLogger(__name__)
    
    def summarize_file(self, filepath: Path) -> FileSummary:
        """
        Generate summary for a single file.
        
        Args:
            filepath: Path to file
            
        Returns:
            FileSummary object
        """
        return FileAnalyzer.analyze_file(filepath, self.detail_level)
    
    def summarize_folder(self, folder_path: Path) -> FolderSummary:
        """
        Generate summary for a folder.
        
        Args:
            folder_path: Path to folder
            
        Returns:
            FolderSummary object
        """
        files = []
        file_types: Dict[str, int] = {}
        total_lines = 0
        
        for item in folder_path.iterdir():
            if item.is_file():
                file_type = FileAnalyzer.detect_file_type(item)
                if file_type != FileType.UNKNOWN:
                    summary = self.summarize_file(item)
                    files.append(summary)
                    
                    type_name = file_type.value
                    file_types[type_name] = file_types.get(type_name, 0) + 1
                    total_lines += summary.line_count
        
        description = f"{len(files)} files in {folder_path.name}"
        if file_types:
            main_type = max(file_types, key=file_types.get)
            description += f", primarily {main_type}"
        
        return FolderSummary(
            path=str(folder_path),
            file_count=len(files),
            total_lines=total_lines,
            file_types=file_types,
            files=files,
            description=description
        )
    
    def summarize_project(self, project_path: Path) -> ProjectSummary:
        """
        Generate summary for a project.
        
        Args:
            project_path: Path to project root
            
        Returns:
            ProjectSummary object
        """
        return ProjectAnalyzer.analyze_project(project_path, self.detail_level)
    
    def format_markdown(self, summary: Any) -> str:
        """Format summary as markdown."""
        if isinstance(summary, FileSummary):
            return self._format_file_markdown(summary)
        elif isinstance(summary, FolderSummary):
            return self._format_folder_markdown(summary)
        elif isinstance(summary, ProjectSummary):
            return self._format_project_markdown(summary)
        else:
            return str(summary)
    
    def format_json(self, summary: Any) -> str:
        """Format summary as JSON."""
        if hasattr(summary, 'to_dict'):
            return json.dumps(summary.to_dict(), indent=2, ensure_ascii=False)
        return json.dumps(summary, indent=2, ensure_ascii=False)
    
    def format_text(self, summary: Any) -> str:
        """Format summary as plain text."""
        if isinstance(summary, FileSummary):
            return self._format_file_text(summary)
        elif isinstance(summary, FolderSummary):
            return self._format_folder_text(summary)
        elif isinstance(summary, ProjectSummary):
            return self._format_project_text(summary)
        else:
            return str(summary)
    
    def _format_file_markdown(self, summary: FileSummary) -> str:
        """Format file summary as markdown."""
        lines = [
            f"# File Summary: {Path(summary.path).name}",
            "",
            f"**Path:** `{summary.path}`",
            f"**Type:** {summary.file_type.value}",
            f"**Size:** {summary.size_bytes:,} bytes",
            f"**Lines:** {summary.line_count}",
            "",
        ]
        
        if summary.key_elements:
            lines.append("## Key Elements")
            lines.append("")
            for elem in summary.key_elements[:10]:
                lines.append(f"- **{elem.element_type}** `{elem.name}` (line {elem.line_number})")
                if elem.docstring:
                    lines.append(f"  - {elem.docstring}")
            lines.append("")
        
        if summary.imports:
            lines.append("## Imports")
            lines.append("")
            lines.append(", ".join(f"`{i}`" for i in summary.imports[:10]))
            lines.append("")
        
        if summary.todos:
            lines.append("## TODOs")
            lines.append("")
            for todo in summary.todos:
                lines.append(f"- [ ] {todo}")
            lines.append("")
        
        if summary.blockers:
            lines.append("## Blockers")
            lines.append("")
            for blocker in summary.blockers:
                lines.append(f"- [!] {blocker}")
            lines.append("")
        
        return "\n".join(lines)
    
    def _format_folder_markdown(self, summary: FolderSummary) -> str:
        """Format folder summary as markdown."""
        lines = [
            f"# Folder Summary: {Path(summary.path).name}",
            "",
            f"**Path:** `{summary.path}`",
            f"**Files:** {summary.file_count}",
            f"**Total Lines:** {summary.total_lines:,}",
            "",
        ]
        
        if summary.file_types:
            lines.append("## File Types")
            lines.append("")
            for ftype, count in sorted(summary.file_types.items(), key=lambda x: -x[1]):
                lines.append(f"- {ftype}: {count}")
            lines.append("")
        
        if summary.files:
            lines.append("## Files")
            lines.append("")
            for file in summary.files[:10]:
                lines.append(f"- `{Path(file.path).name}` - {file.description}")
            lines.append("")
        
        return "\n".join(lines)
    
    def _format_project_markdown(self, summary: ProjectSummary) -> str:
        """Format project summary as markdown."""
        lines = [
            f"# Project Summary: {summary.name}",
            "",
            f"**Type:** {summary.project_type.value}",
        ]
        
        if summary.version:
            lines.append(f"**Version:** {summary.version}")
        
        lines.extend([
            f"**Files:** {summary.file_count}",
            f"**Total Lines:** {summary.total_lines:,}",
            "",
        ])
        
        if summary.description:
            lines.append(f"> {summary.description}")
            lines.append("")
        
        if summary.main_technologies:
            lines.append("## Technologies")
            lines.append("")
            lines.append(", ".join(f"**{t}**" for t in summary.main_technologies))
            lines.append("")
        
        if summary.entry_points:
            lines.append("## Entry Points")
            lines.append("")
            for entry in summary.entry_points:
                lines.append(f"- `{entry}`")
            lines.append("")
        
        if summary.dependencies:
            lines.append("## Dependencies")
            lines.append("")
            lines.append(", ".join(f"`{d}`" for d in summary.dependencies[:15]))
            lines.append("")
        
        if summary.key_files:
            lines.append("## Key Files")
            lines.append("")
            for file in summary.key_files[:5]:
                lines.append(f"### {Path(file.path).name}")
                lines.append(f"{file.description}")
                lines.append("")
        
        if summary.todos:
            lines.append("## TODOs")
            lines.append("")
            for todo in summary.todos[:5]:
                lines.append(f"- [ ] {todo}")
            lines.append("")
        
        if summary.blockers:
            lines.append("## Blockers")
            lines.append("")
            for blocker in summary.blockers[:5]:
                lines.append(f"- [!] {blocker}")
            lines.append("")
        
        if summary.recent_changes:
            lines.append("## Recent Changes")
            lines.append("")
            for change in summary.recent_changes:
                lines.append(f"- {change}")
            lines.append("")
        
        return "\n".join(lines)
    
    def _format_file_text(self, summary: FileSummary) -> str:
        """Format file summary as text."""
        return f"""File: {summary.path}
Type: {summary.file_type.value}
Size: {summary.size_bytes:,} bytes
Lines: {summary.line_count}
Elements: {len(summary.key_elements)}
TODOs: {len(summary.todos)}
Blockers: {len(summary.blockers)}"""
    
    def _format_folder_text(self, summary: FolderSummary) -> str:
        """Format folder summary as text."""
        return f"""Folder: {summary.path}
Files: {summary.file_count}
Lines: {summary.total_lines:,}
Types: {', '.join(f'{k}:{v}' for k, v in summary.file_types.items())}"""
    
    def _format_project_text(self, summary: ProjectSummary) -> str:
        """Format project summary as text."""
        return f"""Project: {summary.name}
Type: {summary.project_type.value}
Version: {summary.version or 'unknown'}
Files: {summary.file_count}
Lines: {summary.total_lines:,}
Technologies: {', '.join(summary.main_technologies)}
Dependencies: {len(summary.dependencies)}
TODOs: {len(summary.todos)}"""


def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="ContextSynth - Instant Context Summarizer"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # File command
    file_parser = subparsers.add_parser("file", help="Summarize a file")
    file_parser.add_argument("path", help="Path to file")
    file_parser.add_argument("--format", "-f", choices=["markdown", "json", "text"],
                            default="markdown", help="Output format")
    file_parser.add_argument("--output", "-o", help="Output file")
    
    # Folder command
    folder_parser = subparsers.add_parser("folder", help="Summarize a folder")
    folder_parser.add_argument("path", help="Path to folder")
    folder_parser.add_argument("--format", "-f", choices=["markdown", "json", "text"],
                              default="markdown", help="Output format")
    folder_parser.add_argument("--output", "-o", help="Output file")
    
    # Project command
    project_parser = subparsers.add_parser("project", help="Summarize a project")
    project_parser.add_argument("path", nargs="?", default=".", help="Path to project")
    project_parser.add_argument("--format", "-f", choices=["markdown", "json", "text"],
                               default="markdown", help="Output format")
    project_parser.add_argument("--detail", "-d", choices=["brief", "standard", "detailed"],
                               default="standard", help="Detail level")
    project_parser.add_argument("--output", "-o", help="Output file")
    
    # Version
    parser.add_argument("--version", "-v", action="version", 
                       version=f"ContextSynth {__version__}")
    
    args = parser.parse_args()
    
    # Set detail level
    detail_map = {
        "brief": DetailLevel.BRIEF,
        "standard": DetailLevel.STANDARD,
        "detailed": DetailLevel.DETAILED
    }
    detail_level = detail_map.get(getattr(args, 'detail', 'standard'), DetailLevel.STANDARD)
    
    synth = ContextSynth(detail_level)
    
    if args.command == "file":
        filepath = Path(args.path)
        if not filepath.exists():
            print(f"Error: File not found: {filepath}")
            exit(1)
        
        summary = synth.summarize_file(filepath)
        
    elif args.command == "folder":
        folder_path = Path(args.path)
        if not folder_path.exists():
            print(f"Error: Folder not found: {folder_path}")
            exit(1)
        
        summary = synth.summarize_folder(folder_path)
        
    elif args.command == "project":
        project_path = Path(args.path)
        if not project_path.exists():
            print(f"Error: Project not found: {project_path}")
            exit(1)
        
        summary = synth.summarize_project(project_path)
        
    else:
        parser.print_help()
        exit(0)
    
    # Format output
    format_map = {
        "markdown": synth.format_markdown,
        "json": synth.format_json,
        "text": synth.format_text
    }
    
    output = format_map[args.format](summary)
    
    # Write output
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(output)
        print(f"Summary saved to {args.output}")
    else:
        print(output)


if __name__ == "__main__":
    main()
