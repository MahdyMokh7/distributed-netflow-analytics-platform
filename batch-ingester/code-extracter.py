from pathlib import Path
import fnmatch

# =============================================================================
# Project root
# =============================================================================
ROOT = Path(__file__).resolve().parent
SRC_DIR = ROOT / "src"

OUTPUT_DIR = ROOT / "docs"
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / "project_contents.txt"

SEPARATOR = "\n" + "=" * 120 + "\n\n"


# =============================================================================
# Directories to completely skip
# =============================================================================
EXCLUDED_DIRECTORIES = {
    "target",
    "build",
    "out",
    "logs",
    ".idea",
    ".gradle",
    ".mvn",
    ".vscode",
    ".settings",
    "node_modules",
}

EXCLUDED_DIRECTORY_NAMES = {
    directory.lower()
    for directory in EXCLUDED_DIRECTORIES
}


# =============================================================================
# Test directories
# =============================================================================
TEST_DIRECTORIES = {
    "test",
    "tests",
    "src-test",
    "test-output",
}


# =============================================================================
# Files to export
# =============================================================================
ALLOWED_EXTENSIONS = {
    ".java",
    ".yml",
    ".yaml",
    ".xml",
}


# =============================================================================
# .gitignore-style patterns
# =============================================================================
GITIGNORE_PATTERNS = [
    # Maven / Java
    "target/",
    "build/",
    "*.class",

    # IDE
    ".idea/",
    "*.iml",
    ".vscode/",
    ".settings/",
    "*.swp",
    "*.swo",

    # Logs
    "logs/",
    "*.log",
    "*.processing",

    # OS
    ".DS_Store",
    "Thumbs.db",

    # Temporary
    "*.tmp",
    "*.bak",
    "*.orig",
    "*.rej",
]


def get_relative_path(path: Path):
    """Return the path relative to the project root, or None if outside it."""
    try:
        return path.relative_to(ROOT)
    except ValueError:
        return None


def is_excluded_directory(path: Path) -> bool:
    """
    Return True if the path is inside an excluded directory
    or a hidden directory.
    """
    relative = get_relative_path(path)

    if relative is None:
        return True

    return any(
        part.lower() in EXCLUDED_DIRECTORY_NAMES or part.startswith(".")
        for part in relative.parts
    )


def is_test_path(path: Path) -> bool:
    """
    Ignore anything located inside a test directory.
    """
    relative = get_relative_path(path)

    if relative is None:
        return True

    return any(
        part.lower() in TEST_DIRECTORIES
        for part in relative.parts
    )


def matches_directory_pattern(relative: str, pattern: str) -> bool:
    """Check whether a relative path matches a directory pattern."""
    directory = pattern.rstrip("/")

    return (
        relative == directory
        or relative.startswith(directory + "/")
        or f"/{directory}/" in relative
    )


def matches_wildcard_pattern(path: Path, relative: str, pattern: str) -> bool:
    """Check whether a path matches a wildcard pattern."""
    return (
        fnmatch.fnmatch(relative, pattern)
        or fnmatch.fnmatch(path.name, pattern)
    )


def matches_gitignore_pattern(
    path: Path,
    relative: str,
    pattern: str,
) -> bool:
    """Check whether a path matches one gitignore-style pattern."""
    if pattern.endswith("/"):
        return matches_directory_pattern(relative, pattern)

    if "*" in pattern:
        return matches_wildcard_pattern(path, relative, pattern)

    return relative == pattern


def is_gitignored(path: Path) -> bool:
    """
    Small .gitignore-style matcher.
    """
    relative_path = get_relative_path(path)

    if relative_path is None:
        return True

    relative = relative_path.as_posix()

    return any(
        matches_gitignore_pattern(path, relative, pattern)
        for pattern in GITIGNORE_PATTERNS
    )


def is_java_test_file(file_path: Path) -> bool:
    """Return True if the file follows a common Java test naming convention."""
    if file_path.suffix.lower() != ".java":
        return False

    name = file_path.name.lower()

    return (
        name.startswith("test")
        or name.endswith("test.java")
        or name.endswith("tests.java")
    )


def is_inside_source_directory(file_path: Path) -> bool:
    """Return True if the file is located inside src/."""
    try:
        file_path.relative_to(SRC_DIR)
        return True
    except ValueError:
        return False


def should_export(file_path: Path) -> bool:
    """
    Determine whether a file should be exported.
    """
    if not is_inside_source_directory(file_path):
        return False

    if file_path.suffix.lower() not in ALLOWED_EXTENSIONS:
        return False

    if is_excluded_directory(file_path.parent):
        return False

    # Preserve existing behavior: test paths are currently exported.
    if is_test_path(file_path):
        return True  # Temp

    if is_gitignored(file_path):
        return False

    return not is_java_test_file(file_path)


def read_file_content(file_path: Path):
    """Read a file using UTF-8, falling back to Latin-1."""
    try:
        return file_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        try:
            return file_path.read_text(encoding="latin-1")
        except Exception:
            print(f"Skipping unreadable file: {file_path}")
            return None
    except Exception as error:
        print(f"Could not read {file_path}: {error}")
        return None


def write_file_content(file_path: Path, output):
    """
    Write filename, relative path and contents to the output file.
    """
    content = read_file_content(file_path)

    if content is None:
        return

    relative_path = file_path.relative_to(ROOT)

    output.write(SEPARATOR)
    output.write(f"File: {file_path.name}\n")
    output.write(f"Path: {relative_path}\n")
    output.write(SEPARATOR)
    output.write(content)

    if not content.endswith("\n"):
        output.write("\n")


def validate_source_directory() -> bool:
    """Validate that the source directory exists and is a directory."""
    if not SRC_DIR.exists():
        print(f"ERROR: src directory does not exist: {SRC_DIR}")
        return False

    if not SRC_DIR.is_dir():
        print(f"ERROR: src is not a directory: {SRC_DIR}")
        return False

    return True


def export_source_files() -> int:
    """Export all eligible source files and return the number exported."""
    exported_count = 0

    with OUTPUT_FILE.open("w", encoding="utf-8") as output:
        for file in sorted(SRC_DIR.rglob("*")):
            if not file.is_file():
                continue

            if not should_export(file):
                continue

            write_file_content(file, output)
            exported_count += 1

    return exported_count


def print_summary(exported_count: int):
    """Print the extraction summary."""
    print("Done!")
    print(f"Project root : {ROOT}")
    print(f"Source       : {SRC_DIR}")
    print(f"Output       : {OUTPUT_FILE}")
    print(f"Files        : {exported_count}")


def main():
    if not validate_source_directory():
        return

    exported_count = export_source_files()
    print_summary(exported_count)


if __name__ == "__main__":
    main()