from pathlib import Path
import fnmatch

# Project root (parent of the scripts directory)
ROOT = Path(__file__).resolve().parent.parent

# Output directory
OUTPUT_DIR = ROOT / "docs"
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / "project_java_contents.txt"

SEPARATOR = "\n" + "=" * 120 + "\n\n"

# -----------------------------------------------------------------------------
# Directories to completely skip while iterating
# -----------------------------------------------------------------------------
EXCLUDED_DIRECTORIES = {
    "gradle",
    "docs",
    "build",
    ".idea",
    ".gradle",
    "target",
    ".vscode",
    ".settings",
    "out",
    "logs",
}

# -----------------------------------------------------------------------------
# .gitignore rules (copied manually)
# -----------------------------------------------------------------------------
GITIGNORE_PATTERNS = [
    # Java / Maven
    "target/",
    "pom.xml.tag",
    "pom.xml.releaseBackup",
    "pom.xml.versionsBackup",
    "pom.xml.next",
    "release.properties",
    "dependency-reduced-pom.xml",
    "buildNumber.properties",
    ".mvn/timing.properties",
    ".mvn/wrapper/maven-wrapper.jar",

    # Eclipse
    ".project",
    ".classpath",

    # IDE
    ".idea/",
    "*.iml",
    ".vscode/",
    ".settings/",
    "*.swp",
    "*.swo",
    "*.iws",
    "*.ipr",
    "out/",

    # Logs
    "logs/",
    "*.log",
    "*.processing",

    # OS
    ".DS_Store",
    "Thumbs.db",

    # Runtime
    "config/rules.yaml",

    # Docker
    ".env",

    # Testing
    "target/surefire-reports/",
    "target/failsafe-reports/",
    "test-output/",

    # Development
    "docs/devGuide/",

    # Temporary
    "*.tmp",
    "*.bak",
    "*.orig",
    "*.rej",

    # Module specific
    "**/build/pom.xml",
]


def is_excluded_directory(path: Path) -> bool:
    """
    Ignore excluded directories and hidden directories.
    """
    relative = path.relative_to(ROOT)

    excluded = {
        directory.lower()
        for directory in EXCLUDED_DIRECTORIES
    }

    for part in relative.parts:
        if part.lower() in excluded:
            return True

        if part.startswith("."):
            return True

    return False


def is_test_file(file_path: Path) -> bool:
    """
    Ignore Java test files and anything located inside a test directory.
    """
    relative = file_path.relative_to(ROOT)

    test_directories = {
        "test",
        "tests",
        "src-test",
        "test-output",
    }

    # Ignore anything inside a test directory.
    for part in relative.parts[:-1]:
        if part.lower() in test_directories:
            return True

    name = file_path.name.lower()

    # Ignore common Java test naming conventions.
    return (
        name.startswith("test")
        or name.endswith("test.java")
        or name.endswith("tests.java")
    )


def is_gitignored(path: Path) -> bool:
    """
    Very small .gitignore matcher supporting the patterns above.
    """
    relative = path.relative_to(ROOT).as_posix()

    for pattern in GITIGNORE_PATTERNS:
        if pattern.endswith("/"):
            directory = pattern.rstrip("/")

            if (
                relative == directory
                or relative.startswith(directory + "/")
                or f"/{directory}/" in relative
            ):
                return True

        if "*" in pattern:
            if fnmatch.fnmatch(relative, pattern):
                return True

            if fnmatch.fnmatch(path.name, pattern):
                return True

        if relative == pattern:
            return True

    return False


def should_export(file_path: Path) -> bool:
    """
    Export only non-test Java source files.
    """

    # Ignore everything that is not a Java file.
    if file_path.suffix.lower() != ".java":
        return False

    # Ignore excluded directories.
    if is_excluded_directory(file_path.parent):
        return False

    # Ignore gitignored files/directories.
    if is_gitignored(file_path):
        return False

    # Ignore test files/directories.
    if is_test_file(file_path):
        return False

    return True


def write_file_content(file_path: Path, output):
    """
    Write filename, path, and file contents to the output file.
    """
    try:
        content = file_path.read_text(encoding="utf-8")

    except UnicodeDecodeError:
        try:
            content = file_path.read_text(encoding="latin-1")
        except Exception:
            print(f"Skipping unreadable file: {file_path}")
            return

    except Exception as e:
        print(f"Could not read {file_path}: {e}")
        return

    relative_path = file_path.relative_to(ROOT)

    output.write(SEPARATOR)
    output.write(f"File: {file_path.name}\n")
    output.write(f"Path: {relative_path}\n")
    output.write(SEPARATOR)
    output.write(content)

    if not content.endswith("\n"):
        output.write("\n")


def main():
    with OUTPUT_FILE.open("w", encoding="utf-8") as output:

        for file in sorted(ROOT.rglob("*")):

            if not file.is_file():
                continue

            if should_export(file):
                write_file_content(file, output)

    print(f"Done! Output written to:\n{OUTPUT_FILE}")


if __name__ == "__main__":
    main()