from pathlib import Path
import fnmatch

# Project root (parent of the scripts directory)
ROOT = Path(__file__).resolve().parent.parent

# Output directory
OUTPUT_DIR = ROOT / "docs"
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / "project_contents.txt"

SEPARATOR = "\n" + "=" * 120 + "\n\n"


# -----------------------------------------------------------------------------
# Files to exclude
# -----------------------------------------------------------------------------

EXCLUDED_EXTENSIONS = {
    ".pdf",
}

EXCLUDED_FILE_PATTERNS = {
    "*.java",  # handled separately because we allow non-test java files
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


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def is_test_file(file_path: Path) -> bool:
    """
    Ignore Java test files and files inside test directories.
    """

    relative = file_path.relative_to(ROOT)

    for part in relative.parts:
        lower = part.lower()

        if lower in {
            "test",
            "tests",
            "src-test",
            "test-output",
        }:
            return True

    name = file_path.name.lower()

    if file_path.suffix.lower() == ".java":
        return (
            name.endswith("test.java")
            or name.endswith("tests.java")
            or name.startswith("test")
        )

    return False



def is_gitignored(path: Path) -> bool:
    """
    Small .gitignore matcher supporting the patterns above.
    """

    relative = path.relative_to(ROOT).as_posix()

    for pattern in GITIGNORE_PATTERNS:

        # directory rule
        if pattern.endswith("/"):
            directory = pattern.rstrip("/")

            if (
                relative == directory
                or relative.startswith(directory + "/")
                or f"/{directory}/" in relative
            ):
                return True

        # wildcard rule
        if "*" in pattern:
            if fnmatch.fnmatch(relative, pattern):
                return True

            if fnmatch.fnmatch(path.name, pattern):
                return True

        # exact match
        if relative == pattern:
            return True

    return False



def should_export(file_path: Path) -> bool:

    if is_gitignored(file_path):
        return False

    if is_test_file(file_path):
        return False

    if file_path.suffix.lower() in EXCLUDED_EXTENSIONS:
        return False

    return True



def write_file_content(file_path: Path, output):

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



# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

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