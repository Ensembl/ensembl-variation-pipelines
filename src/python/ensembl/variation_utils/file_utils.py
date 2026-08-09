import os
import subprocess

def is_bgzip(file: str) -> bool:
    with open(file, "rb") as f:
        gzip = (f.read(2) == b'\x1f\x8b')
        f.seek(12)
        bgzip_extra_field = (f.read(2) == b'BC')

        return gzip and bgzip_extra_field
    
def ungzip_file(file: str) -> str:
    """Decompress a gzip file in place and return the decompressed filename.

    Args:
        file (str): Path to the .gz file.

    Returns:
        str: Decompressed filename (original minus '.gz').

    Raises:
        FileNotFoundError: If the input file does not exist.
        SystemExit: Exits with an error if gzip fails.
    """
    if not os.path.isfile(file):
        raise FileNotFoundError(f"Could not un-gzip. File does not exist - {file}")

    process = subprocess.run(
        ["gzip", "-df", file], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    if process.returncode != 0:
        print(f"[ERROR] Could not uncompress file - {file}")
        exit(1)

    return file[:-3]


def bgzip_file(file: str) -> str:
    """Compress a file using bgzip and return the compressed filename.

    Args:
        file (str): Path to a file to compress.

    Returns:
        str: Path to the compressed file (appended with '.gz').

    Raises:
        FileNotFoundError: If the input file does not exist.
        SystemExit: Exits with an error if bgzip fails.
    """
    if not os.path.isfile(file):
        raise FileNotFoundError(f"Could not run bgzip. File does not exist - {file}")

    process = subprocess.run(
        ["bgzip", "-f", file], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    if process.returncode != 0:
        print(f"[ERROR] Could not bgzip file - {file}")
        exit(1)

    return file + ".gz"
