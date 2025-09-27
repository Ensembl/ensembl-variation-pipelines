import os
import shutil
import subprocess


def download_file(url: str, target_file: str) -> bool:
    """Download a remote file using wget.

    Args:
        url (str): Remote URL to download.
        target_dir (str): Target directory to write.

    Returns:
        int: Return code from the wget subprocess; non-zero indicates failure.
    """
    process = subprocess.run(
        ["wget", url, "-O", target_file],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if process.returncode != 0:
        print(
            f"[WARNING] Failed to download - {url}"
            + f"\tError = {process.stderr}"
        )
        if os.path.isfile(target_file):
            os.remove(target_file)

    return process.returncode == 0

def copy_file(source_file, target_file) -> bool:
    try:
        shutil.copy(source_file, target_file)
        return True
    except Exception as e:
        print(
            f"[WARNING] Copy failed - {e}\n"
            + f"\tsource file - {source_file}\n"
            + f"\ttarget directory - {target_file}"
        )
        return False