#!/usr/bin/env python3

# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Compare the ProteinFunction pipeline's SQLite output against a variation
MySQL database, expanding the prediction matrices and reporting where they
differ.

Both writers store Bio::EnsEMBL::Variation::ProteinFunctionPredictionMatrix
->serialize: a gzip stream whose contents are the three bytes "VEP" followed by
one little-endian unsigned short per (peptide position, mutant amino acid), 20
amino acids per position in the order A C D E F G H I K L M N P Q R S T V W Y.
0xFFFF means no prediction; otherwise the top two bits hold the qualitative
prediction and the low ten bits the score times 1000.

Matrices are compared cell by cell on the *decompressed* bytes. That matters:
gzip output depends on the zlib version and compression level, so two blobs can
differ byte for byte while encoding identical predictions. Comparing the
compressed bytes alone would report differences that do not exist.

To stay fast on a whole-genome run, the compressed digests are compared first.
Byte-identical compressed blobs cannot decode differently, so those records are
settled without transferring anything. Only the records whose digests disagree
have their matrices fetched and expanded. Use --expand-all to expand every
record regardless.

Differences are reported per record as a cigar-like run-length string over
peptide positions, where = is a position whose 20 cells all agree and X is a
position where at least one differs, followed by the individual cells. Mutant
amino acids are named with their reference residue when --peptides supplies the
translated FASTA, so a cell reads A1C rather than 1C.

Scope: the SQLite file carries only the prediction matrices from the run that
produced it, and has no equivalent of protein_function_predictions_attrib, so
the per-position evidence stored alongside each matrix in MySQL
(sequence_number, conservation_score) is not compared.

Exit status is 0 when every prediction in the SQLite file is present in MySQL
and decodes identically, and 1 otherwise. Predictions that exist only in MySQL
are reported but do not affect the exit status, because a run normally covers a
subset of what the database already holds.
"""

import argparse
import gzip
import hashlib
import os
import sqlite3
import stat
import struct
import subprocess
import sys
import tempfile
import time

# Analysis attrib IDs the pipeline writes into the SQLite file, hardcoded in
# bin/store_sift_scores.pl and bin/store_polyphen_scores.pl. A database whose
# attrib table disagrees would leave the SQLite analysis column mislabelled.
EXPECTED_ANALYSIS = {
    267: "sift",
    268: "polyphen_humvar",
    269: "polyphen_humdiv",
}

# Both PolyPhen models decode with the polyphen prediction table.
DECODE_TABLE = {267: "sift", 268: "polyphen", 269: "polyphen"}

ALL_AAS = list("ACDEFGHIKLMNPQRSTVWY")
HEADER = b"VEP"
NO_PREDICTION = 0xFFFF
PRED_BITS = 2
SCORE_MASK = (1 << 10) - 1

VAL_TO_PREDICTION = {
    "sift": {
        0: "tolerated",
        1: "deleterious",
        2: "tolerated - low confidence",
        3: "deleterious - low confidence",
    },
    "polyphen": {
        0: "probably damaging",
        1: "possibly damaging",
        2: "benign",
        3: "unknown",
    },
}

BATCH = 200


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compare SQLite protein function predictions against MySQL",
    )
    parser.add_argument("sqlite", help="SQLite file written by the pipeline")
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", default="3306")
    parser.add_argument("--user", required=True)
    parser.add_argument(
        "--pass",
        dest="password",
        default=os.environ.get("MYSQL_PWD", ""),
        help="MySQL password; defaults to $MYSQL_PWD. Note that a command line "
             "argument is visible to other users in the process list",
    )
    parser.add_argument("--database", required=True)
    parser.add_argument(
        "--mapping",
        help="translation_mapping.tsv from the run, so differences can be "
             "reported against transcript stable IDs. If omitted, the "
             "translation_mapping table is read from MySQL when present",
    )
    parser.add_argument(
        "--peptides",
        help="protein FASTA from the run, so that differing cells can be named "
             "with their reference residue, e.g. A1C instead of 1C. Either the "
             "published <name>_translated.fa or the deduplicated FASTA will do",
    )
    parser.add_argument(
        "--analysis",
        action="append",
        choices=sorted(EXPECTED_ANALYSIS.values()),
        help="restrict to one analysis; repeatable (default: all)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="compare at most this many SQLite rows, for a quick check",
    )
    parser.add_argument(
        "--expand-all",
        action="store_true",
        help="decode every matrix, not only those whose digests disagree",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=20,
        help="how many differing records to describe in full (default: 20)",
    )
    parser.add_argument(
        "--max-cells",
        type=int,
        default=10,
        help="how many differing cells to list per record (default: 10)",
    )
    parser.add_argument(
        "--show",
        metavar="MD5:ANALYSIS",
        help="decode and print one record from both sides, then exit",
    )
    parser.add_argument("--out-dir", help="write difference reports here")
    parser.add_argument("--mysql", default="mysql", help="mysql client to use")
    parser.add_argument(
        "--no-progress", action="store_true", help="suppress the progress bar"
    )
    return parser.parse_args()


class Progress:
    """Minimal progress reporting that behaves on a terminal and in a log.

    On a terminal it rewrites one line; redirected it prints a line every 10%,
    so a log file does not fill with carriage returns.
    """

    def __init__(self, label, total, enabled=True):
        self.label = label
        self.total = total
        self.done = 0
        self.tty = sys.stderr.isatty()
        self.enabled = enabled and total > 0
        self.last = 0.0
        self.last_decile = -1
        if self.enabled:
            self.render(force=True)

    def advance(self, count=1):
        self.done += count
        if not self.enabled:
            return
        now = time.monotonic()
        if self.tty:
            if now - self.last >= 0.2 or self.done >= self.total:
                self.render()
                self.last = now
        else:
            decile = int(10 * self.done / self.total)
            if decile > self.last_decile:
                self.last_decile = decile
                self.render(newline=True)

    def render(self, newline=False, force=False):
        pct = 100.0 * self.done / self.total if self.total else 100.0
        if self.tty and not newline:
            width = 30
            filled = int(width * self.done / self.total) if self.total else width
            bar = "#" * filled + "-" * (width - filled)
            end = "\n" if self.done >= self.total and not force else ""
            sys.stderr.write(
                f"\r  {self.label}: [{bar}] {self.done:,}/{self.total:,} "
                f"({pct:.0f}%){end}"
            )
        else:
            sys.stderr.write(
                f"  {self.label}: {self.done:,}/{self.total:,} ({pct:.0f}%)\n"
            )
        sys.stderr.flush()

    def finish(self):
        if self.enabled and self.done < self.total:
            self.done = self.total
            self.render(newline=not self.tty)


class MySQL:
    """Runs queries through the mysql client, so no driver is needed.

    Credentials go into an option file readable only by this user, so they are
    not handed to the client on its command line.
    """

    def __init__(self, client, host, port, user, database, password):
        self.client = client
        fd, self.defaults = tempfile.mkstemp(prefix="pfp-cmp-", suffix=".cnf")
        with os.fdopen(fd, "w") as handle:
            handle.write("[client]\n")
            handle.write(f"host={host}\n")
            handle.write(f"port={port}\n")
            handle.write(f"user={user}\n")
            handle.write(f"password={password}\n")
            handle.write(f"database={database}\n")
        os.chmod(self.defaults, stat.S_IRUSR | stat.S_IWUSR)

    def close(self):
        if self.defaults and os.path.exists(self.defaults):
            os.unlink(self.defaults)
            self.defaults = None

    def rows(self, sql, tolerate_failure=False):
        command = [
            self.client,
            f"--defaults-extra-file={self.defaults}",
            "--batch",
            "--skip-column-names",
            "--execute",
            sql,
        ]
        try:
            result = subprocess.run(
                command, capture_output=True, text=True, check=True
            )
        except FileNotFoundError:
            sys.exit(f"ERROR: mysql client not found: {self.client}")
        except subprocess.CalledProcessError as error:
            if tolerate_failure:
                return
            sys.exit(f"ERROR: mysql query failed:\n{error.stderr.strip()}")

        for line in result.stdout.splitlines():
            if line:
                yield line.split("\t")


def decode_matrix(blob, attrib_id):
    """Expand a serialized matrix into {(pos, aa): (prediction, score)}.

    Returns (peptide_length, cells). Cells with no prediction are omitted.
    """
    try:
        raw = gzip.decompress(blob)
    except (OSError, EOFError) as error:
        raise ValueError(f"not a gzip stream: {error}") from error

    if not raw.startswith(HEADER):
        raise ValueError("missing the VEP header; matrix looks corrupted")

    body = raw[len(HEADER):]
    if len(body) % (2 * len(ALL_AAS)):
        raise ValueError(f"body of {len(body)} bytes is not a whole number of rows")

    length = len(body) // 2 // len(ALL_AAS)
    table = VAL_TO_PREDICTION[DECODE_TABLE[attrib_id]]
    cells = {}
    for pos in range(1, length + 1):
        base = (pos - 1) * len(ALL_AAS) * 2
        for index, aa in enumerate(ALL_AAS):
            (val,) = struct.unpack_from("<H", body, base + index * 2)
            if val == NO_PREDICTION:
                continue
            prediction = table.get(val >> (16 - PRED_BITS), "?")
            cells[(pos, aa)] = (prediction, (val & SCORE_MASK) / 1000)
    return length, cells


def cigar_like(positions_differing, length):
    """Run-length string over positions: = agrees, X has a differing cell."""
    if not length:
        return ""
    parts = []
    run_char = None
    run_length = 0
    for pos in range(1, length + 1):
        char = "X" if pos in positions_differing else "="
        if char == run_char:
            run_length += 1
        else:
            if run_char:
                parts.append(f"{run_length}{run_char}")
            run_char, run_length = char, 1
    parts.append(f"{run_length}{run_char}")
    return "".join(parts)


def diff_matrices(sqlite_blob, mysql_blob, attrib_id):
    """Compare two serialized matrices.

    Returns (identical, description, cigar_string, cell_differences).
    """
    try:
        sq_len, sq_cells = decode_matrix(sqlite_blob, attrib_id)
    except ValueError as error:
        return False, f"sqlite matrix unreadable: {error}", "", []
    try:
        my_len, my_cells = decode_matrix(mysql_blob, attrib_id)
    except ValueError as error:
        return False, f"mysql matrix unreadable: {error}", "", []

    differences = []
    for key in sorted(set(sq_cells) | set(my_cells)):
        left = sq_cells.get(key)
        right = my_cells.get(key)
        if left != right:
            differences.append((key[0], key[1], left, right))

    if not differences and sq_len == my_len:
        return True, "identical predictions", "", []

    note = ""
    if sq_len != my_len:
        note = f"peptide length differs: sqlite {sq_len}, mysql {my_len}"

    positions = {pos for pos, _, _, _ in differences}
    return False, note, cigar_like(positions, max(sq_len, my_len)), differences


def describe_cell(value):
    if value is None:
        return "no prediction"
    prediction, score = value
    return f"{prediction} ({score:g})"


def analysis_filter(names):
    """SQL and SQLite predicates restricting the comparison to some analyses."""
    if not names:
        return "", ""
    ids = sorted(i for i, name in EXPECTED_ANALYSIS.items() if name in names)
    joined = ",".join(str(i) for i in ids)
    return (
        f" AND p.analysis_attrib_id IN ({joined})",
        f" WHERE analysis IN ({joined})",
    )


def check_attrib_mapping(db):
    """Warn if the database's analysis attrib IDs differ from the hardcoded ones."""
    problems = []
    for attrib_id, value in db.rows(
        "SELECT DISTINCT p.analysis_attrib_id, a.value "
        "FROM protein_function_predictions p "
        "JOIN attrib a ON a.attrib_id = p.analysis_attrib_id"
    ):
        attrib_id = int(attrib_id)
        expected = EXPECTED_ANALYSIS.get(attrib_id)
        if expected is None:
            problems.append(
                f"  attrib {attrib_id} ({value}) is not one the pipeline writes"
            )
        elif expected != value:
            problems.append(
                f"  attrib {attrib_id} is '{value}' here but the pipeline "
                f"writes it as '{expected}'"
            )

    if problems:
        print("WARNING: analysis attrib IDs do not match the pipeline's assumptions.")
        print("         The SQLite file labels predictions by hardcoded ID, so its")
        print("         analysis column may be mislabelled against this database.")
        print("\n".join(problems))
        print()


def load_mapping(path, db):
    """Map each translation MD5 to the transcript stable IDs that share it."""
    mapping = {}
    if path:
        if not os.path.exists(path):
            sys.exit(f"ERROR: no such mapping file: {path}")
        with open(path) as handle:
            for line in handle:
                fields = line.rstrip("\n").split("\t")
                if len(fields) >= 2:
                    mapping.setdefault(fields[1], []).append(fields[0])
    else:
        # The pipeline writes this table in online mode; absent otherwise.
        for stable_id, md5 in db.rows(
            "SELECT stable_id, md5 FROM translation_mapping", tolerate_failure=True
        ):
            mapping.setdefault(md5, []).append(stable_id)
    for md5 in mapping:
        mapping[md5].sort()
    return mapping


def load_peptides(path):
    """Map each translation MD5 to its peptide sequence.

    Hashing matches bin/dedup_translations.pl - stop codons removed and the
    sequence uppercased - so that the MD5s line up with the stored ones.
    """
    if not path:
        return {}
    if not os.path.exists(path):
        sys.exit(f"ERROR: no such peptide FASTA: {path}")

    peptides = {}

    def keep(chunks):
        if not chunks:
            return
        sequence = "".join(chunks).replace("*", "").upper()
        if sequence:
            peptides.setdefault(hashlib.md5(sequence.encode()).hexdigest(), sequence)

    chunks = []
    with open(path) as handle:
        for line in handle:
            if line.startswith(">"):
                keep(chunks)
                chunks = []
            else:
                chunks.append(line.strip())
    keep(chunks)
    return peptides


def reference_aa(md5, position, peptides):
    """The reference residue at a position, or None when it is not known."""
    sequence = peptides.get(md5)
    if not sequence or not 1 <= position <= len(sequence):
        return None
    return sequence[position - 1]


def substitution(md5, position, mutant, peptides):
    """Name a cell as refPOSalt where the reference residue is known."""
    reference = reference_aa(md5, position, peptides)
    return f"{reference or ''}{position}{mutant}"


def label(md5, mapping):
    """Describe a record by transcript stable ID where one is known."""
    ids = mapping.get(md5)
    if not ids:
        return md5
    if len(ids) == 1:
        return f"{ids[0]} ({md5})"
    return f"{ids[0]} +{len(ids) - 1} more ({md5})"


def read_sqlite(path, predicate, limit, progress_enabled):
    """Map (md5, analysis) to a digest, byte length and the blob itself."""
    if not os.path.exists(path):
        sys.exit(f"ERROR: no such SQLite file: {path}")

    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        if not connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='predictions'"
        ).fetchall():
            sys.exit(f"ERROR: {path} has no 'predictions' table")

        count_sql = f"SELECT COUNT(*) FROM predictions{predicate}"
        total = connection.execute(count_sql).fetchone()[0]
        if limit:
            total = min(total, limit)

        sql = f"SELECT md5, analysis, matrix FROM predictions{predicate}"
        if limit:
            sql += f" LIMIT {int(limit)}"

        progress = Progress("reading sqlite", total, progress_enabled)
        found = {}
        for md5, analysis, matrix in connection.execute(sql):
            found[(md5, int(analysis))] = (
                hashlib.md5(matrix).hexdigest(),
                len(matrix),
                matrix,
            )
            progress.advance()
        progress.finish()
        return found
    finally:
        connection.close()


def read_mysql_digests(db, predicate, progress_enabled):
    """Map (translation_md5, analysis_attrib_id) to a digest and byte length."""
    total = 0
    for row in db.rows(
        "SELECT COUNT(*) FROM protein_function_predictions p "
        f"WHERE 1 = 1{predicate}"
    ):
        total = int(row[0])

    progress = Progress("reading mysql", total, progress_enabled)
    found = {}
    for md5, attrib_id, digest, length in db.rows(
        "SELECT t.translation_md5, p.analysis_attrib_id, "
        "MD5(p.prediction_matrix), LENGTH(p.prediction_matrix) "
        "FROM protein_function_predictions p "
        "JOIN translation_md5 t ON t.translation_md5_id = p.translation_md5_id "
        f"WHERE 1 = 1{predicate}"
    ):
        found[(md5, int(attrib_id))] = (digest, int(length))
        progress.advance()
    progress.finish()
    return found


def fetch_mysql_matrices(db, keys, progress_enabled):
    """Fetch the raw matrices for specific keys, in batches."""
    keys = sorted(keys)
    progress = Progress("fetching matrices", len(keys), progress_enabled)
    matrices = {}
    for start in range(0, len(keys), BATCH):
        batch = keys[start:start + BATCH]
        tuples = ",".join(f"('{md5}',{attrib})" for md5, attrib in batch)
        for md5, attrib_id, hex_blob in db.rows(
            "SELECT t.translation_md5, p.analysis_attrib_id, "
            "HEX(p.prediction_matrix) "
            "FROM protein_function_predictions p "
            "JOIN translation_md5 t ON t.translation_md5_id = p.translation_md5_id "
            f"WHERE (t.translation_md5, p.analysis_attrib_id) IN ({tuples})"
        ):
            matrices[(md5, int(attrib_id))] = bytes.fromhex(hex_blob)
        progress.advance(len(batch))
    progress.finish()
    return matrices


def name_of(attrib_id):
    return EXPECTED_ANALYSIS.get(attrib_id, f"attrib_{attrib_id}")


def show_one(db, sqlite_path, key, mapping, peptides):
    """Decode and print one record from both sides."""
    try:
        md5, name = key.rsplit(":", 1)
    except ValueError:
        sys.exit("ERROR: --show expects MD5:ANALYSIS, e.g. 7f3a...:sift")

    ids = [i for i, value in EXPECTED_ANALYSIS.items() if value == name]
    if not ids:
        sys.exit(f"ERROR: unknown analysis '{name}'")
    attrib_id = ids[0]

    connection = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
    try:
        row = connection.execute(
            "SELECT matrix FROM predictions WHERE md5 = ? AND analysis = ?",
            (md5, attrib_id),
        ).fetchone()
    finally:
        connection.close()

    fetched = fetch_mysql_matrices(db, [(md5, attrib_id)], False)
    print(f"{label(md5, mapping)}  {name}")
    for source, blob in (("sqlite", row[0] if row else None),
                         ("mysql", fetched.get((md5, attrib_id)))):
        if blob is None:
            print(f"  {source}: absent")
            continue
        try:
            length, cells = decode_matrix(blob, attrib_id)
        except ValueError as error:
            print(f"  {source}: unreadable ({error})")
            continue
        print(f"  {source}: {len(blob)} bytes, peptide length {length}, "
              f"{len(cells):,} predictions")
        for (pos, aa), value in sorted(cells.items())[:20]:
            print(f"    {substitution(md5, pos, aa, peptides)}: "
                  f"{describe_cell(value)}")
        if len(cells) > 20:
            print(f"    ... {len(cells) - 20:,} more")


def counts_by_analysis(rows):
    counts = {}
    for _, attrib_id in rows:
        counts[attrib_id] = counts.get(attrib_id, 0) + 1
    return counts


def main():
    args = parse_args()
    show_progress = not args.no_progress

    db = MySQL(
        args.mysql, args.host, args.port, args.user, args.database, args.password
    )
    try:
        mapping = load_mapping(args.mapping, db)
        peptides = load_peptides(args.peptides)

        if args.show:
            show_one(db, args.sqlite, args.show, mapping, peptides)
            return 0

        mysql_predicate, sqlite_predicate = analysis_filter(args.analysis)
        check_attrib_mapping(db)

        sqlite_rows = read_sqlite(
            args.sqlite, sqlite_predicate, args.limit, show_progress
        )
        mysql_rows = read_mysql_digests(db, mysql_predicate, show_progress)

        shared = set(sqlite_rows) & set(mysql_rows)
        missing = set(sqlite_rows) - set(mysql_rows)
        extra = set(mysql_rows) - set(sqlite_rows)

        # Byte-identical compressed blobs cannot decode differently, so only
        # the rest need expanding - unless the caller asked for everything.
        candidates = {
            key for key in shared
            if args.expand_all or sqlite_rows[key][0] != mysql_rows[key][0]
        }
        fetched = (
            fetch_mysql_matrices(db, candidates, show_progress)
            if candidates else {}
        )
    finally:
        db.close()

    if show_progress and candidates:
        sys.stderr.write("")

    same_bytes = len(shared) - len(candidates)
    identical = []
    differing = []
    unreadable = []
    progress = Progress("expanding matrices", len(candidates), show_progress)
    for key in sorted(candidates):
        mysql_blob = fetched.get(key)
        if mysql_blob is None:
            unreadable.append((key, "mysql matrix could not be fetched", "", []))
            progress.advance()
            continue
        ok, note, shape, cells = diff_matrices(
            sqlite_rows[key][2], mysql_blob, key[1]
        )
        if ok:
            identical.append(key)
        elif not shape and not cells:
            unreadable.append((key, note, shape, cells))
        else:
            differing.append((key, note, shape, cells))
        progress.advance()
    progress.finish()

    sqlite_counts = counts_by_analysis(sqlite_rows)
    mysql_counts = counts_by_analysis(mysql_rows)

    print()
    print("predictions per analysis")
    print(f"  {'analysis':<20}{'sqlite':>12}{'mysql':>12}")
    for attrib_id in sorted(set(sqlite_counts) | set(mysql_counts)):
        print(
            f"  {name_of(attrib_id):<20}"
            f"{sqlite_counts.get(attrib_id, 0):>12,}"
            f"{mysql_counts.get(attrib_id, 0):>12,}"
        )
    print(f"  {'total':<20}{len(sqlite_rows):>12,}{len(mysql_rows):>12,}")
    print()

    print("comparison")
    print(f"  identical, compressed bytes match : {same_bytes:,}")
    print(f"  identical, matrices expanded      : {len(identical):,}")
    print(f"  DIFFERENT predictions             : {len(differing):,}")
    print(f"  unreadable matrices               : {len(unreadable):,}")
    print(f"  in sqlite but missing in mysql    : {len(missing):,}")
    print(f"  in mysql only (not this run)      : {len(extra):,}")
    print()

    if differing:
        print(f"differing records (first {min(len(differing), args.max_records)}):")
        for (md5, attrib_id), note, shape, cells in differing[:args.max_records]:
            print(f"  {label(md5, mapping)}  {name_of(attrib_id)}")
            if note:
                print(f"    {note}")
            if shape:
                print(f"    cigar-like: {shape}")
            for pos, aa, left, right in cells[:args.max_cells]:
                print(
                    f"    {substitution(md5, pos, aa, peptides)}: "
                    f"sqlite={describe_cell(left)}  "
                    f"mysql={describe_cell(right)}"
                )
            if len(cells) > args.max_cells:
                print(f"    ... {len(cells) - args.max_cells:,} more cells differ")
        print()

    if unreadable:
        print(f"unreadable records (first {min(len(unreadable), 5)}):")
        for (md5, attrib_id), note, _, _ in unreadable[:5]:
            print(f"  {label(md5, mapping)}  {name_of(attrib_id)}: {note}")
        print()

    if missing:
        print(f"missing from mysql (first {min(len(missing), 5)}):")
        for md5, attrib_id in sorted(missing)[:5]:
            print(f"  {label(md5, mapping)}  {name_of(attrib_id)}")
        print()

    if args.out_dir:
        os.makedirs(args.out_dir, exist_ok=True)
        print("difference reports")
        if differing:
            path = os.path.join(args.out_dir, "matrix_differs.tsv")
            with open(path, "w") as handle:
                handle.write(
                    "stable_ids\tmd5\tanalysis\tnote\tcigar_like\t"
                    "cells_differing\tposition\treference_aa\t"
                    "mutant_aa\tsqlite\tmysql\n"
                )
                for (md5, attrib_id), note, shape, cells in differing:
                    ids = ",".join(mapping.get(md5, [])) or "-"
                    for pos, aa, left, right in cells:
                        reference = reference_aa(md5, pos, peptides) or "-"
                        handle.write(
                            f"{ids}\t{md5}\t{name_of(attrib_id)}\t{note or '-'}\t"
                            f"{shape or '-'}\t{len(cells)}\t{pos}\t{reference}\t"
                            f"{aa}\t{describe_cell(left)}\t{describe_cell(right)}\n"
                        )
            print(f"  wrote {path}")
        for filename, keys in (
            ("missing_from_mysql.tsv", sorted(missing)),
            ("mysql_only.tsv", sorted(extra)),
        ):
            if not keys:
                continue
            path = os.path.join(args.out_dir, filename)
            with open(path, "w") as handle:
                handle.write("stable_ids\tmd5\tanalysis\n")
                for md5, attrib_id in keys:
                    ids = ",".join(mapping.get(md5, [])) or "-"
                    handle.write(f"{ids}\t{md5}\t{name_of(attrib_id)}\n")
            print(f"  wrote {path} ({len(keys)} rows)")
        print()

    if differing or missing or unreadable:
        print("RESULT: the databases disagree.")
        if differing:
            print(f"  {len(differing):,} records decode to different predictions")
        if unreadable:
            print(f"  {len(unreadable):,} records could not be decoded")
        if missing:
            print(f"  {len(missing):,} records in the SQLite file are not in MySQL")
        return 1

    if not sqlite_rows:
        print("RESULT: the SQLite file has no predictions to compare.")
        return 1

    print("RESULT: every prediction in the SQLite file matches MySQL.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
