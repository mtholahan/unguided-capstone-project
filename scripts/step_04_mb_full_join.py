"""Step 04: MusicBrainz Full Join (Final, Data-Driven)
Performs enriched join of release, release_group, artist_credit.
Restores secondary types, derives is_soundtrack, and recovers artist names
via release_group fallback if direct artist_credit.id join fails.
"""

from base_step import BaseStep
import csv
from config import MB_CLEANSED_DIR, DATA_DIR, ROW_LIMIT, DEBUG_MODE
from utils import make_progress_bar  # ✅ unified progress helper
import random


class Step04MBFullJoin(BaseStep):
    def __init__(self, name="Step 04: MB Full Join"):
        super().__init__(name="Step 04: MB Full Join")
        self.data = {}

    def load_tsv(self, path):
        csv.field_size_limit(1_000_000)
        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f, delimiter="\t")
            rows = [row for row in reader]
        if not rows:
            return [], []
        header, data = rows[0], rows[1:]
        return header, data

    def safe_get_artist_name(self, row):
        """Best guess for artist_credit name column."""
        if not row:
            return "(unknown artist)"
        for idx in [1, 2, 3]:
            if len(row) > idx and row[idx].strip() not in ("", "\\N"):
                return row[idx].strip()
        return "(unknown artist)"

    def run(self):
        # --- Input paths (GUID-aware) ---
        guided_path = MB_CLEANSED_DIR / "release_enriched_guided.tsv"
        default_path = MB_CLEANSED_DIR / "release_enriched.tsv"

        # Prefer the GUID-enriched file if present
        release_path = guided_path if guided_path.exists() else default_path
        if guided_path.exists():
            self.logger.info("📘 Using GUID-enriched file: release_enriched_guided.tsv")
        else:
            self.logger.warning("⚠️ GUID-enriched file not found; falling back to release_enriched.tsv")

        # Other references
        rg_path = MB_CLEANSED_DIR / "release_group.tsv"
        rgst_path = MB_CLEANSED_DIR / "release_group_secondary_type.tsv"
        rgstj_path = MB_CLEANSED_DIR / "release_group_secondary_type_join.tsv"
        ac_path = MB_CLEANSED_DIR / "artist_credit.tsv"

        # --- Sanity checks ---
        for p in [release_path, rg_path, rgst_path, rgstj_path, ac_path]:
            if not p.exists():
                self.fail(f"Missing input file: {p}")
                return

        # --- Load tables ---
        self.logger.info("📥 Loading reference tables...")
        rg_header, rg_rows = self.load_tsv(rg_path)
        rgst_header, rgst_rows = self.load_tsv(rgst_path)
        rgstj_header, rgstj_rows = self.load_tsv(rgstj_path)
        ac_header, ac_rows = self.load_tsv(ac_path)

        # --- Log diagnostics ---
        self.logger.info(f"Artist_credit header: {ac_header}")
        self.logger.info("Sample artist_credit rows:")
        for row in ac_rows[:3]:
            self.logger.info(f"   {row}")

        # --- Restore secondary types ---
        st_map = {r[0]: r[1] for r in rgst_rows if len(r) >= 2}
        rg_secondary = {}
        for j in rgstj_rows:
            if len(j) >= 2:
                rg_id, st_id = j[0], j[1]
                rg_secondary.setdefault(rg_id, []).append(st_map.get(st_id, "").lower())
        self.logger.info(f"🔗 Restored secondary types for {len(rg_secondary):,} release_groups.")

        # --- Build maps ---
        ac_map = {r[0]: r for r in ac_rows if len(r) > 1}
        rg_map = {r[0]: r for r in rg_rows if len(r) > 1}
        self.logger.info(f"🎵 Loaded artist_credit: {len(ac_map):,}")
        self.logger.info(f"🎬 Loaded release_group: {len(rg_map):,}")

        # --- Prepare join output ---
        output_path = DATA_DIR / "joined_release_data.tsv"
        with open(release_path, encoding="utf-8") as fin:
            reader = csv.reader(fin, delimiter="\t")
            release_header = next(reader, [])

        header = release_header + [
            "release_group_name",
            "artist_credit_name",
            "secondary_types",
            "is_soundtrack",
        ]

        total_rows = sum(1 for _ in open(release_path, encoding="utf-8")) - 1
        effective_limit = ROW_LIMIT or total_rows
        joined_rows, soundtrack_count, recovered_artists = 0, 0, 0
        soundtrack_samples = []

        with open(release_path, encoding="utf-8") as fin, open(
            output_path, "w", encoding="utf-8", newline=""
        ) as fout:
            reader = csv.reader(fin, delimiter="\t")
            writer = csv.writer(fout, delimiter="\t")
            next(reader, None)
            writer.writerow(header)

            # ✅ Unified progress bar
            with make_progress_bar(total=min(total_rows, effective_limit),
                                   desc="Joining Releases",
                                   leave=True,
                                   unit="rows") as bar:
                for i, row in enumerate(reader, start=1):
                    if ROW_LIMIT and i > ROW_LIMIT:
                        break
                    if len(row) < 5:
                        continue

                    release_id = row[0]
                    artist_credit_id = row[3] if len(row) > 3 else None
                    release_group_id = row[4] if len(row) > 4 else None

                    # --- Artist join, with two-hop fallback ---
                    ac = ac_map.get(artist_credit_id, [])
                    rg = rg_map.get(release_group_id, [])
                    if not ac and len(rg) > 3:
                        rg_artist_credit_id = rg[3]
                        ac = ac_map.get(rg_artist_credit_id, [])
                        if ac:
                            recovered_artists += 1

                    ac_name = self.safe_get_artist_name(ac)
                    rg_name = rg[2] if len(rg) > 2 else ""

                    sec_types = rg_secondary.get(release_group_id, [])
                    is_soundtrack = any("soundtrack" in s for s in sec_types)

                    if is_soundtrack:
                        soundtrack_count += 1
                        if len(soundtrack_samples) < 10 or random.random() < 0.001:
                            soundtrack_samples.append(
                                {
                                    "release_id": release_id,
                                    "release_group": rg_name,
                                    "artist_credit": ac_name,
                                    "secondary_types": ", ".join(sec_types),
                                }
                            )

                    writer.writerow(
                        row + [rg_name, ac_name, ", ".join(sec_types), int(is_soundtrack)]
                    )

                    joined_rows += 1
                    if joined_rows % 100_000 == 0:
                        self.logger.info(f"Processed {joined_rows:,} joined rows...")
                    bar.update(1)

        soundtrack_pct = (soundtrack_count / joined_rows * 100) if joined_rows else 0
        self.logger.info(
            f"✅ [DONE] Joined {joined_rows:,} releases "
            f"({soundtrack_count:,} soundtracks = {soundtrack_pct:.1f}%) → {output_path.name}"
        )
        self.logger.info(f"🎯 Recovered {recovered_artists:,} artist names via release_group fallback.")

        unique_groups = len(set(r["release_group"] for r in soundtrack_samples))
        self.logger.info(
            f"🧩 Validation summary: {unique_groups} unique soundtrack groups sampled "
            f"({soundtrack_count:,} total soundtracks = {soundtrack_pct:.1f}%)."
        )

        if soundtrack_samples:
            self.logger.info("🎧 Validation sample (soundtrack matches):")
            for row in soundtrack_samples[:5]:
                self.logger.info(
                    f"   release_id={row['release_id']} | group='{row['release_group']}' | "
                    f"artist='{row['artist_credit']}' | types={row['secondary_types']}"
                )
        else:
            self.logger.warning("⚠️ No soundtrack rows found during validation.")


if __name__ == "__main__":
    step = Step04MBFullJoin()
    step.run()
