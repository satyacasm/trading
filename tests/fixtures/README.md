# Test fixtures

Real exchange files, trimmed to ~50 rows, committed deliberately (spec D15).

**Never hand-write a fixture.** Synthetic CSVs test the author's belief about a
format rather than the format itself, which is how parser bugs survive review.

To regenerate from live sources:

    ./scripts/fetch_recon_samples.sh 20260813

then trim with `scripts/trim_fixture.py`, preserving the header, at least one
ordinary row, and every edge case named in
`docs/data-formats/eod-source-formats.md` for that source.
