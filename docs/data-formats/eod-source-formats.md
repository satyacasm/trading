# EOD Source Formats — verified reference

**Verified:** 2026-08-14, by downloading live files. Every column list below was read off a real file, not from documentation.
**Sample files:** `data/raw/_recon/` (gitignored; regenerate with `scripts/fetch_recon_samples.sh`)

This document is the source of truth for parser implementations. If a parser disagrees with this file, one of them is wrong — check against a real download before assuming it's this document.

---

## Summary: three parsers, not five

The Phase 0 spec originally assumed five parser variants. Live inspection shows **three**:

| Parser | Covers | Why |
|---|---|---|
| `UdiffParser` | NSE CM · NSE FO · **BSE CM** | All three emit a byte-identical 34-column header |
| `NseLegacyCmParser` | NSE CM before UDiFF (~pre-Jul 2024) | Completely different 13-column format |
| `AmfiNavParser` | AMFI daily NAVs | Hierarchical semicolon-delimited text, not CSV |

---

## 1. UDiFF — NSE CM, NSE FO, BSE CM

### Access

| Source | URL |
|---|---|
| NSE CM | `https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{YYYYMMDD}_F_0000.csv.zip` |
| NSE FO | `https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{YYYYMMDD}_F_0000.csv.zip` |
| BSE CM | `https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{YYYYMMDD}_F_0000.CSV` |

**NSE requires cookie priming.** A bare request returns 403. The working sequence is:
1. `GET https://www.nseindia.com` with a browser `User-Agent`, keeping cookies.
2. `GET` the archive URL reusing those cookies, with `Referer: https://www.nseindia.com/`.

NSE files are **zipped**; BSE is **plain CSV**.

### Line endings differ

NSE uses `LF`. **BSE uses `CRLF`.** The headers are identical only after stripping `\r`. A parser that compares headers byte-for-byte will reject BSE. Normalise line endings before parsing.

### Schema — 34 columns, identical across all three

```
 1 TradDt                 12 StrkPric              23 OpnIntrst
 2 BizDt                  13 OptnTp                24 ChngInOpnIntrst
 3 Sgmt                   14 FinInstrmNm           25 TtlTradgVol
 4 Src                    15 OpnPric               26 TtlTrfVal
 5 FinInstrmTp            16 HghPric               27 TtlNbOfTxsExctd
 6 FinInstrmId            17 LwPric                28 SsnId
 7 ISIN                   18 ClsPric               29 NewBrdLotQty
 8 TckrSymb               19 LastPric              30 Rmks
 9 SctySrs                20 PrvsClsgPric          31 Rsvd1
10 XpryDt                 21 UndrlygPric           32 Rsvd2
11 FininstrmActlXpryDt    22 SttlmPric             33 Rsvd3
                                                   34 Rsvd4
```

`Rmks` and `Rsvd1`–`Rsvd4` were empty in every sampled row. Do not map them.

### Discriminator columns

- **`Sgmt`** — `CM` | `FO`
- **`Src`** — `NSE` | `BSE`
- **`FinInstrmTp`** — the asset-class discriminator:

| Value | Meaning | Maps to `asset_class` |
|---|---|---|
| `STK` | Cash-segment scrip | `EQUITY` |
| `STO` | Stock option | `OPTION` |
| `IDO` | Index option | `OPTION` |
| `STF` | Stock future | `FUTURE` |
| `IDF` | Index future | `FUTURE` |

**`SctySrs`** (CM only) carries the series: `EQ`, `BE`, `SM`, `ST`, `GS`, `GB`, `N0`, `BZ`, `SG`, `N2`, …
Only `EQ`/`BE`/`SM`/`ST` are ordinary equity. `GS`/`GB` are government securities and gold bonds; `N*` are debt instruments. **Do not filter these out** — they are legitimately tradable and belong in the instrument master with the appropriate `asset_class`.

### Formats

- Dates (`TradDt`, `BizDt`, `XpryDt`, `FininstrmActlXpryDt`): **`YYYY-MM-DD`**
- Numerics: plain decimal, 2 dp, no thousands separators
- Empty values: **empty string**, not `NULL`, not `-`
- `ISIN` is populated in CM, **empty in FO**

### Sample rows

Cash (gold bond, series `GB`):
```
2026-08-13,2026-08-13,CM,NSE,STK,19078,IN0020200104,SGBJUN28,GB,,,,,2.5%GOLDBONDS2028SR-III,
15120.00,15120.00,15120.00,15120.00,15120.00,15140.00,,15120.00,,,3,45360.00,2,F1,1,,,,,
```

Stock option — **note `OpnPric`/`HghPric`/`LwPric` are all `0.00` while `ClsPric` is `19.45`**:
```
2026-08-13,2026-08-13,FO,NSE,STO,53047,,ABCAPITAL,,2026-10-27,2026-10-27,430.00,CE,ABCAPITAL26OCT430CE,
0.00,0.00,0.00,19.45,0.00,19.45,407.70,22.95,0,0,0,0.00,0,F1,3100,,,,,
```

### ⚠️ Untraded contracts break naive OHLC validation

**20,954 of 34,799 F&O rows (60%) on 2026-08-13 had `OpnPric = 0.00` with a non-zero `ClsPric`.** These are contracts that did not trade that day; the exchange still publishes a close and a settlement price derived theoretically.

Consequences:

1. A `CHECK (high >= low AND high >= open AND high >= close …)` constraint **rejects most of the F&O universe**. The invariant must be conditioned on `TtlTradgVol > 0`.
2. A validator that quarantines `open <= 0` would discard most of every F&O day.
3. Backtests must treat these bars as **non-tradable marks**, not as fillable prices. `TtlTradgVol = 0` is the flag.

### Two fields that are worth more than they look

- **`NewBrdLotQty`** is the lot size, present on every row of every day. This means `instrument_lot_history` (spec §4.2) is **derivable directly from the EOD backfill** — no separate lot-size source is needed. Emit a new history row whenever the value changes for an instrument.
- **`UndrlygPric`** is the underlying spot, present on option rows (`407.70` above). This gives a free spot series aligned to every option row, which satisfies verification check 3 (cross-source agreement) and supplies the spot series the relative-strike reconstruction of parent-plan §3.1 requires.

---

## 2. NSE legacy CM (pre-UDiFF)

### Access

```
https://nsearchives.nseindia.com/content/historical/EQUITIES/{YYYY}/{MON}/cm{DD}{MON}{YYYY}bhav.csv.zip
```
`{MON}` is the **uppercase three-letter month** (`MAR`), `{DD}` is zero-padded. Same cookie priming as above. Verified working for 2019-03-14.

### Schema — 13 columns + a trailing empty field

```
SYMBOL, SERIES, OPEN, HIGH, LOW, CLOSE, LAST, PREVCLOSE,
TOTTRDQTY, TOTTRDVAL, TIMESTAMP, TOTALTRADES, ISIN,
```

Every line ends with a trailing comma, producing a 14th empty field. Polars will name it `column_14` — expect and drop it rather than treating it as corruption.

```
20MICRONS,EQ,39.5,40,38.5,38.95,39,39.05,23869,933556.8,14-MAR-2019,235,INE144J01027,
```

### Differences from UDiFF that matter

| | Legacy | UDiFF |
|---|---|---|
| Date format | `14-MAR-2019` (`DD-MON-YYYY`) | `2026-08-13` (`YYYY-MM-DD`) |
| Numeric formatting | variable dp (`40`, `38.5`) | fixed 2 dp |
| Open interest | absent | present |
| Lot size | absent | `NewBrdLotQty` |
| Underlying price | absent | `UndrlygPric` |
| Coverage | cash only | cash + derivatives |

Legacy rows therefore produce `NULL` for `open_interest`, `settle_price`, and `delivery_*`, and contribute **nothing** to `instrument_lot_history`.

---

## 3. AMFI daily NAV

### Access

```
https://portal.amfiindia.com/spages/NAVAll.txt
```

⚠️ **The URL has moved.** The widely-documented `https://www.amfiindia.com/spages/NAVAll.txt` now returns **302** to the `portal.` host. Follow redirects, or hardcode the portal URL. No cookies, no user-agent games — a plain request works.

### It is not a CSV

Semicolon-delimited data rows are interleaved with blank lines, scheme-type section headers, and AMC name lines. `read_csv` on this file produces garbage. **A stateful line scanner is required.**

```
Scheme Code;ISIN Div Payout/ ISIN Growth;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Date
                                                    ← blank
Open Ended Schemes(Debt Scheme - Banking and PSU Fund)   ← scheme-type header (no ';')
                                                    ← blank
Aditya Birla Sun Life Mutual Fund                   ← AMC name (no ';')
                                                    ← blank
119551;INF209KA12Z1;INF209KA13Z9;Aditya Birla…;107.2564;13-Aug-2026    ← data (5 ';')
```

### Parsing rules

1. Skip the first line (the header).
2. Skip blank / whitespace-only lines.
3. A line **containing no `;`** is a section marker:
   - if it matches `^(Open|Close) Ended Schemes\(` → set current **scheme type**
   - otherwise → set current **AMC name**
4. A line **with exactly 5 semicolons** is a data row: `scheme_code; isin_growth; isin_reinvest; scheme_name; nav; date`
5. Attach the current scheme type and AMC to every data row.

### Field notes

- `Scheme Code` — AMFI's integer code; the stable natural key for a scheme
- Either ISIN may be `-`, meaning absent. **Normalise `-` to `NULL`.**
- `Date` format is **`13-Aug-2026`** (`DD-Mon-YYYY`, mixed case)
- `Net Asset Value` may be `N.A.` for suspended schemes → `NULL`, and the row goes to quarantine with reason `nav_not_available`
- The file carries the **latest** NAV per scheme, so the business date is per-row, not per-file. Rows may legitimately carry different dates.

Sample size on 2026-08-13: 17,779 lines.

---

## Regenerating samples

`scripts/fetch_recon_samples.sh` re-downloads all five sample files into `data/raw/_recon/`. Run it if a parser test starts failing for reasons this document does not explain — the exchange may have changed the format again.
