## TGS Sheet Automation (Country Extraction)

Python script to filter TGS scenario workbooks so they contain only the tabs relevant to a target country.

### Install

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### Windows (PowerShell)

```powershell
cd "C:\\path\\to\\tg-data-citi-automation"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

If you see an execution policy error, run this once in the current PowerShell session and try again:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

### Run

```bash
python main.py \
  --input-dir /path/to/scenarios \
  --output-dir /path/to/output \
  --country EC \
  --region LATAM \
  --lob ICG
```

#### Poland example

Use the 2-letter country code `PL`. Set `--region` to match your workbook’s membership key scheme (commonly `EMEA`), so the script looks for `EMEA_PL_<LOB>` in `Incl_Country_LOB_Lst`.

macOS/Linux:

```bash
python main.py \
  --input-dir /path/to/scenarios \
  --output-dir /path/to/output \
  --country PL \
  --region EMEA \
  --lob ICG
```

Windows (PowerShell):

```powershell
python .\main.py \
  --input-dir "C:\\path\\to\\scenarios" \
  --output-dir "C:\\path\\to\\output" \
  --country PL \
  --region EMEA \
  --lob ICG
```

Use `--dry-run` to preview which sheets would be kept without writing files.

The script keeps:
- the `Change Log` tab (if present), and
- the country tab `TGS_<CC>_<LOB>` (e.g., `TGS_EC_ICG`) if it exists, and/or
- the general tab `TGS_EN_GNRL_<LOB>` if the row with `TSHLD_NM == Incl_Country_LOB_Lst` includes the key `<REGION>_<CC>_<LOB>` (e.g., `LATAM_EC_ICG`).

Outputs are saved as `<original_name>__<CC>.<ext>` under the output directory.


