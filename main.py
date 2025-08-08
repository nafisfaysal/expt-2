import os
import asyncio
import aiofiles
import json
import argparse
import subprocess
from typing import Dict, List, Optional

import vertexai
from vertexai.generative_models import GenerativeModel
from google.oauth2.credentials import Credentials
from tqdm.asyncio import tqdm_asyncio
from aiocsv import AsyncReader, AsyncDictWriter
import random
import time


VERTEX_PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "pr-gen-ai-9571")
VERTEX_API_ENDPOINT = os.getenv("R2D2_VERTEX_ENDPOINT")
VERTEX_MODEL_NAME = os.getenv("VERTEX_MODEL_NAME", "gemini-1.5-pro-002")
DEFAULT_CA_BUNDLE = r"C:\\citi_ca_certs\\citiInternalCAchain_PROD.pem"

# Lazily initialized global model instance
_vertex_model: Optional[GenerativeModel] = None

SYSTEM_PROMPT = """You are a highly specialized AI engine designed for one purpose: to accurately identify the country from a given address string. You must be precise and avoid making assumptions.

Your task is to analyze the provided batch of addresses, which may be incomplete or in any language, and return the country information for each one.

You will receive a JSON array of address strings in the user message. You MUST respond with a valid JSON object containing a single key "results". This key should hold a JSON array of objects, where each object corresponds to an address in the input array and maintains the original order.

### BATCH OUTPUT JSON STRUCTURE
Your entire response must be a single JSON object like this. Do not include any other text or explanations.
{
  "results": [
{
      "shortForm": "The 2-letter ISO 3166-1 alpha-2 country code...",
      "longForm": "The full, official English name of the country...",
      "confidence": "A floating-point number from 0.0 to 1.0..."
    },
    {
      "shortForm": "...",
      "longForm": "...",
      "confidence": "..."
    }
  ]
}

### RULES FOR EACH INDIVIDUAL ADDRESS

For each address in the input batch, apply the following logic:

### Analysis and Confidence Rules
1.  **Basis of Identification**: Your decision must be based on concrete evidence within the address string. Look for:
    *   **Postal/ZIP Codes**: Patterns specific to a country (e.g., 5-digit US ZIP, UK postcode format).
    *   **State/Province/Region**: Abbreviations or full names (e.g., "CA" for California, USA; "Bavaria" for Germany).
    *   **City Names**: Unambiguous major city names (e.g., "Paris, France"). Be cautious with common city names (e.g., Paris, Texas).
    *   **Street/Address Terminology**: Language-specific terms (e.g., "Calle" in Spanish, "Rue" in French, "Straße" in German).
    *   **Country Names**: The presence of the country name itself.

2.  **Calculating Confidence**:
    *   **1.0**: The country name is explicitly mentioned, or there are multiple, unambiguous indicators (e.g., "10 Downing Street, London, SW1A 2AA, UK").
    *   **0.8-0.9**: A unique identifier is present, like a specific postal code format or a major city and state combination (e.g., "90210 Beverly Hills, CA").
    *   **0.5-0.7**: A strong indicator is present, but it could have rare exceptions (e.g., a city name that is very common in one country but exists elsewhere).
    *   **0.1-0.4**: The only clue is a weak indicator, like a common street name word ("Main Street") or a name that exists in multiple countries.
    *   **0.0**: No geographic information at all.

3.  **When to use 'UNKNOWN'**:
    *   If the calculated confidence is less than 0.5, you MUST return 'UNKNOWN' for `shortForm` and `longForm`.
    *   If the input is gibberish, a single generic word, or lacks any geographical clues (e.g., "my house", "123456").
    *   If the address is ambiguous and could plausibly belong to multiple countries with similar confidence scores.

### Examples of Individual Address Analysis

**Input Address:** "1600 Pennsylvania Avenue NW, Washington, DC 20500"
**Resulting JSON Object:**
{
  "shortForm": "US",
  "longForm": "United States",
  "confidence": 1.0
}

**Input Address:** "Tour Eiffel, Champ de Mars, 5 Av. Anatole France, 75007 Paris"
**Resulting JSON Object:**
{
  "shortForm": "FR",
  "longForm": "France",
  "confidence": 0.9
}

**Input Address:** "some random street"
**Resulting JSON Object:**
{
  "shortForm": "UNKNOWN",
  "longForm": "UNKNOWN",
  "confidence": 0.0
}
"""

# --- Vertex Authentication / Init ---
def _ensure_windows_ca_bundle() -> None:
    """On Windows, set REQUESTS_CA_BUNDLE to the standard corporate chain if missing.

    Mirrors the screenshot by defaulting to C:\citi_ca_certs\citiInternalCAchain_PROD.pem.
    """
    try:
        if os.name == "nt":
            if not os.getenv("REQUESTS_CA_BUNDLE") and os.path.exists(DEFAULT_CA_BUNDLE):
                os.environ["REQUESTS_CA_BUNDLE"] = DEFAULT_CA_BUNDLE
    except Exception:
        # Do not block startup if we cannot set it automatically
        pass


def _get_helix_token_via_powershell(helix_dir: Optional[str]) -> Optional[str]:
    """Attempt to get a token via PowerShell, optionally injecting HELIX_DIR.

    Returns the token string on success; None on failure.
    """
    if os.name != "nt":
        return None

    command = [
        "powershell",
        "-NoProfile",
        "-Command",
    ]

    if helix_dir:
        ps = f"$env:HELIX_DIR=\"{helix_dir}\"; helix auth access-token print -a"
    else:
        ps = "helix auth access-token print -a"

    command.append(ps)

    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        token = result.stdout.strip()
        return token or None
    except Exception:
        return None


def _get_helix_access_token() -> str:
    """Fetch an access token via the helix CLI.

    The command executed is equivalent to: `helix auth access-token print -a`.
    The helix binary must be available on PATH. If it lives elsewhere, ensure your shell PATH includes it
    before running this script.
    """
    # First, try direct invocation (works when helix is on PATH on any OS)
    try:
        result = subprocess.run(
            ["helix", "auth", "access-token", "print", "-a"],
            check=True,
            capture_output=True,
            text=True,
        )
        token = result.stdout.strip()
        if token:
            return token
    except Exception:
        pass

    # If on Windows, try the PowerShell + HELIX_DIR approach used in your screenshot
    if os.name == "nt":
        helix_dir = os.getenv("HELIX_DIR")
        # Try with HELIX_DIR from env
        token = _get_helix_token_via_powershell(helix_dir)
        if token:
            return token

        # Try a sensible default path if not provided (may vary by workstation)
        default_dir = os.path.join(
            os.path.expanduser("~"),
            "AppData",
            "Local",
            "CitiSoftware",
            "HELIXCLI_0.24",
        )
        if os.path.isdir(default_dir):
            token = _get_helix_token_via_powershell(default_dir)
            if token:
                return token

    raise RuntimeError(
        "Unable to obtain Helix access token. Ensure helix is on PATH or set HELIX_DIR and try again."
    )


def _init_vertex_if_needed() -> None:
    """Initialize Vertex AI client and the global `GenerativeModel` once."""
    global _vertex_model
    if _vertex_model is not None:
        return

    if not VERTEX_API_ENDPOINT:
        raise RuntimeError(
            "R2D2_VERTEX_ENDPOINT env var is required (your R2D2 Vertex endpoint)."
        )

    _ensure_windows_ca_bundle()

    # Build Google credentials object from helix token
    access_token = _get_helix_access_token()
    credentials = Credentials(token=access_token)

    # Optional: pass R2D2 username in metadata header
    r2d2_user = os.getenv("USERNAME") or os.getenv("USER") or "unknown"

    # Initialize vertex pointing to the R2D2 proxy endpoint
    vertexai.init(
        project=VERTEX_PROJECT_ID,
        credentials=credentials,
        api_transport="rest",
        api_endpoint=VERTEX_API_ENDPOINT,
        metadata={"x-r2d2-user": r2d2_user},
    )

    _vertex_model = GenerativeModel(VERTEX_MODEL_NAME)


def _generate_country_results_sync(addresses: List[str]) -> Dict:
    """Blocking call to Gemini via Vertex to classify a batch of addresses.

    Returns the parsed JSON object according to the SYSTEM_PROMPT contract.
    """
    _init_vertex_if_needed()

    prompt = (
        SYSTEM_PROMPT
        + "\n\nAddresses JSON array follows. Respond with the JSON object only.\n"
        + json.dumps(addresses)
    )

    response = _vertex_model.generate_content(prompt)
    # Vertex returns candidates; `.text` is the concatenated string result
    text = getattr(response, "text", None) or str(response)
    return json.loads(text)


# --- Core Functions ---
async def process_address_batch_with_backoff(batch, semaphore, writer):
    """
    Analyzes a batch of addresses with exponential backoff for retries.
    """
    initial_delay = 1
    exponential_base = 2
    max_retries = 10
    num_retries = 0
    delay = initial_delay

    while True:
        try:
            async with semaphore:
                valid_addresses = [addr for addr in batch if addr and addr.strip()]
                if not valid_addresses:
                    return

                response_data = await asyncio.to_thread(
                    _generate_country_results_sync, valid_addresses
                )
                results = response_data.get("results", [])

                for i, address in enumerate(valid_addresses):
                    result_line = {"address": address}
                    if i < len(results) and results[i].get("confidence", 0.0) >= 0.7 and results[i].get("shortForm") != "UNKNOWN":
                        result_line.update(results[i])
                    else:
                        result_line["error"] = "Confidence too low or unknown"
                    await writer.writerow(result_line)
                return # Success, exit loop

        except Exception as e:
            # Retry on transient errors
            num_retries += 1
            if num_retries > max_retries:
                raise Exception(f"Maximum retries exceeded for batch starting with: {batch[0]}")
            
            delay *= exponential_base * (1 + random.random())
            print(f"Rate limit exceeded. Retrying in {delay:.2f} seconds...")
            await asyncio.sleep(delay)
            
            # If we want fine-grained control, inspect `e` for HTTP status or google errors
            # and decide retryability. For now, we backoff on any exception up to max_retries.


def create_batches(items, size):
    """Yield successive n-sized chunks from list."""
    for i in range(0, len(items), size):
        yield items[i:i + size]

# --- Main Execution ---
async def main(input_csv: str, output_csv: str, concurrency: int, batch_size: int):
    """
    Main function to orchestrate the bulk processing of addresses from a CSV file.
    """
    if not os.path.isfile(input_csv):
        print(f"❌ Error: Input CSV file '{input_csv}' not found.")
        return

    addresses = []
    async with aiofiles.open(input_csv, 'r', newline='', encoding='utf-8') as infile:
        reader = AsyncReader(infile)
        await reader.__anext__() # Skip header
        async for row in reader:
            if row:
                addresses.append(row[0])
    
    if not addresses:
        print("🤷 No addresses found in the input file.")
        return

    print(f"🚀 Found {len(addresses)} addresses. Creating batches of size {batch_size}...")
    
    address_batches = list(create_batches(addresses, batch_size))
    
    semaphore = asyncio.Semaphore(concurrency)
    
    async with aiofiles.open(output_csv, 'w', newline='', encoding='utf-8') as outfile:
        fieldnames = ["address", "shortForm", "longForm", "confidence", "error"]
        writer = AsyncDictWriter(outfile, fieldnames)
        await writer.writeheader()

        tasks = [process_address_batch_with_backoff(batch, semaphore, writer) for batch in address_batches]

        await tqdm_asyncio.gather(*tasks, desc="Processing batches")

    print(f"\n✅ Processing complete. Results saved to '{output_csv}'.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bulk process addresses from a CSV file to find their country.")
    parser.add_argument("input_csv", help="Input CSV file with an 'address' column.")
    parser.add_argument("output_csv", help="Path to save the output CSV file.")
    parser.add_argument("--concurrency", type=int, default=50, help="Number of concurrent API requests.")
    parser.add_argument("--batch-size", type=int, default=50, help="Number of addresses to send in each API request.")
    
    args = parser.parse_args()

    # Ensure Vertex is initialized early to fail-fast if environment is misconfigured
    _init_vertex_if_needed()

    asyncio.run(main(args.input_csv, args.output_csv, args.concurrency, args.batch_size))
