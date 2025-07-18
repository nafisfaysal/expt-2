import os
import asyncio
import aiofiles
import json
import argparse
from openai import AsyncOpenAI, RateLimitError
from tqdm.asyncio import tqdm_asyncio
from aiocsv import AsyncReader, AsyncDictWriter
import random
import time

# --- Configuration ---
# WARNING: Storing API keys directly in code is a security risk.
# This is for temporary, non-production use only.
# For production, use environment variables.
API_KEY = "s"

client = AsyncOpenAI(api_key=API_KEY)

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

                completion = await client.chat.completions.create(
                    model="gpt-4o",
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": json.dumps(valid_addresses)}
                    ],
                    temperature=0.1,
                    response_format={"type": "json_object"}
                )

                response_data = json.loads(completion.choices[0].message.content)
                results = response_data.get("results", [])

                for i, address in enumerate(valid_addresses):
                    result_line = {"address": address}
                    if i < len(results) and results[i].get("confidence", 0.0) >= 0.7 and results[i].get("shortForm") != "UNKNOWN":
                        result_line.update(results[i])
                    else:
                        result_line["error"] = "Confidence too low or unknown"
                    await writer.writerow(result_line)
                return # Success, exit loop

        except RateLimitError as e:
            num_retries += 1
            if num_retries > max_retries:
                raise Exception(f"Maximum retries exceeded for batch starting with: {batch[0]}")
            
            delay *= exponential_base * (1 + random.random())
            print(f"Rate limit exceeded. Retrying in {delay:.2f} seconds...")
            await asyncio.sleep(delay)

        except Exception as e:
            for address in batch:
                await writer.writerow({"address": address, "error": f"Batch failed: {str(e)}"})
            return # Exit loop after handling non-retryable error


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
    
    asyncio.run(main(args.input_csv, args.output_csv, args.concurrency, args.batch_size)) 
