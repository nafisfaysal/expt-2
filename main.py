const { OpenAI } = require('openai');

// Initialize OpenAI client with API key handling
const initializeOpenAI = () => {
    const apiKey = process.env.OPENAI_API_KEY;

    if (!apiKey) {
        console.log('❌ No API key found in environment variables.');
        console.log('Please enter your OpenAI API key:');

        const readline = require('readline').createInterface({
            input: process.stdin,
            output: process.stdout
        });

        return new Promise((resolve) => {
            readline.question('OpenAI API Key: ', (key) => {
                readline.close();
                process.env.OPENAI_API_KEY = key;
                return resolve(new OpenAI({ apiKey: key }));
            });
        });
    }

    return Promise.resolve(new OpenAI({ apiKey }));
};

async function getCountryFromAddress(address, openai) {
    try {
        console.log(`\n📝 Processing address: ${address}`);

        // Add basic input validation
        if (!address || address.trim().length < 3) {
            throw new Error('Address is too short or empty to determine location');
        }

        const completion = await openai.chat.completions.create({
            model: "gpt-4o",  // Keeping the correct model name
            messages: [
                {
                    role: "system",
                    content: `You are a highly specialized AI engine designed for one purpose: to accurately identify the country from a given address string. You must be precise and avoid making assumptions.

Your task is to analyze the provided address, which may be incomplete or in any language, and return the country information in a specific JSON format.

### JSON Output Structure
You MUST respond with a valid JSON object in the following format. Do not include any other text or explanations.

{
  "shortForm": "The 2-letter ISO 3166-1 alpha-2 country code in UPPERCASE. Use 'UNKNOWN' if the country cannot be determined with high confidence.",
  "longForm": "The full, official English name of the country. Use 'UNKNOWN' if the country cannot be determined.",
  "confidence": "A floating-point number between 0.0 and 1.0 representing your confidence in the result."
}

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
    *   If the calculated confidence is less than 0.5, you MUST return 'UNKNOWN' for \`shortForm\` and \`longForm\`.
    *   If the input is gibberish, a single generic word, or lacks any geographical clues (e.g., "my house", "123456").
    *   If the address is ambiguous and could plausibly belong to multiple countries with similar confidence scores.

### Examples

**User Input:** "1600 Pennsylvania Avenue NW, Washington, DC 20500"
**Your Response:**
{
  "shortForm": "US",
  "longForm": "United States",
  "confidence": 1.0
}

**User Input:** "Tour Eiffel, Champ de Mars, 5 Av. Anatole France, 75007 Paris"
**Your Response:**
{
  "shortForm": "FR",
  "longForm": "France",
  "confidence": 0.9
}

**User Input:** "some random street"
**Your Response:**
{
  "shortForm": "UNKNOWN",
  "longForm": "UNKNOWN",
  "confidence": 0.0
}

**User Input:** "Bahnhofstraße 1, 8001 Zürich"
**Your Response:**
{
  "shortForm": "CH",
  "longForm": "Switzerland",
  "confidence": 0.9
}

**User Input:** "48 Pirrama Rd, Pyrmont NSW 2009"
**Your Response:**
{
  "shortForm": "AU",
  "longForm": "Australia",
  "confidence": 0.95
}`
                },
                {
                    role: "user",
                    content: `Return JSON: Extract the country from this address (which may be incomplete): ${address}`
                }
            ],
            temperature: 0.1,
            response_format: { type: "json_object" }
        });

        // Validate API response structure
        if (!completion?.choices?.[0]?.message) {
            throw new Error('Invalid API response structure');
        }

        const result = JSON.parse(completion.choices[0].message.content);

        // Validate result and confidence
        if (!result.shortForm || !result.longForm || typeof result.confidence !== 'number') {
            throw new Error('Invalid response format');
        }

        // Handle low confidence or UNKNOWN results
        if (result.confidence < 0.7 || result.shortForm === "UNKNOWN") {
            throw new Error('Unable to determine country with sufficient confidence');
        }

        return {
            shortForm: result.shortForm,
            longForm: result.longForm,
            confidence: result.confidence
        };

    } catch (error) {
        // Check for specific API errors
        if (error.response?.status === 429) {
            throw new Error('Rate limit exceeded. Please try again in a few moments.');
        }
        if (error.response?.status === 500) {
            throw new Error('AI service temporarily unavailable. Please try again later.');
        }

        // If it's our custom error, pass it through
        if (error.message.includes('Unable to determine country') ||
            error.message.includes('too short') ||
            error.message.includes('Invalid')) {
            throw error;
        }

        // For unexpected errors, log them but return a user-friendly message
        console.error('❌ Error:', error);
        throw new Error('Unable to process address. Please try again with more specific details.');
    }
}

// Modified main function to handle multiple addresses
async function main() {
    try {
        // Initialize OpenAI first
        const openai = await initializeOpenAI();

        // Using readline for address input
        const readline = require('readline').createInterface({
            input: process.stdin,
            output: process.stdout
        });

        // Promisify the question method
        const askQuestion = (query) => new Promise((resolve) => readline.question(query, resolve));

        console.log('\n👋 Welcome to Address Country Detector!');
        console.log('Type "exit" or "quit" to end the program\n');

        while (true) {
            const address = await askQuestion('\nPlease enter an address: ');

            if (address.toLowerCase() === 'exit' || address.toLowerCase() === 'quit') {
                console.log('\n👋 Goodbye!');
                break;
            }

            try {
                console.log('\nProcessing address...');
                const countryInfo = await getCountryFromAddress(address, openai);
                console.log('✅ Country Information:', countryInfo);
            } catch (error) {
                console.error('❌ Error processing this address:', error.message);
                console.log('Please try another address.');
            }
        }

        readline.close();
    } catch (error) {
        console.error('Main error:', error);
    }
}

// Call main if this file is run directly
if (require.main === module) {
    main();
}

module.exports = { getCountryFromAddress };
