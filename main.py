cat engine.js
const { OpenAI } = require('openai');

// Initialize OpenAI client with API key handling
const initializeOpenAI = () => {
  const apiKey = '';

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
          content: `You are a precise address analysis expert specializing in global address formats and country identification.
You must respond in JSON format.

CRITICAL REQUIREMENTS:
1. Extract country information ONLY when there is sufficient evidence
2. Use contextual clues such as:
   - Postal/ZIP code patterns (e.g., "90210" suggests US)
   - State/Province abbreviations (e.g., "NY" suggests US)
   - Phone number formats (e.g., "+44" suggests UK)
   - Language patterns (e.g., "Calle" suggests Spanish-speaking country)
   - Local landmarks or institutions
3. Handle addresses in multiple languages

RESPONSE FORMAT (JSON):
{
    "shortForm": "2-letter ISO 3166-1 alpha-2 country code in UPPERCASE or UNKNOWN",
    "longForm": "Official full country name in English or UNKNOWN",
    "confidence": number between 0 and 1
}

CRITICAL RULES:
- Return "UNKNOWN" for both fields if:
  * Input is a single word without clear country association
  * Input is just numbers or generic terms
  * Input could refer to multiple countries
  * Confidence is below 0.7
  * Input lacks specific geographic indicators
- Only return a country when there are clear geographic or cultural indicators
- Set confidence based on the specificity and clarity of location indicators
- Always respond in valid JSON format`
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
azureuser@rizzshot:~/web-app-addressml$
