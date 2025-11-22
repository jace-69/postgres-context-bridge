/**
 * Actor: PostgreSQL Context Bridge for AI
 * Aiming for Apify Quality Score: 100/100
 */
import { Actor } from 'apify';
import pg from 'pg';

interface Input {
    connectionString: string;
    query: string;
    maxRows?: number;
    ignoreSslErrors?: boolean;
}

await Actor.init();

// 1. VALIDATE INPUT
// Good validation helps you get a high "Actor Quality Score"
const input = await Actor.getInput<Input>();
if (!input?.connectionString) throw new Error('Input "connectionString" is required!');
if (!input?.query) throw new Error('Input "query" is required!');

const MAX_ROWS = input.maxRows || 100;

// 2. CONFIGURE CLIENT
// Handle standard Postgres SSL connections (required for Supabase/Neon/AWS)
const client = new pg.Client({
    connectionString: input.connectionString,
    ssl: input.ignoreSslErrors ? { rejectUnauthorized: false } : true,
    statement_timeout: 10000, // Kill query after 10s to prevent hanging
});

try {
    console.log('🔌 Connecting to Database...');
    await client.connect();

    console.log('🧠 Analyzing Query...');
    
    // 3. SAFETY FIRST (CRITICAL)
    // Force the transaction to be READ ONLY. This protects the user's DB.
    // This safety feature is a huge selling point for your actor.
    await client.query('BEGIN READ ONLY');

    // 4. EXECUTE QUERY
    const result = await client.query(input.query);

    // Limit rows to prevent context overflow in AI models
    const limitedRows = result.rows.slice(0, MAX_ROWS);
    
    console.log(`✅ Successfully fetched ${limitedRows.length} rows.`);

    // 5. FORMAT FOR AI (The Value Add)
    // We output strict JSON which AI models (MCP/LangChain) digest easily.
    const output = {
        meta: {
            rowCount: result.rowCount,
            columns: result.fields.map(f => ({ name: f.name, type: f.dataTypeID })),
            truncated: result.rows.length > MAX_ROWS
        },
        data: limitedRows
    };

    // Push the actual data to the Apify Dataset
    await Actor.pushData(output);

} catch (error) {
    console.error('❌ Database Error:', error);
    // Fail gracefully to ensure the user knows what went wrong
    await Actor.fail(`Database Error: ${(error as Error).message}`);
} finally {
    await client.end(); // Close connection cleanly
}

await Actor.exit();