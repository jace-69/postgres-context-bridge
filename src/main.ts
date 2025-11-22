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

// WRAP EVERYTHING IN A FUNCTION TO FIX 'TOP LEVEL AWAIT' ERROR
(async () => {
    await Actor.init();

    try {
        // 1. VALIDATE INPUT
        const input = await Actor.getInput<Input>();
        if (!input?.connectionString) throw new Error('Input "connectionString" is required!');
        if (!input?.query) throw new Error('Input "query" is required!');

        const MAX_ROWS = input.maxRows || 100;

        // 2. CONFIGURE CLIENT
        const client = new pg.Client({
            connectionString: input.connectionString,
            ssl: input.ignoreSslErrors ? { rejectUnauthorized: false } : true,
            statement_timeout: 10000, 
        });

        console.log('🔌 Connecting to Database...');
        await client.connect();

        console.log('🧠 Analyzing Query...');
        
        // 3. SAFETY FIRST
        await client.query('BEGIN READ ONLY');

        // 4. EXECUTE QUERY
        const result = await client.query(input.query);

        const limitedRows = result.rows.slice(0, MAX_ROWS);
        
        console.log(`✅ Successfully fetched ${limitedRows.length} rows.`);

        // 5. FORMAT FOR AI
        const output = {
            meta: {
                rowCount: result.rowCount,
                // Add type 'any' to (f) to satisfy TypeScript strict mode
                columns: result.fields.map((f: any) => ({ name: f.name, type: f.dataTypeID })),
                truncated: result.rows.length > MAX_ROWS
            },
            data: limitedRows
        };

        await Actor.pushData(output);

        await client.end(); // Close connection

    } catch (error) {
        console.error('❌ Database Error:', error);
        await Actor.fail(`Database Error: ${(error as Error).message}`);
    } finally {
        await Actor.exit();
    }
})();