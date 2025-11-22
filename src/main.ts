/**
 * Actor: PostgreSQL Context Bridge for AI
 * Aiming for Apify Quality Score: 100/100
 */
import { Actor } from 'apify';
import pg from 'pg';
import dns from 'dns'; // <--- NEW IMPORT

// <--- FIX FOR ENETUNREACH ERROR: FORCE IPv4
// This tells Node to prefer standard IP addresses over the complex IPv6 ones
if (dns.setDefaultResultOrder) {
    dns.setDefaultResultOrder('ipv4first');
}

interface Input {
    connectionString: string;
    query: string;
    maxRows?: number;
    ignoreSslErrors?: boolean;
}

(async () => {
    await Actor.init();

    try {
        const input = await Actor.getInput<Input>();
        if (!input?.connectionString) throw new Error('Input "connectionString" is required!');
        if (!input?.query) throw new Error('Input "query" is required!');

        const MAX_ROWS = input.maxRows || 100;

        const client = new pg.Client({
            connectionString: input.connectionString,
            ssl: input.ignoreSslErrors ? { rejectUnauthorized: false } : true,
            statement_timeout: 10000, 
        });

        console.log('🔌 Connecting to Database (forcing IPv4)...');
        await client.connect();

        console.log('🧠 Analyzing Query...');
        await client.query('BEGIN READ ONLY');

        const result = await client.query(input.query);

        const limitedRows = result.rows.slice(0, MAX_ROWS);
        
        console.log(`✅ Successfully fetched ${limitedRows.length} rows.`);

        const output = {
            meta: {
                rowCount: result.rowCount,
                columns: result.fields.map((f: any) => ({ name: f.name, type: f.dataTypeID })),
                truncated: result.rows.length > MAX_ROWS
            },
            data: limitedRows
        };

        await Actor.pushData(output);
        await client.end(); 

    } catch (error) {
        console.error('❌ Database Error:', error)  ;
        await Actor.fail(`Database Error: ${(error as Error).message}`);
    } finally {
        await Actor.exit();
    }
})();