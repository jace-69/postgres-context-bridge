/**
 * Actor: PostgreSQL Context Bridge for AI
 * Aiming for Apify Quality Score: 100/100
 */
import { Actor } from 'apify';
import pg from 'pg';
import { promises as dns } from 'dns'; 
import { URL } from 'url';

interface Input {
    connectionString: string;
    query: string;
    maxRows?: number;
    ignoreSslErrors?: boolean;
}

(async () => {
    await Actor.init();
    console.log("🚀 Starting Actor v3.0 - Manual DNS Mode"); // <--- LOOK FOR THIS LINE

    try {
        const input = await Actor.getInput<Input>();
        if (!input?.connectionString) throw new Error('Input "connectionString" is required!');
        if (!input?.query) throw new Error('Input "query" is required!');

        const MAX_ROWS = input.maxRows || 100;

        // 1. MANUALLY RESOLVE HOSTNAME TO IPV4
        const dbUrl = new URL(input.connectionString);
        const originalHostname = dbUrl.hostname;
        let targetHost = originalHostname;
        
        console.log(`🔍 Resolving IPv4 for: ${originalHostname}...`);
        
        try {
            // This bypasses the broken system DNS and forces an IPv4 lookup
            const addresses = await dns.resolve4(originalHostname);
            if (addresses && addresses.length > 0) {
                targetHost = addresses[0];
                console.log(`👉 SUCCESS: Found IP ${targetHost}. Connecting directly...`);
            }
        } catch (err) {
            console.log(`⚠️ Resolution failed, using original host: ${(err as Error).message}`);
        }

        // 2. CONFIGURE CLIENT
        const client = new pg.Client({
            connectionString: input.connectionString,
            host: targetHost, // Connect to IP directly
            ssl: input.ignoreSslErrors 
                ? { rejectUnauthorized: false } 
                : { rejectUnauthorized: true, servername: originalHostname }, // Match SSL certificate
            statement_timeout: 10000, 
        });

        console.log('🔌 Connecting to Database...');
        await client.connect();

        console.log('🧠 Analyzing Query...');
        await client.query('BEGIN READ ONLY');

        const result = await client.query(input.query);
        const limitedRows = result.rows.slice(0, MAX_ROWS);
        
        console.log(`✅ VICTORY: Successfully fetched ${limitedRows.length} rows.`);

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
        console.error('❌ Database Error:', error);
        await Actor.fail(`Database Error: ${(error as Error).message}`);
    } finally {
        await Actor.exit();
    }
})();
