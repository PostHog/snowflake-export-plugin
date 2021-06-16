import * as snowflake from 'snowflake-sdk'
import { createPool, Pool } from 'generic-pool'
import { PluginEvent, Plugin, RetryError } from '@posthog/plugin-scaffold'
import crypto from 'crypto'

interface SnowflakePluginInput {
    global: {
        snowflake: Snowflake
        eventsToIgnore: Set<string>
    }
    config: {
        account: string
        username: string
        password: string
        database: string
        dbschema: string
        table: string
        stage: string
        eventsToIgnore: string
    }
}

interface TableRow {
    uuid: string
    event: string
    properties: string // Record<string, any>
    elements: string // Record<string, any>
    set: string // Record<string, any>
    set_once: string // Record<string, any>
    distinct_id: string
    team_id: number
    ip: string
    site_url: string
    timestamp: string
}

const TABLE_SCHEMA = [
    { name: 'uuid', type: 'STRING' },
    { name: 'event', type: 'STRING' },
    { name: 'properties', type: 'VARIANT' },
    { name: 'elements', type: 'VARIANT' },
    { name: 'set', type: 'VARIANT' },
    { name: 'set_once', type: 'VARIANT' },
    { name: 'distinct_id', type: 'STRING' },
    { name: 'team_id', type: 'INTEGER' },
    { name: 'ip', type: 'STRING' },
    { name: 'site_url', type: 'STRING' },
    { name: 'timestamp', type: 'TIMESTAMP' },
]

function transformEventToRow(fullEvent: PluginEvent): TableRow {
    const {
        event,
        properties,
        $set,
        $set_once,
        distinct_id,
        team_id,
        site_url,
        now,
        sent_at,
        uuid,
        ...rest
    } = fullEvent
    const ip = properties?.['$ip'] || event.ip
    const timestamp = fullEvent.timestamp || properties?.timestamp || now || sent_at
    let ingestedProperties = properties
    let elements = []

    // only move prop to elements for the $autocapture action
    if (event === '$autocapture' && properties?.['$elements']) {
        const { $elements, ...props } = properties
        ingestedProperties = props
        elements = $elements
    }
    
    return {
        uuid: uuid!,
        event,
        properties: JSON.stringify(ingestedProperties || {}),
        elements: JSON.stringify(elements || []),
        set: JSON.stringify($set || {}),
        set_once: JSON.stringify($set_once || {}),
        distinct_id,
        team_id,
        ip,
        site_url,
        timestamp,
    }
}

class Snowflake {
    private pool: Pool<snowflake.Connection>

    constructor(account: string, username: string, password: string) {
        this.pool = this.createConnectionPool(account, username, password)
    }

    public async clear(): Promise<void> {
        await this.pool.drain()
        await this.pool.clear()
    }

    public async createTableIfNotExists(
        database: string,
        dbschema: string,
        table: string,
        columns: string
    ): Promise<void> {
        await this.execute({
            sqlText: `CREATE TABLE IF NOT EXISTS "${database}"."${dbschema}"."${table}" (${columns})`,
        })
    }
    
    public async dropTableIfExists(
        database: string,
        dbschema: string,
        table: string
    ): Promise<void> {
        await this.execute({
            sqlText: `DROP TABLE IF EXISTS "${database}"."${dbschema}"."${table}"`,
        })
    }
    
    public async createPipeIfNotExists(
        database: string,
        dbschema: string,
        table: string,
        pipe: string,
        stage: string
    ): Promise<void> {
        await this.execute({
            sqlText: `CREATE PIPE "${database}"."${dbschema}"."${pipe}" IF NOT EXISTS
            AS COPY INTO "${database}"."${dbschema}"."${table}" FROM "${database}"."${dbschema}"."${stage}"`
        })
    }
    
    public async createStageIfNotExists(
        database: string,
        dbschema: string, stage: string): Promise<void> {
            await this.execute({
                sqlText:
        `CREATE STAGE IF NOT EXISTS "${database}"."${dbschema}"."${stage}"
        FILE_FORMAT = ( TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE )
        COMMENT = 'Stage used by the PostHog Snowflake export plugin'` })
    }
    
    public async execute({ sqlText, binds }: {
        sqlText: string
        binds?: snowflake.Binds
    }): Promise<any[] | undefined> {
        const snowflake = await this.pool.acquire()
        try {
            return await new Promise((resolve, reject) =>
                snowflake.execute({
                    sqlText,
                    binds,
                    complete: function (err, _stmt, rows) {
                        if (err) {
                            console.error('Error executing Snowflake query:', { sqlText, error: err.message })
                            reject(err)
                        } else {
                            resolve(rows)
                        }
                    },
                })
            )
        } finally {
            await this.pool.release(snowflake)
        }
    }

    private createConnectionPool(account: string, username: string, password: string): Snowflake['pool'] {
        return createPool(
            {
                create: async () => {
                    const connection = snowflake.createConnection({
                        account,
                        username,
                        password,
                    })
    
                    await new Promise<string>((resolve, reject) =>
                        connection.connect((err, conn) => {
                            if (err) {
                                console.error('Error connecting to Snowflake:' + err.message)
                                reject(err)
                            } else {
                                resolve(conn.getId())
                            }
                        })
                    )
    
                    return connection
                },
                destroy: async (connection) => {
                    await new Promise<void>((resolve, reject) =>
                        connection.destroy(function (err) {
                            if (err) {
                                console.error('Error disconnecting from Snowflake:' + err.message)
                                reject(err)
                            } else {
                                resolve()
                            }
                        })
                    )
                },
            },
            {
                min: 1,
                max: 1,
                autostart: true,
                fifo: true,
            }
        )
    }
}

const exportTableColumns = TABLE_SCHEMA.map(({ name, type }) => `"${name.toUpperCase()}" ${type}`).join(', ')

const jsonFields = new Set(TABLE_SCHEMA.filter(({ type }) => type === 'VARIANT').map(({ name }) => name))

const snowflakePlugin: Plugin<SnowflakePluginInput> = {
    async setupPlugin(meta) {
        const { global, config } = meta
        // Prepare for working with Snowflake
        global.snowflake = new Snowflake(config.account, config.username, config.password)
    
        // Create table
        await global.snowflake.createTableIfNotExists(
            config.database,
            config.dbschema,
            config.table,
            exportTableColumns
        )

        // Create stage
        await global.snowflake.createStageIfNotExists(
            config.database,
            config.dbschema,
            config.stage,
        )
    
        global.eventsToIgnore = new Set<string>(
            (config.eventsToIgnore || '').split(',').map((event) => event.trim())
        )
    },

    async teardownPlugin({ global }) {
        await global.snowflake.clear()
    },
    
    async exportEvents (events, { global, config }) {
        const rows = events.filter((event) => !global.eventsToIgnore.has(event.event.trim())).map(transformEventToRow)
        if (rows.length) {
            console.info(`Saving batch of ${rows.length} event${rows.length !== 1 ? 's' : ''} to Snowflake stage "${config.stage}"`)
        } else {
            console.info(`Skipping an empty batch of events`)
        }
        try {
            await fetch(`https://${config.host}/e`, {
                method: 'POST',
                body: JSON.stringify(events),
                headers: { 'Content-Type': 'application/json' },
            })
        } catch (error) {
            throw new RetryError() 
        }
    },
}

export default snowflakePlugin

function getSnowpipeJwt() {
    const publicKeyBytes = crypto.createPublicKey(pem).export({ type: 'spki', format: 'der'});
    const signature = 'SHA256:' + crypto.createHash('sha256').update(publicKeyBytes).digest().toString('base64');const bearer = jose.encode({
        iss: `${account}.${username}.${signature}`,
        sub: `${account}.${username}`,
        iat: Math.round(new Date().getTime() / 1000),
        exp: Math.round(new Date().getTime() / 1000 + 60 * 59)
      }, pem, 'RS256');
}
