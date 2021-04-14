import { createBuffer } from '@posthog/plugin-contrib'
import * as snowflake from 'snowflake-sdk'
import { createPool, Pool } from 'generic-pool'
import { randomBytes } from 'crypto'
import { PluginEvent, PluginMeta } from '@posthog/plugin-scaffold'
import { Connection } from 'snowflake-sdk'

interface SnowflakePluginMeta extends PluginMeta {
    global: {
        snowflakePool: Pool<Connection>
        snowflakeExecute: SnowflakeExecute
        uniqueTime: string
        temporaryTable: string
        buffer: any
        eventsToIgnore: Set<string>
        initDone: boolean
    }
    config: {
        account: string
        username: string
        password: string
        database: string
        dbschema: string
        table: string
        eventsToIgnore: string
        mergeFrequency: MergeFrequency
    }
}

type SnowflakeExecuteParams = {
    sqlText: string
    binds?: any[] | any[][]
    verbose?: boolean
}
type SnowflakeExecute = (opts: SnowflakeExecuteParams) => Promise<any[]>

enum MergeFrequency {
    Hour = 'hour',
    Minute = 'minute',
}

const tableSchema = [
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

const exportTableColumns = tableSchema.map(({ name, type }) => `"${name.toUpperCase()}" ${type}`).join(', ')
const temporaryTableColumns = tableSchema
    .map(({ name, type }) => `"${name.toUpperCase()}" ${type === 'VARIANT' ? 'STRING' : type}`)
    .join(', ')

const jsonFields = new Set(tableSchema.filter(({ type }) => type === 'VARIANT').map(({ name }) => name))

function verifyConfig(config: SnowflakePluginMeta['config']) {
    if (!config.account) {
        throw new Error('Account not provided!')
    }
    if (!config.username) {
        throw new Error('Username not provided!')
    }
    if (!config.password) {
        throw new Error('Password not provided!')
    }
    if (!config.database) {
        throw new Error('Database not provided!')
    }
    if (!config.dbschema) {
        throw new Error('DB Schema not provided!')
    }
    if (!config.table) {
        throw new Error('Table not provided!')
    }
}

function createSnowflakeConnectionPool(config: SnowflakePluginMeta['config']) {
    return createPool(
        {
            create: async () => {
                const connection = snowflake.createConnection({
                    account: config.account,
                    username: config.username,
                    password: config.password,
                })

                await new Promise((resolve, reject) =>
                    connection.connect((err, conn) => {
                        if (err) {
                            console.error('Unable to connect to SnowFlake: ' + err.message)
                            reject(err)
                        } else {
                            resolve(conn.getId())
                        }
                    })
                )

                return connection
            },
            destroy: async (connection) => {
                await new Promise((resolve, reject) =>
                    connection.destroy(function (err) {
                        if (err) {
                            console.error('Unable to disconnect: ' + err.message)
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

function createSnowflakeExecute(snowflakePool: Pool<Connection>): SnowflakeExecute {
    return async ({ sqlText, binds, verbose = false }: SnowflakeExecuteParams): Promise<any[]> => {
        console.log({ sqlText })
        const snowflake = await snowflakePool.acquire()
        try {
            return await new Promise((resolve, reject) =>
                snowflake.execute({
                    sqlText,
                    binds,
                    complete: function (err, _stmt, rows) {
                        if (err) {
                            if (verbose) {
                                console.error('Error executing SnowFlake query', { sqlText, error: err.message })
                            }
                            reject(err)
                        } else {
                            resolve(rows)
                        }
                    },
                })
            )
        } finally {
            await snowflakePool.release(snowflake)
        }
    }
}

async function createTableIfNotExists(
    snowflakeExecute: SnowflakeExecute,
    database: string,
    dbschema: string,
    table: string,
    columns: string
) {
    await snowflakeExecute({
        sqlText: `CREATE TABLE IF NOT EXISTS "${database}"."${dbschema}"."${table}" (${columns})`,
    })
}

async function dropTableIfExists(
    snowflakeExecute: SnowflakeExecute,
    database: string,
    dbschema: string,
    table: string
) {
    await snowflakeExecute({
        sqlText: `DROP TABLE IF EXISTS "${database}"."${dbschema}"."${table}"`,
    })
}

function getCurrentUniqueTime(date = new Date(), mergeFrequency: MergeFrequency): string {
    const isoTime = date.toISOString()
    const [day, time] = isoTime.split('T')
    const times = time.split(':')
    return `${day.split('-').join('')}-${times[0]}${mergeFrequency === MergeFrequency.Minute ? `${times[1]}M` : '00H'}`
}

function getTemporaryTableName(table: string, uniqueTime: string): string {
    return `${table}__BUFFER_${uniqueTime}_${randomBytes(8).toString('hex')}`
}

function getInsertSql(database: string, dbschema: string, table: string): string {
    return `INSERT INTO "${database}"."${dbschema}"."${table}" (${tableSchema
        .map(({ name }) => `"${name.toUpperCase()}"`)
        .join(', ')}) VALUES (${tableSchema.map(() => '?').join(', ')})`
}

async function copyTemporaryToExport(
    snowflakeExecute: SnowflakeExecute,
    database: string,
    dbschema: string,
    tempTable: string,
    exportTable: string
): Promise<void> {
    await snowflakeExecute({
        sqlText: `INSERT INTO "${database}"."${dbschema}"."${exportTable}" (${tableSchema
            .map(({ name }) => `"${name.toUpperCase()}"`)
            .join(', ')}) SELECT ${tableSchema
            .map(({ name, type }) =>
                type === 'VARIANT' ? `PARSE_JSON("${name.toUpperCase()}")` : `"${name.toUpperCase()}"`
            )
            .join(', ')} FROM "${database}"."${dbschema}"."${tempTable}"`,
    })
}

export async function setupPlugin({ global, config }: SnowflakePluginMeta) {
    verifyConfig(config)

    // get the connections
    global.snowflakePool = createSnowflakeConnectionPool(config)
    global.snowflakeExecute = createSnowflakeExecute(global.snowflakePool)

    // create default table
    await createTableIfNotExists(
        global.snowflakeExecute,
        config.database,
        config.dbschema,
        config.table,
        exportTableColumns
    )

    // create temporary table for this worker
    async function setupTemporary(uniqueTime: string) {
        global.uniqueTime = uniqueTime
        global.temporaryTable = getTemporaryTableName(config.table, uniqueTime)
        await createTableIfNotExists(
            global.snowflakeExecute,
            config.database,
            config.dbschema,
            global.temporaryTable,
            temporaryTableColumns
        )
    }

    global.buffer = createBuffer({
        limit: 100 * 1024, // 100 kb
        timeoutSeconds: 10, // 10 seconds
        onFlush: async (batch) => {
            console.log(`Flushing batch of ${batch.length} events to SnowFlake`)
            console.log(`temporaryTable: ${global.temporaryTable}, uniqueTime: ${global.uniqueTime}`)

            const date = new Date()
            if (!global.temporaryTable || global.uniqueTime !== getCurrentUniqueTime(date, config.mergeFrequency)) {
                await setupTemporary(getCurrentUniqueTime(date, config.mergeFrequency))
            }

            try {
                await global.snowflakeExecute({
                    sqlText: getInsertSql(config.database, config.dbschema, global.temporaryTable),
                    binds: batch,
                    verbose: true,
                })
            } catch (e) {
                console.error(e)
            }
        },
    })

    global.eventsToIgnore = new Set<string>(
        (config.eventsToIgnore || '').split(',').map((event: string) => event.trim())
    )

    global.initDone = true
}

export async function teardownPlugin({ global }: SnowflakePluginMeta) {
    global.buffer.flush()
    await global.snowflakePool.drain()
    await global.snowflakePool.clear()
}

export async function processEvent(oneEvent: PluginEvent, { global }: SnowflakePluginMeta) {
    if (!global.initDone) {
        throw new Error('No SnowFlake client connected!')
    }

    if (global.eventsToIgnore.has(oneEvent.event.trim())) {
        return oneEvent
    }

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
        ..._discard
    } = oneEvent
    const ip = properties?.['$ip'] || oneEvent.ip
    const timestamp = oneEvent.timestamp || properties?.timestamp || now || sent_at
    let ingestedProperties = properties
    let elements = []

    // only move prop to elements for the $autocapture action
    if (event === '$autocapture' && properties?.['$elements']) {
        const { $elements, ...props } = properties
        ingestedProperties = props
        elements = $elements
    }

    const row = {
        uuid,
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

    // add an approximate length, as we're not sure what will end up in the final SQL
    global.buffer.add(Object.values(row), JSON.stringify(row).length)

    return oneEvent
}

export async function runEveryMinute({ config, global }: SnowflakePluginMeta) {
    if (!global.initDone) {
        return
    }
    if (config.mergeFrequency === MergeFrequency.Hour && new Date().getMinutes() % 10 === 0) {
        // check every 5min if merging once per hour
        return
    }

    type SnowflakeTable = {
        name: string
        database_name: string
        schema_name: string
        kind: string
        rows: number
        bytes: number
        // and some more fields we don't care about
    }

    const response: SnowflakeTable[] = await global.snowflakeExecute({
        sqlText: `SHOW TABLES LIKE '${config.table}__BUFFER_%' IN "${config.database}"."${config.dbschema}"`,
    })

    const date = new Date()

    const offLimits = [
        // skip this and the last 2 minutes
        getCurrentUniqueTime(date, MergeFrequency.Minute),
        getCurrentUniqueTime(new Date(date.valueOf() - 60000), MergeFrequency.Minute),
        getCurrentUniqueTime(new Date(date.valueOf() - 2 * 60000), MergeFrequency.Minute),

        // skip the last hour if less than 9 minutes passed in this hour
        getCurrentUniqueTime(date, MergeFrequency.Hour),
        getCurrentUniqueTime(new Date(date.valueOf() - 9 * 60000), MergeFrequency.Hour),
    ]

    for (const { name, rows } of response) {
        const uniqueString = name.substring(`${config.table}__BUFFER_`.length)
        const uniqueTime = uniqueString.split('_')[0]
        if (!offLimits.includes(uniqueTime)) {
            await copyTemporaryToExport(
                global.snowflakeExecute,
                config.database,
                config.dbschema,
                `${config.table}__BUFFER_${uniqueString}`,
                config.table
            )
            await dropTableIfExists(
                global.snowflakeExecute,
                config.database,
                config.dbschema,
                `${config.table}__BUFFER_${uniqueString}`
            )
        }
    }
}
