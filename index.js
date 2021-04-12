import snowflake from 'snowflake-sdk'
import { createPool } from 'generic-pool'

export async function setupPlugin({ global, config }) {
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

    global.snowflakePool = createPool(
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
                    connection.destroy(function (err, conn) {
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
            max: 2,
            autostart: true,
        }
    )

    global.snowflakeExecute = async ({ sqlText, binds, verbose = false }) => {
        let snowflake
        try {
            snowflake = await global.snowflakePool.acquire()
            return await new Promise((resolve, reject) =>
                snowflake.execute({
                    sqlText,
                    binds,
                    complete: function (err, stmt, rows) {
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
            await global.snowflakePool.release(snowflake)
        }
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

    const columns = tableSchema.map(({ name, type }) => `"${name.toUpperCase()}" ${type}`).join(', ')

    try {
        await global.snowflakeExecute({
            sqlText: `CREATE TABLE "${config.database}"."${config.dbschema}"."${config.table}" (${columns})`,
        })
    } catch (error) {
        if (!error.message.includes('already exists')) {
            throw error
        }
    }

    global.jsonFields = Object.fromEntries(
        tableSchema.filter(({ type }) => type === 'VARIANT').map(({ name }) => [name, true])
    )
    global.eventsToIgnore = Object.fromEntries(
        (config.eventsToIgnore || '').split(',').map((event) => [event.trim(), true])
    )

    global.initDone = true
}

export async function teardownPlugin({ global }) {
    await global.snowflakePool.drain()
    await global.snowflakePool.clear()
}

export async function processEvent(oneEvent, { global, config }) {
    if (!global.initDone) {
        throw new Error('No SnowFlake client connected!')
    }

    if (global.eventsToIgnore[oneEvent.event]) {
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
    const timestamp = oneEvent.timestamp || oneEvent.data?.timestamp || properties?.timestamp || now || sent_at
    let ingestedProperties = properties
    let elements = []

    // only move prop to elements for the $autocapture action
    if (event === '$autocapture' && properties['$elements']) {
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

    let sqlText = ''
    sqlText += `INSERT INTO "${config.database}"."${config.dbschema}"."${config.table}"`
    sqlText += `(${Object.keys(row)
        .map((r) => `"${r.toUpperCase()}"`)
        .join(', ')})`
    sqlText += ` SELECT `
    sqlText += Object.keys(row)
        .map((a, i) => (global.jsonFields[a] ? `PARSE_JSON(?)` : `?`))
        .join(', ')

    await global.snowflakeExecute({
        sqlText,
        binds: Object.values(row),
        verbose: true,
    })

    return oneEvent
}
