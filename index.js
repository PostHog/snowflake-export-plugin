import snowflake from 'snowflake-sdk'

async function setupPlugin({ global, config }) {
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

    const connection = snowflake.createConnection({
        account: config.account,
        username: config.username,
        password: config.password
    })

    await new Promise((resolve, reject) => connection.connect(
        (err, conn) => {
            if (err) {
                console.error('Unable to connect to SnowFlake: ' + err.message)
                reject(err)
            } else {
                resolve(conn.getId())
            }
        }
    ))

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

    await new Promise((resolve, reject) => connection.execute({
        sqlText: `CREATE TABLE "${config.database}"."${config.dbschema}"."${config.table}" (${columns})`,
        complete: function(err, stmt, rows) {
            if (err && !err.message.includes('already exists')) {
                console.error('Failed to execute statement due to the following error: ' + err.message);
                reject(err)
            } else {
                resolve()
            }
        }
    }))

    global.connection = connection
    global.jsonFields = Object.fromEntries(tableSchema.filter(({ type }) => type === 'VARIANT').map(({ name }) => [name, true]))
    global.eventsToIgnore = Object.fromEntries((config.eventsToIgnore || '').split(',').map((event) => [event.trim(), true]))
}

async function processEvent(oneEvent, { global, config }) {
    if (!global.connection) {
        throw new Error('No SnowFlake client connected!')
    }

    if (global.eventsToIgnore[oneEvent.event]) {
        return oneEvent
    }

    const { event, properties, $set, $set_once, distinct_id, team_id, site_url, now, sent_at, uuid, ..._discard } = oneEvent
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
    sqlText += `(${Object.keys(row).map(r => `"${r.toUpperCase()}"`).join(', ')})`
    sqlText += ` SELECT `
    sqlText += Object.keys(row).map((a, i) => global.jsonFields[a] ? `PARSE_JSON(?)` : `?`).join(', ')

    await new Promise((resolve, reject) => global.connection.execute({
        sqlText,
        binds: Object.values(row),
        complete: function(err, stmt, rows) {
            if (err) {
                console.error('Error inserting into SnowFlake: ' + err.message);
                reject(err)
            }
            resolve(rows)
        }
    }))

    return oneEvent
}