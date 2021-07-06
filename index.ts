import * as snowflake from 'snowflake-sdk'
import { createPool, Pool } from 'generic-pool'
import { PluginEvent, Plugin, RetryError } from '@posthog/plugin-scaffold'
import { randomBytes } from 'crypto'
import { ManagedUpload } from 'aws-sdk/clients/s3'
import { S3 } from 'aws-sdk'

interface SnowflakePluginInput {
    global: {
        snowflake: Snowflake
        eventsToIgnore: Set<string>
        useS3: boolean
        filesStagedForCopy: string[]
        batchEmpty: boolean
        purgeEventsFromStage: boolean
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
        s3BucketName: string
        awsAccessKeyId: string
        awsSecretAccessKey: string
        awsRegion: string
        stageToUse: 'S3' | 'Google Cloud Storage'
        purgeFromStage: 'Yes' | 'No'
        role?: string
    }
}

interface TableRow {
    uuid: string
    event: string
    properties: string // Record<string, any>
    elements: string // Record<string, any>
    people_set: string // Record<string, any>
    people_set_once: string // Record<string, any>
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
    { name: 'people_set', type: 'VARIANT' },
    { name: 'people_set_once', type: 'VARIANT' },
    { name: 'distinct_id', type: 'STRING' },
    { name: 'team_id', type: 'INTEGER' },
    { name: 'ip', type: 'STRING' },
    { name: 'site_url', type: 'STRING' },
    { name: 'timestamp', type: 'TIMESTAMP' },
]

const CSV_FIELD_DELIMITER = '|$|'

function transformEventToRow(fullEvent: PluginEvent): TableRow {
    const { event, properties, $set, $set_once, distinct_id, team_id, site_url, now, sent_at, uuid, ...rest } =
        fullEvent
    const ip = properties?.['$ip'] || fullEvent.ip
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
        event,
        distinct_id,
        team_id,
        ip,
        site_url,
        timestamp,
        uuid: uuid!,
        properties: JSON.stringify(ingestedProperties || {}),
        elements: JSON.stringify(elements || []),
        people_set: JSON.stringify($set || {}),
        people_set_once: JSON.stringify($set_once || {}),
    }
}
interface SnowflakeOptions {
    account: string
    username: string
    password: string
    database: string
    dbschema: string
    table: string
    stage: string
    specifiedRole?: string
}

interface S3AuthOptions {
    awsAccessKeyId: string
    awsSecretAccessKey: string
    bucketName: string
}

interface RetryCopyIntoJobPayload {
    retriesPerformedSoFar: number
    filesStagedForCopy: string[]
}
class Snowflake {
    private pool: Pool<snowflake.Connection>
    private s3connector: S3 | null
    database: string
    dbschema: string
    table: string
    stage: string
    s3Options: S3AuthOptions | null
    specifiedRole?: string

    constructor({ account, username, password, database, dbschema, table, stage, specifiedRole }: SnowflakeOptions) {
        this.pool = this.createConnectionPool(account, username, password, specifiedRole)
        this.s3connector = null
        this.database = database.toUpperCase()
        this.dbschema = dbschema.toUpperCase()
        this.table = table.toUpperCase()
        this.stage = stage.toUpperCase()
        this.s3Options = null
    }

    public async clear(): Promise<void> {
        await this.pool.drain()
        await this.pool.clear()
    }

    public createS3Connector(
        awsAccessKeyId: string,
        awsSecretAccessKey: string,
        awsRegion: string,
        bucketName: string
    ) {
        if (!awsAccessKeyId || !awsSecretAccessKey || !awsRegion || !bucketName) {
            throw new Error(
                'You must provide an AWS Access Key ID, Secret Access Key, bucket name, and bucket region to use the S3 stage.'
            )
        }
        this.s3connector = new S3({
            accessKeyId: awsAccessKeyId,
            secretAccessKey: awsSecretAccessKey,
            region: awsRegion,
        })
        this.s3Options = {
            awsAccessKeyId,
            awsSecretAccessKey,
            bucketName,
        }
    }

    public async createTableIfNotExists(columns: string): Promise<void> {
        await this.execute({
            sqlText: `CREATE TABLE IF NOT EXISTS "${this.database}"."${this.dbschema}"."${this.table}" (${columns})`,
        })
    }

    public async dropTableIfExists(): Promise<void> {
        await this.execute({
            sqlText: `DROP TABLE IF EXISTS "${this.database}"."${this.dbschema}"."${this.table}"`,
        })
    }

    public async createStageIfNotExists(useS3 = true): Promise<void> {
        if (useS3) {
            if (!this.s3Options) {
                throw new Error('S3 connector not initialized correctly.')
            }
            await this.execute({
                sqlText: `CREATE STAGE IF NOT EXISTS "${this.database}"."${this.dbschema}"."${this.stage}"
            URL='s3://${this.s3Options.bucketName}'
            FILE_FORMAT = ( TYPE = 'CSV' SKIP_HEADER = 1 FIELD_DELIMITER = '${CSV_FIELD_DELIMITER}' )
            CREDENTIALS=(aws_key_id='${this.s3Options.awsAccessKeyId}' aws_secret_key='${this.s3Options.awsSecretAccessKey}')
            ENCRYPTION=(type='AWS_SSE_KMS' kms_key_id = 'aws/key')
            COMMENT = 'S3 Stage used by the PostHog Snowflake export plugin';`,
            })
        }
    }

    public async execute({ sqlText, binds }: { sqlText: string; binds?: snowflake.Binds }): Promise<any[] | undefined> {
        const snowflake = await this.pool.acquire()
        try {
            return await new Promise((resolve, reject) =>
                snowflake.execute({
                    sqlText,
                    binds,
                    complete: function (err, _stmt, rows) {
                        if (err) {
                            console.error('Error executing Snowflake query: ', { sqlText, error: err.message })
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

    private createConnectionPool(account: string, username: string, password: string, specifiedRole?: string): Snowflake['pool'] {
        const roleConfig = specifiedRole ? { role: specifiedRole } : {}
        return createPool(
            {
                create: async () => {
                    const connection = snowflake.createConnection({
                        account,
                        username,
                        password,
                        database: this.database,
                        schema: this.dbschema,
                        ...roleConfig
                    })

                    await new Promise<string>((resolve, reject) =>
                        connection.connect((err, conn) => {
                            if (err) {
                                console.error('Error connecting to Snowflake: ' + err.message)
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

    async uploadToS3(events: TableRow[], meta: SnowflakePluginInput) {
        if (!this.s3connector) {
            throw new Error('S3 connector not setup correctly!')
        }
        const { global, config } = meta

        const date = new Date().toISOString()
        const [day, time] = date.split('T')
        const dayTime = `${day.split('-').join('')}-${time.split(':').join('')}`
        const suffix = randomBytes(8).toString('hex')

        let csvString =
            'uuid,event,properties,elements,people_set,people_set_once,distinct_id,team_id,ip,site_url,timestamp\n'

        for (let i = 0; i < events.length; ++i) {
            const {
                uuid,
                event,
                properties,
                elements,
                people_set,
                people_set_once,
                distinct_id,
                team_id,
                ip,
                site_url,
                timestamp,
            } = events[i]

            const d = CSV_FIELD_DELIMITER
            // order is important
            csvString += `${uuid}${d}${event}${d}${properties}${d}${elements}${d}${people_set}${d}${people_set_once}${d}${distinct_id}${d}${team_id}${d}${ip}${d}${site_url}${d}${timestamp}`

            if (i !== events.length - 1) {
                csvString += '\n'
            }
        }

        const fileName = `snowflake-export-${day}-${dayTime}-${suffix}.csv`

        const params = {
            Bucket: config.s3BucketName,
            Key: fileName,
            Body: Buffer.from(csvString, 'utf8'),
        }

        console.log(`Flushing ${events.length} events!`)
        await new Promise<void>((resolve, reject) => {
            this.s3connector!.upload(params, async (err: Error, _: ManagedUpload.SendData) => {
                if (err) {
                    console.error(`Error uploading to S3: ${err.message}`)
                    reject()
                }
                console.log(
                    `Uploaded ${events.length} event${events.length === 1 ? '' : 's'} to bucket ${config.s3BucketName}`
                )
                resolve()
            })
        })
        global.filesStagedForCopy.push(fileName)
    }

    async copyIntoTableFromStage(files: string[], purge = false) {
        let filesList = ''
        for (let i = 0; i < files.length; ++i) {
            filesList += `'${files[i]}'`
            if (i !== files.length-1) {
                filesList += ','
            }
        }
        await this.execute({
            sqlText: `COPY INTO "${this.database}"."${this.dbschema}"."${this.table}"
            FROM @"${this.database}"."${this.dbschema}".${this.stage}
            FILES = ( ${filesList} )
            PURGE = ${purge};`,
        })
    }
}

const exportTableColumns = TABLE_SCHEMA.map(({ name, type }) => `"${name.toUpperCase()}" ${type}`).join(', ')

const snowflakePlugin: Plugin<SnowflakePluginInput> = {
    jobs: {
        retryCopyIntoSnowflake: async (payload: RetryCopyIntoJobPayload, { global, jobs }) => {
            if (payload.retriesPerformedSoFar >= 15) {
                return
            }
            try {
                await global.snowflake.copyIntoTableFromStage(payload.filesStagedForCopy, global.purgeEventsFromStage)
            } catch {
                const nextRetrySeconds = 2 ** payload.retriesPerformedSoFar * 3
                await jobs.retryCopyIntoSnowflake({ retriesPerformedSoFar: payload.retriesPerformedSoFar+1, filesStagedForCopy: payload.filesStagedForCopy }).runIn(nextRetrySeconds, 'seconds')
                console.error(`Failed to copy ${String(payload.filesStagedForCopy)} from S3 into Snowflake. Retrying in ${nextRetrySeconds}s.`)
            }
        },
    },

    async setupPlugin(meta) {
        const { global, config } = meta
        const { account, username, password, dbschema, table, stage, database, role } = config
        // Prepare for working with Snowflake
        global.snowflake = new Snowflake({
            account,
            username,
            password,
            dbschema,
            table,
            stage,
            database,
            specifiedRole: role
        })

        // Create table
        await global.snowflake.createTableIfNotExists(exportTableColumns)

        global.purgeEventsFromStage = config.purgeFromStage === 'Yes'

        global.useS3 = config.stageToUse === 'S3'
        if (global.useS3) {
            global.snowflake.createS3Connector(
                config.awsAccessKeyId,
                config.awsSecretAccessKey,
                config.awsRegion,
                config.s3BucketName
            )
        }

        // Create stage
        await global.snowflake.createStageIfNotExists()

        global.filesStagedForCopy = []
        global.batchEmpty = true

        global.eventsToIgnore = new Set<string>((config.eventsToIgnore || '').split(',').map((event) => event.trim()))
    },

    async teardownPlugin({ global }) {
        await global.snowflake.clear()
    },

    async exportEvents(events, meta) {
        const { global, config } = meta
        const rows = events.filter((event) => !global.eventsToIgnore.has(event.event.trim())).map(transformEventToRow)
        if (rows.length) {
            console.info(
                `Saving batch of ${rows.length} event${rows.length !== 1 ? 's' : ''} to Snowflake stage "${
                    config.stage
                }"`
            )
        } else {
            console.info(`Skipping an empty batch of events`)
        }
        try {
            await global.snowflake.uploadToS3(rows, meta)
            global.batchEmpty = false
        } catch (error) {
            throw new RetryError()
        }
    },

    async runEveryMinute({ cache, global, jobs }) {
        if (global.batchEmpty) {
            return
        }
        const lastRun = await cache.get('lastRun', null)
        const ONE_HOUR = 60 * 60 * 1000
        const timeNow = new Date().getTime()
        if (lastRun && timeNow - Number(lastRun) < ONE_HOUR) {
            return
        }
        await cache.set('lastRun', timeNow)
        if (global.useS3) {
            console.log(`Copying ${String(global.filesStagedForCopy)} from S3 into Snowflake`)
            try {
                await global.snowflake.copyIntoTableFromStage(global.filesStagedForCopy, global.purgeEventsFromStage)
            } catch {
                await jobs.retryCopyIntoSnowflake({ retriesPerformedSoFar: 0, filesStagedForCopy: global.filesStagedForCopy }).runIn(3, 'seconds')
                console.error(`Failed to copy ${String(global.filesStagedForCopy)} from S3 into Snowflake. Retrying in 3s.`)
            }
        }
        global.filesStagedForCopy = []
        global.batchEmpty = true
    },
}

export default snowflakePlugin
