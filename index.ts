import * as snowflake from 'snowflake-sdk'
import { createPool, Pool } from 'generic-pool'
import { PluginEvent, Plugin, RetryError, CacheExtension, Meta, StorageExtension } from '@posthog/plugin-scaffold'
import { randomBytes } from 'crypto'
import { ManagedUpload } from 'aws-sdk/clients/s3'
import { S3 } from 'aws-sdk'
import { Storage, Bucket } from '@google-cloud/storage'
import { PassThrough } from 'stream'

interface SnowflakePluginInput {
    global: {
        snowflake: Snowflake
        eventsToIgnore: Set<string>
        useS3: boolean
        purgeEventsFromStage: boolean
        parsedBucketPath: string
        forceCopy: boolean
        debug: boolean
        copyCadenceMinutes: number
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
        bucketName: string
        warehouse: string
        awsAccessKeyId?: string
        awsSecretAccessKey?: string
        awsRegion?: string
        storageIntegrationName?: string
        role?: string
        stageToUse: 'S3' | 'Google Cloud Storage'
        purgeFromStage: 'Yes' | 'No'
        bucketPath: string
        retryCopyIntoOperations: 'Yes' | 'No'
        forceCopy: 'Yes' | 'No'
        debug: 'ON' | 'OFF'
        copyCadenceMinutes: string
    }
    cache: CacheExtension
    storage: StorageExtension
}

/**
 * Util function to return a promise which is resolved in provided milliseconds

 * 
 * @param millSeconds milliseconds to wait before resolving
 * @returns
 */
function waitFor(millSeconds) {
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      resolve();
    }, millSeconds);
  });
}

/**
 * Util function to return a promise which is resolved or times out in provided milliseconds
 *  
 * @param prom promise to be resolved 
 * @param time timeout in milliseconds 
 * @returns 
 */
const timeout = (prom, time) =>
	Promise.race([prom, new Promise((_r, rej) => setTimeout(rej, time))]);

/**
 * Util function to retry a promise with a delay between retries
 * as well as timeouts for each retry
 * 
 * @param promise promise to be resolved
 * @param nthTry number of times to retry
 * @param delayTime delay between retries in milliseconds
 * @param timeoutTime timeout for each retry in milliseconds
 * @returns
 */  
async function retryPromiseWithDelay(promise, nthTry, delayTime, timeoutTime) {
  try {
    const res = await timeout(promise, timeoutTime);
    return res;
  } catch (e) {
    if (nthTry <= 1) {
      return Promise.reject(e);
    }
    console.log('retrying', nthTry, 'time');
    // wait for delayTime amount of time before calling this method again
    await waitFor(delayTime);
    return retryPromiseWithDelay(promise, nthTry - 1, delayTime);
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

interface SnowflakeOptions {
    account: string
    username: string
    password: string
    database: string
    dbschema: string
    table: string
    stage: string
    warehouse: string
    specifiedRole?: string
}

interface S3AuthOptions {
    awsAccessKeyId: string
    awsSecretAccessKey: string
}

interface GCSAuthOptions {
    storageIntegrationName: string
}

interface GCSCredentials {
    project_id?: string
    client_email?: string
    private_key?: string
}

interface RetryCopyIntoJobPayload {
    retriesPerformedSoFar: number
    filesStagedForCopy: string[]
}

interface SnowFlakeColumn {
    name: string
    type: string
}

type SnowFlakeTableSchema = SnowFlakeColumn[]

const TABLE_SCHEMA: SnowFlakeTableSchema = [
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
const FILES_STAGED_KEY = '_files_staged_for_copy_into_snowflake'

// NOTE: we patch the Event type with an elements property as it is not in the
// one imported from the scaffolding. The original assumption was that there
// would be an $elements property in `properties`, but in reality this isn't
// what the plugin server sends to `exportEvents`. Rather, it sends something
// that is similar to a `PluginEvent`, but without $elements in `properties` and
// instead it has already been moved to the top level as `elements`.
// TODO: add elements to `PluginEvent` within scaffolding, or use a different
// type
type PluginEventWithElements = PluginEvent & { elements: { [key: string]: any }[] }

function transformEventToRow(fullEvent: PluginEventWithElements): TableRow {
    const { event, elements, properties, $set, $set_once, distinct_id, team_id, site_url, now, sent_at, uuid, ...rest } =
        fullEvent
    const ip = properties?.['$ip'] || fullEvent.ip
    const timestamp = fullEvent.timestamp || properties?.timestamp || now || sent_at
    let ingestedProperties = properties

    return {
        event,
        distinct_id,
        team_id,
        ip,
        site_url,
        timestamp,
        uuid: uuid!,
        properties: JSON.stringify(ingestedProperties || {}),
        elements: JSON.stringify(elements ?? properties?.$elements ?? []),
        people_set: JSON.stringify($set || {}),
        people_set_once: JSON.stringify($set_once || {}),
    }
}

function generateCsvFileName(): string {
    const date = new Date().toISOString()
    const [day, time] = date.split('T')
    const dayTime = `${day.split('-').join('')}-${time.split(':').join('')}`
    const suffix = randomBytes(8).toString('hex')

    return `snowflake-export-${day}-${dayTime}-${suffix}.csv`
}

function generateCsvString(events: TableRow[]): string {
    const columns: (keyof TableRow)[] = [
        'uuid',
        'event',
        'properties',
        'elements',
        'people_set',
        'people_set_once',
        'distinct_id',
        'team_id',
        'ip',
        'site_url',
        'timestamp',
    ]
    const csvHeader = columns.join(CSV_FIELD_DELIMITER)
    const csvRows: string[] = [csvHeader]
    events.forEach((currentEvent) => { 
        csvRows.push(columns.map((column) => (currentEvent[column] || '').toString()).join(CSV_FIELD_DELIMITER))
    })
    return csvRows.join('\n')
}

class Snowflake {
    private pool: Pool<snowflake.Connection>
    private s3connector: S3 | null
    database: string
    dbschema: string
    table: string
    stage: string
    warehouse: string
    s3Options: S3AuthOptions | null
    gcsOptions: GCSAuthOptions | null
    gcsConnector: Bucket | null

    constructor({
        account,
        username,
        password,
        database,
        dbschema,
        table,
        stage,
        specifiedRole,
        warehouse,
    }: SnowflakeOptions) {
        this.pool = this.createConnectionPool(account, username, password, specifiedRole)
        this.s3connector = null
        this.database = database.toUpperCase()
        this.dbschema = dbschema.toUpperCase()
        this.table = table.toUpperCase()
        this.stage = stage.toUpperCase()
        this.warehouse = warehouse.toUpperCase()
        this.s3Options = null
        this.gcsOptions = null
        this.gcsConnector = null
    }

    public async clear(): Promise<void> {
        await this.pool.drain()
        await this.pool.clear()
    }

    public createS3Connector(awsAccessKeyId?: string, awsSecretAccessKey?: string, awsRegion?: string) {
        if (!awsAccessKeyId || !awsSecretAccessKey || !awsRegion) {
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
        }
    }

    public createGCSConnector(credentials: GCSCredentials, bucketName: string, storageIntegrationName?: string) {
        if (!credentials || !storageIntegrationName) {
            throw new Error(
                'You must provide valid credentials and your storage integration name to use the GCS stage.'
            )
        }
        const gcsStorage = new Storage({
            projectId: credentials['project_id'],
            credentials,
            autoRetry: false,
        })
        this.gcsConnector = gcsStorage.bucket(bucketName)
        this.gcsOptions = { storageIntegrationName: storageIntegrationName.toUpperCase() }
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

    public async createStageIfNotExists(useS3: boolean, bucketName: string): Promise<void> {
        bucketName = bucketName.endsWith('/') ? bucketName : `${bucketName}/`
        if (useS3) {
            if (!this.s3Options) {
                throw new Error('S3 connector not initialized correctly.')
            }
            await this.execute({
                sqlText: `CREATE STAGE IF NOT EXISTS "${this.database}"."${this.dbschema}"."${this.stage}"
            URL='s3://${bucketName}'
            FILE_FORMAT = ( TYPE = 'CSV' SKIP_HEADER = 1 FIELD_DELIMITER = '${CSV_FIELD_DELIMITER}', ESCAPE = NONE, ESCAPE_UNENCLOSED_FIELD = NONE )
            CREDENTIALS=(aws_key_id='${this.s3Options.awsAccessKeyId}' aws_secret_key='${this.s3Options.awsSecretAccessKey}')
            ENCRYPTION=(type='AWS_SSE_KMS' kms_key_id = 'aws/key')
            COMMENT = 'S3 Stage used by the PostHog Snowflake export plugin';`,
            })

            return
        }

        if (!this.gcsOptions) {
            throw new Error('GCS connector not initialized correctly.')
        }

        await this.execute({
            sqlText: `CREATE STAGE IF NOT EXISTS "${this.database}"."${this.dbschema}"."${this.stage}"
        URL='gcs://${bucketName}'
        FILE_FORMAT = ( TYPE = 'CSV' SKIP_HEADER = 1 FIELD_DELIMITER = '${CSV_FIELD_DELIMITER}', ESCAPE = NONE, ESCAPE_UNENCLOSED_FIELD = NONE )
        STORAGE_INTEGRATION = ${this.gcsOptions.storageIntegrationName}
        COMMENT = 'GCS Stage used by the PostHog Snowflake export plugin';`,
        })
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

    private createConnectionPool(
        account: string,
        username: string,
        password: string,
        specifiedRole?: string
    ): Snowflake['pool'] {
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
                        ...roleConfig,
                    })

                    await retryPromiseWithDelay(new Promise<string>((resolve, reject) =>
                        connection.connect((err, conn) => {
                            if (err) {
                                console.error('Error connecting to Snowflake: ' + err.message)
                                reject(err)
                            } else {
                                resolve(conn.getId())
                            }
                        })
                    ), 5, 5000, 5000)
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

    async appendToFilesList(storage: StorageExtension, fileName: string) {
        const existingFiles = (await storage.get(FILES_STAGED_KEY, [])) as string[]
        await storage.set(FILES_STAGED_KEY, existingFiles.concat([fileName]))
    }

    async uploadToS3(events: TableRow[], meta: SnowflakePluginInput) {
        if (!this.s3connector) {
            throw new Error('S3 connector not setup correctly!')
        }
        const { config, global, storage } = meta

        const csvString = generateCsvString(events)
        const fileName = `${global.parsedBucketPath}${generateCsvFileName()}`

        const params = {
            Bucket: config.bucketName,
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
                    `Uploaded ${events.length} event${events.length === 1 ? '' : 's'} to bucket ${config.bucketName}`
                )
                resolve()
            })
        })
        await this.appendToFilesList(storage, fileName)
    }

    async uploadToGcs(events: TableRow[], { global, storage }: SnowflakePluginInput) {
        if (!this.gcsConnector) {
            throw new Error('GCS connector not setup correctly!')
        }
        const csvString = generateCsvString(events)
        const fileName = `${global.parsedBucketPath}${generateCsvFileName()}`

        // some minor hackiness to upload without access to the filesystem
        const dataStream = new PassThrough()
        const gcFile = this.gcsConnector.file(fileName)

        dataStream.push(csvString)
        dataStream.push(null)

        await new Promise((resolve, reject) => {
            dataStream
                .pipe(
                    gcFile.createWriteStream({
                        resumable: false,
                        validation: false,
                    })
                )
                .on('error', (error: Error) => {
                    reject(error)
                })
                .on('finish', () => {
                    resolve(true)
                })
        })
        await this.appendToFilesList(storage, fileName)
    }

    async copyIntoTableFromStage(files: string[], purge = false, forceCopy = false, debug = false) {
        if (debug) {
            console.log('Trying to copy events into Snowflake')
        }
        await this.execute({
            sqlText: `USE WAREHOUSE ${this.warehouse};`,
        })

        const querySqlText = `COPY INTO "${this.database}"."${this.dbschema}"."${this.table}"
        FROM @"${this.database}"."${this.dbschema}".${this.stage}
        FILES = ( ${files.map((file) => `'${file}'`).join(',')} )
        ${forceCopy ? `FORCE = true` : ``}
        ON_ERROR = 'skip_file'
        PURGE = ${purge};`

        await this.execute({
            sqlText: querySqlText,
        })

        console.log('COPY INTO ran successfully')
    }
}

const exportTableColumns = TABLE_SCHEMA.map(({ name, type }) => `"${name.toUpperCase()}" ${type}`).join(', ')

const snowflakePlugin: Plugin<SnowflakePluginInput> = {
    jobs: {
        retryCopyIntoSnowflake: async (payload: RetryCopyIntoJobPayload, { global, jobs, config }) => {
            if (payload.retriesPerformedSoFar >= 15 || config.retryCopyIntoOperations === 'No') {
                return
            }
            try {
                await global.snowflake.copyIntoTableFromStage(
                    payload.filesStagedForCopy,
                    global.purgeEventsFromStage,
                    global.forceCopy,
                    global.debug
                )
            } catch {
                const nextRetrySeconds = 2 ** payload.retriesPerformedSoFar * 3
                await jobs
                    .retryCopyIntoSnowflake({
                        retriesPerformedSoFar: payload.retriesPerformedSoFar + 1,
                        filesStagedForCopy: payload.filesStagedForCopy,
                    })
                    .runIn(nextRetrySeconds, 'seconds')
                console.error(
                    `Failed to copy ${String(
                        payload.filesStagedForCopy
                    )} from object storage into Snowflake. Retrying in ${nextRetrySeconds}s.`
                )
            }
        },
        copyIntoSnowflakeJob: async (_, meta) => await copyIntoSnowflake(meta)
    },

    async setupPlugin(meta) {
        const { global, config, attachments } = meta

        const requiredConfigOptions = [
            'account',
            'username',
            'password',
            'dbschema',
            'table',
            'stage',
            'database',
            'bucketName',
            'warehouse',
        ]
        for (const option of requiredConfigOptions) {
            if (!(option in config)) {
                throw new Error(`Required config option ${option} is missing!`)
            }
        }

        const { account, username, password, dbschema, table, stage, database, role, warehouse, copyCadenceMinutes } = config

        global.snowflake = new Snowflake({
            account,
            username,
            password,
            dbschema,
            table,
            stage,
            database,
            warehouse,
            specifiedRole: role,
        })
        const parsedCopyCadenceMinutes = parseInt(copyCadenceMinutes)
        global.copyCadenceMinutes = parsedCopyCadenceMinutes > 0 ? parsedCopyCadenceMinutes : 10

        await global.snowflake.createTableIfNotExists(exportTableColumns)

        global.purgeEventsFromStage = config.purgeFromStage === 'Yes'
        global.debug = config.debug === 'ON'
        global.forceCopy = config.forceCopy === 'Yes'

        global.useS3 = config.stageToUse === 'S3'
        if (global.useS3) {
            global.snowflake.createS3Connector(config.awsAccessKeyId, config.awsSecretAccessKey, config.awsRegion)
        } else {
            if (!attachments.gcsCredentials) {
                throw new Error('Credentials JSON file not provided!')
            }
            let credentials: GCSCredentials
            try {
                credentials = JSON.parse(attachments.gcsCredentials.contents.toString())
            } catch {
                throw new Error('Credentials JSON file has invalid JSON!')
            }
            global.snowflake.createGCSConnector(credentials, config.bucketName, config.storageIntegrationName)
        }

        await global.snowflake.createStageIfNotExists(global.useS3, config.bucketName)

        global.eventsToIgnore = new Set<string>((config.eventsToIgnore || '').split(',').map((event) => event.trim()))

        let bucketPath = config.bucketPath
        if (bucketPath && !bucketPath.endsWith('/')) {
            bucketPath = `${config.bucketPath}/`
        }

        if (bucketPath.startsWith('/')) {
            bucketPath = bucketPath.slice(1)
        }

        global.parsedBucketPath = bucketPath
    },

    async teardownPlugin(meta) {
        const { global } = meta
        try {
            // prevent some issues with plugin reloads
            await copyIntoSnowflake(meta, true)
        } catch { }
        await global.snowflake.clear()
    },

    async exportEvents(events, meta) {
        const { global, config } = meta
        const rows = events.filter((event) => !global.eventsToIgnore.has(event.event.trim())).map(transformEventToRow)
        if (rows.length) {
            console.info(
                `Saving batch of ${rows.length} event${rows.length !== 1 ? 's' : ''} to Snowflake stage "${config.stage
                }"`
            )
        } else {
            console.info(`Skipping an empty batch of events`)
            return
        }
        try {
            if (global.useS3) {
                console.log('Uploading to S3')
                await global.snowflake.uploadToS3(rows, meta)
            } else {
                await global.snowflake.uploadToGcs(rows, meta)
            }
        } catch (error) {
            console.error((error as Error).message || String(error))
            throw new RetryError()
        }
    },

    async runEveryMinute(meta) {
        // Run copyIntoSnowflake more often to spread out load better
        await meta.jobs.copyIntoSnowflakeJob({}).runIn(20, 'seconds')
        await meta.jobs.copyIntoSnowflakeJob({}).runIn(40, 'seconds')
        await copyIntoSnowflake(meta)
    },
}

async function copyIntoSnowflake({ cache, storage, global, jobs, config }: Meta<SnowflakePluginInput>, force = false) {
    if (global.debug) {
        console.info('Running copyIntoSnowflake')
    }

    const filesStagedForCopy = (await storage.get(FILES_STAGED_KEY, [])) as string[]
    if (filesStagedForCopy.length === 0) {
        if (global.debug) {
            console.log('No files stagged skipping')
        }
        return
    }

    const lastRun = await cache.get('lastRun', null)
    const maxTime = global.copyCadenceMinutes * 60 * 1000
    const timeNow = new Date().getTime()
    if (!force && lastRun && timeNow - Number(lastRun) < maxTime) {
        if (global.debug) {
            console.log('Skipping COPY INTO', timeNow, lastRun)
        }
        return
    }
    await cache.set('lastRun', timeNow)
    console.log(`Copying ${String(filesStagedForCopy)} from object storage into Snowflake`)

    const chunkSize = 50
    for (let i = 0; i < filesStagedForCopy.length; i += chunkSize) {
        const chunkStagedForCopy = filesStagedForCopy.slice(i, i + chunkSize)

        if (i === 0) {
            try {
                await global.snowflake.copyIntoTableFromStage(
                    chunkStagedForCopy,
                    global.purgeEventsFromStage,
                    global.forceCopy,
                    global.debug
                )
                console.log('COPY INTO ran successfully')

                // if we succeed, go to the next chunk, else we'll enqueue a retry below
                continue
            } catch {
                console.error(
                    `Failed to copy ${String(filesStagedForCopy)} from object storage into Snowflake. Retrying in 3s.`
                )
            }
        }

        await jobs
            .retryCopyIntoSnowflake({ retriesPerformedSoFar: 0, filesStagedForCopy: chunkStagedForCopy })
            .runIn(3, 'seconds')
    }

    await storage.del(FILES_STAGED_KEY)
}

export default snowflakePlugin
