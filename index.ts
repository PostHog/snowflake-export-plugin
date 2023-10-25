import { PluginEvent, Plugin, RetryError, CacheExtension, StorageExtension } from '@posthog/plugin-scaffold'
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
        parsedBucketPath: string
        debug: boolean
    }
    config: {
        eventsToIgnore: string
        bucketName: string
        awsAccessKeyId?: string
        awsSecretAccessKey?: string
        awsRegion?: string
        storageIntegrationName?: string
        stageToUse: 'S3' | 'Google Cloud Storage'
        bucketPath: string
        debug: 'ON' | 'OFF'
    }
    cache: CacheExtension
    storage: StorageExtension
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
    const {
        event,
        elements,
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
    private s3connector: S3 | null
    s3Options: S3AuthOptions | null
    gcsOptions: GCSAuthOptions | null
    gcsConnector: Bucket | null

    constructor() {
        this.s3connector = null
        this.s3Options = null
        this.gcsOptions = null
        this.gcsConnector = null
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
}

const snowflakePlugin: Plugin<SnowflakePluginInput> = {
    async setupPlugin(meta) {
        const { global, config, attachments } = meta

        const requiredConfigOptions = ['bucketName']
        for (const option of requiredConfigOptions) {
            if (!(option in config)) {
                throw new Error(`Required config option ${option} is missing!`)
            }
        }

        global.snowflake = new Snowflake()
        global.debug = config.debug === 'ON'
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

    getSettings(_) {
        return {
            handlesLargeBatches: true,
        }
    },

    async exportEvents(events, meta) {
        const { global, config } = meta
        const rows = events.filter((event) => !global.eventsToIgnore.has(event.event.trim())).map(transformEventToRow)
        if (rows.length) {
            console.info(`Saving batch of ${rows.length} event${rows.length !== 1 ? 's' : ''}`)
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
}

export default snowflakePlugin
