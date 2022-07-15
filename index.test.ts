
import snowflakePlugin from "./index"
import AWS, { S3 } from 'aws-sdk'
import Redis from 'ioredis'
import { v4 as uuid4 } from "uuid"
import { setupServer } from "msw/node"
import { rest } from "msw"
import zlib from "zlib"

jest.setTimeout(50000)

test("handles events", async () => {
    // Checks for the happy path
    //
    // TODO: check for:
    //
    //  1. snowflake retry functionality
    //  2. s3 failure cases
    //  3. what happens if [workhouse is suspended](https://posthogusers.slack.com/archives/C01GLBKHKQT/p1650526998274619?thread_ts=1649761835.322489&cid=C01GLBKHKQT)

    AWS.config.update({
        accessKeyId: "awsAccessKeyId",
        secretAccessKey: "awsSecretAccessKey",
        region: "us-east-1",
        s3ForcePathStyle: true,
        s3: {
            endpoint: 'http://localhost:4566'
        }
    })

    // NOTE: we create random names for tests such that we can run tests
    // concurrently without fear of conflicts
    const bucketName = uuid4()
    const snowflakeAccount = uuid4()

    const s3 = new S3()
    await s3.createBucket({ Bucket: bucketName }).promise()

    const meta = {
        attachments: {}, config: {
            account: snowflakeAccount,
            username: "username",
            password: "password",
            database: "database",
            dbschema: "dbschema",
            table: "table",
            stage: "S3",
            eventsToIgnore: "eventsToIgnore",
            bucketName: bucketName,
            warehouse: "warehouse",
            awsAccessKeyId: "awsAccessKeyId",
            awsSecretAccessKey: "awsSecretAccessKey",
            awsRegion: "string",
            storageIntegrationName: "storageIntegrationName",
            role: "role",
            stageToUse: 'S3' as const,
            purgeFromStage: 'Yes' as const,
            bucketPath: "bucketPath",
            retryCopyIntoOperations: 'Yes' as const,
            forceCopy: 'Yes' as const,
            debug: 'ON' as const,
        },
        jobs: { retryCopyIntoSnowflake: async (payload) => { 
            await snowflakePlugin.copyIntoTableFromStage(payload) 
            return jest.fn()
        } 
    },
        cache: cache,
        storage: storage,
        // Cast to any, as otherwise we don't match plugin call signatures
        global: {} as any,
        geoip: {} as any
    }

    const events = [
        {
            event: "some",
            distinct_id: "123",
            ip: "10.10.10.10",
            site_url: "https://app.posthog.com",
            team_id: 1,
            now: "2020-01-01T01:01:01Z"
        },
        {
            event: "events",
            distinct_id: "456",
            ip: "10.10.10.10",
            site_url: "https://app.posthog.com",
            team_id: 1,
            now: "2020-01-01T01:01:01Z"
        }
    ]
    const db = createSnowflakeMock(snowflakeAccount)

    await snowflakePlugin.setupPlugin?.(meta)
    for (let i = 0; i < 3; i++) { // to have >1 files to copy over
        await cache.expire('lastRun', 0)
        await snowflakePlugin.exportEvents?.(events, meta)
    }
    await snowflakePlugin.runEveryMinute?.(meta)
    await snowflakePlugin.teardownPlugin?.(meta)

    const s3Keys = (await s3.listObjects({ Bucket: bucketName }).promise()).Contents?.map((obj) => obj.Key) || []
    expect(s3Keys.length).toEqual(3)

    // Snowflake gets the right files
    const filesLists = db.queries.map(query => /FILES = (?<files>.*)/m.exec(query)?.groups.files).filter(Boolean)
    const copiedFiles = filesLists.map(files => files.split("'").filter(file => file.includes("csv"))).flat()
    expect(copiedFiles.sort()).toEqual(s3Keys.sort())

    // The content in S3 is what we expect
    const csvStrings = await Promise.all(s3Keys.map(async s3Key => {
        const response = await s3.getObject({ Bucket: bucketName, Key: s3Key }).promise()
        return (response.Body || "").toString('utf8')
    }))

    expect(csvStrings.join()).toContain("123")
    expect(csvStrings.join()).toContain("456")
})

test("handles > 1k files", async () => {
    // NOTE: we create random names for tests such that we can run tests
    // concurrently without fear of conflicts
    const bucketName = uuid4()
    const snowflakeAccount = uuid4()

    const snowflakeMock = jest.fn()
    const meta = {
        attachments: {}, config: {
            account: snowflakeAccount,
            username: "username",
            password: "password",
            database: "database",
            dbschema: "dbschema",
            table: "table",
            stage: "S3",
            eventsToIgnore: "eventsToIgnore",
            bucketName: bucketName,
            warehouse: "warehouse",
            awsAccessKeyId: "awsAccessKeyId",
            awsSecretAccessKey: "awsSecretAccessKey",
            awsRegion: "string",
            storageIntegrationName: "storageIntegrationName",
            role: "role",
            stageToUse: 'S3' as const,
            purgeFromStage: 'Yes' as const,
            bucketPath: "bucketPath",
            retryCopyIntoOperations: 'Yes' as const,
            forceCopy: 'Yes' as const,
            debug: 'ON' as const,
        },
        jobs: { retryCopyIntoSnowflake: () => ({ runIn: snowflakeMock } )},
        cache: cache,
        storage: storage,
        // Cast to any, as otherwise we don't match plugin call signatures
        global: {snowflake: {copyIntoTableFromStage: snowflakeMock}} as any,
        geoip: {} as any
    }

    await storage.set('_files_staged_for_copy_into_snowflake', Array(2100).fill('file'))
    await cache.expire('lastRun', 0)
    await snowflakePlugin.runEveryMinute(meta) 

   expect(snowflakeMock.mock.calls.length).toBe(42)

})


// Use fake timers so we can better control e.g. backoff/retry code.
// Use legacy fake timers. With modern timers there seems to be little feedback
// on fails due to test timeouts.
beforeEach(() => {
    jest.useFakeTimers("legacy")
})

afterEach(() => {
    jest.runOnlyPendingTimers()
    jest.useRealTimers()
})

// Create something that looks like the expected cache interface. Note it only
// differs by the addition of the `defaultValue` argument.
// Redis is required to handle staging(?) of S3 files to be pushed to snowflake
let redis: Redis | undefined;
let cache: any
let storage: any
let mockStorage: Map<string, unknown>

beforeAll(() => {
    redis = new Redis()

    cache = {
        lpush: redis.lpush.bind(redis),
        llen: redis.llen.bind(redis),
        lrange: redis.lrange.bind(redis),
        set: redis.set.bind(redis),
        expire: redis.expire.bind(redis),
        get: (key: string, defaultValue: unknown) => redis.get(key)
    }

    mockStorage = new Map()
    storage = {
        // Based of https://github.com/PostHog/posthog/blob/master/plugin-server/src/worker/vm/extensions/storage.ts
        get: async function (key: string, defaultValue: unknown): Promise<unknown> {
            await Promise.resolve()
            if (mockStorage.has(key)) {
                const res = mockStorage.get(key)
                if (res) {
                    return JSON.parse(String(res))
                }
            }
            return defaultValue
        },
        set: async function (key: string, value: unknown): Promise<void> {
            await Promise.resolve()
            if (typeof value === 'undefined') {
                mockStorage.delete(key)
            } else {
                mockStorage.set(key, JSON.stringify(value))
            }
        },
        del: async function (key: string): Promise<void> {
            await Promise.resolve()
            mockStorage.delete(key)
        },
    }

})

afterAll(() => {
    redis.quit()
})


// Setup Snowflake MSW service
const mswServer = setupServer()

beforeAll(() => {
    mswServer.listen()
})

afterAll(() => {
    mswServer.close()
})

const createSnowflakeMock = (accountName: string) => {
    // Create something that kind of looks like snowflake, albeit not
    // functional.
    const baseUri = `https://${accountName}.snowflakecomputing.com`

    const db = { queries: [] }

    mswServer.use(
        // Before making queries, we need to login via username/password and get
        // a token we can use for subsequent auth requests.
        rest.post(`${baseUri}/session/v1/login-request`, (req, res, ctx) => {
            return res(ctx.json({
                "data": {
                    "token": "token",
                },
                "code": null,
                "message": null,
                "success": true
            }))
        }),
        // The API seems to follow this pattern:
        //
        //  1. POST your SQL query up to the API, the resulting resource
        //     identified via a query id. However, we don't actually need to do
        //     anything explicitly with this id, rather we use we...
        //  2. use the getResultUrl to fetch the results of the query
        //
        // TODO: handle case when query isn't complete on requesting getResultUrl
        rest.post(`${baseUri}/queries/v1/query-request`, async (req, res, ctx) => {
            const queryId = uuid4()
            // snowflake-sdk encodes the request body as gzip, which we recieve
            // in this handler as a stringified hex sequence.
            const requestJson = await new Promise(
                (resolve, reject) => {
                    zlib.gunzip(Buffer.from(req.body, 'hex'), (err, uncompressed) => resolve(uncompressed))
                }
            )
            const request = JSON.parse(requestJson)
            db.queries.push(request.sqlText)
            return res(ctx.json({
                "data": {
                    "getResultUrl": `/queries/${queryId}/result`,
                },
                "code": "333334",
                "message": null,
                "success": true
            }))
        }),
        rest.get(`${baseUri}/queries/:queryId/result`, (req, res, ctx) => {
            return res(ctx.json({
                "data": {
                    "parameters": [],
                    "rowtype": [],
                    "rowset": [],
                    "total": 0,
                    "returned": 0,
                    "queryId": "query-id",
                    "queryResultFormat": "json"
                },
                "code": null,
                "message": null,
                "success": true
            }))
        }),
        // Finally we need to invalidate the authn token by calling logout
        rest.post(`${baseUri}/session/logout-request`, (req, res, ctx) => {
            return res(ctx.status(200))
        }),
    )

    return db;
}
