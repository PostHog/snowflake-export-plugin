
import snowflakePlugin from "./index"
import AWS, { S3 } from 'aws-sdk'
import Redis from 'ioredis'
import { v4 as uuid4 } from "uuid"
import { setupServer } from "msw/node"
import { rest } from "msw"

// Use legacy fake timers. With modern timers there seems to be little feedback
// on fails due to test timeouts.
jest.useFakeTimers("legacy")

// Redis is required to handle staging(?) of S3 files to be pushed to snowflake
const redis = new Redis()

// Create something that looks like the expected cache interface. Note it only
// differs by the addition of the `defaultValue` argument.
const cache = {
    ...redis,
    lpush: redis.lpush.bind(redis),
    llen: redis.llen.bind(redis),
    lrange: redis.lrange.bind(redis),
    set: redis.set.bind(redis),
    expire: redis.expire.bind(redis),
    get: (key: string, defaultValue: unknown) => redis.get(key)
} as any

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
        jobs: {},
        cache: cache,
        // Cast to any, as otherwise we don't match plugin call signatures
        global: {} as any,
        storage: {} as any,
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
            distinct_id: "123",
            ip: "10.10.10.10",
            site_url: "https://app.posthog.com",
            team_id: 1,
            now: "2020-01-01T01:01:01Z"
        }
    ]

    const snowflakeMock = createSnowflakeMock(
        snowflakeAccount,
    )

    await snowflakePlugin.setupPlugin?.(meta)
    await snowflakePlugin.exportEvents?.(events, meta)
    await snowflakePlugin.runEveryMinute?.(meta)
    await snowflakePlugin.teardownPlugin?.(meta)

    // TODO: assert:
    //  
    //  1. snowflake called
    //  2. S3 bucket has the right events
})

const createSnowflakeMock = (accountName: string) => {
    // Create something that kind of looks like snowflake, albeit not
    // functional.
    const baseUri = `https://${accountName}.snowflakecomputing.com`

    const mock = setupServer(
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
        rest.post(`${baseUri}/queries/v1/query-request`, (req, res, ctx) => {
            const queryId = uuid4()
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

    mock.listen()
}
