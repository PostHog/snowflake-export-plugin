
import snowflakePlugin from "./index"
import AWS, { S3 } from 'aws-sdk'
import Redis from 'ioredis'
import nock from 'nock'
import { v4 as uuid4 } from "uuid"

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
    get: (key: string, defaultValue: unknown) => redis.get(key)
} as any

// Make sure no HTTP requests actually head out into the internet
nock.disableNetConnect()

// Make sure S3 clients can use localstack
nock.enableNetConnect("localhost")

test("handles events", async () => {
    // Checks for the happy path
    //
    // TODO: check for:
    //
    //  1. snowflake retry functionality
    //  2. s3 failure cases

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
    return nock(`https://${accountName}.snowflakecomputing.com`)
        .defaultReplyHeaders({
            'Content-Type': 'application/json',
        })
        .post(/\/session\/v1\/login-request\?requestId=.*&roleName=role/)
        .reply(200, JSON.stringify({
            "data": {
                "token": "token",
            },
            "code": null,
            "message": null,
            "success": true
        }))
        .post(/\/queries\/v1\/query-request\?requestId=.*/)
        .reply(200, JSON.stringify({
            "data": {
                "getResultUrl": "/queries/query-id/result",
            },
            "code": "333334",
            "message": null,
            "success": true
        }))
        .get("/queries/query-id/result")
        .reply(200, JSON.stringify({
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
        .post("/session/logout-request")
        .reply(200, JSON.stringify({
            "data": {
                "token": "token",
            },
            "code": null,
            "message": null,
            "success": true
        }))
}