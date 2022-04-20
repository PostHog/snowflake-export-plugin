
import snowflakePlugin from "./index"
import AWS, { S3 } from 'aws-sdk'
import Redis from 'ioredis'
import nock from 'nock'
import { v4 as uuid4 } from "uuid"

jest.useFakeTimers("legacy")

// Redis is required to handle staging(?) of S3 files to be pushed to snowflake
const cache = new Redis()

// Make sure no HTTP requests actually head out into the internet
nock.disableNetConnect()

// Make sure S3 clients can use localstack
nock.enableNetConnect("localhost")

test("handles events", async () => {
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
            warehouse: "string",
            awsAccessKeyId: "string",
            awsSecretAccessKey: "string",
            awsRegion: "string",
            storageIntegrationName: "string",
            role: "string",
            stageToUse: 'S3' as const,
            purgeFromStage: 'Yes' as const,
            bucketPath: "string",
            retryCopyIntoOperations: 'Yes' as const,
            forceCopy: 'Yes' as const,
            debug: 'ON' as const,
        }, 
        jobs: {}, 
        cache: cache, 
        // Cast to any, as otherwise we don't match plugin call signatures
        global: {} as any, 
        storage: {}, 
        geoip: {}
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

    const snowflakeMock = createSnowflakeMock(snowflakeAccount)

    mockSnowflakeLoginApi(snowflakeMock)
    mockSnowflakeCreateTableApi(snowflakeMock)
    mockSnowflakeCreateStageApi(snowflakeMock)
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
}

const mockSnowflakeLoginApi = (snowflakeMock) => {
    snowflakeMock
        .post(/\/session\/v1\/login-request\?requestId=.*&roleName=string/)
        .reply(200, JSON.stringify({
            "data": {
                "token": "token",
            },
            "code": null,
            "message": null,
            "success": true
        }))
}

const mockSnowflakeCreateTableApi = (snowflakeMock) => {
    snowflakeMock
        .post(/\/queries\/v1\/query-request\?requestId=.*/)
        .reply(200, JSON.stringify({
            "data": {
                "getResultUrl": "/queries/create-table-id/result",
            },
            "code": "333334",
            "message": null,
            "success": true
        }))

    snowflakeMock
        .get("/queries/create-table-id/result")
        .reply(200, JSON.stringify({
            "data": {
                "parameters": [],
                "rowtype": [],
                "rowset": [],
                "total": 0,
                "returned": 0,
                "queryId": "create-table-id",
                "queryResultFormat": "json"
            },
            "code": null,
            "message": null,
            "success": true
        }))
}

const mockSnowflakeCreateStageApi = (snowflakeMock) => {
    snowflakeMock
        .post(/\/queries\/v1\/query-request\?requestId=.*/)
        .reply(200, JSON.stringify({
            "data": {
                "getResultUrl": "/queries/create-stage-id/result",
            },
            "code": "333334",
            "message": null,
            "success": true
        }))

    snowflakeMock
        .get("/queries/create-stage-id/result")
        .reply(200, JSON.stringify({
            "data": {
                "parameters": [],
                "rowtype": [],
                "rowset": [],
                "total": 0,
                "returned": 0,
                "queryId": "create-stage-id",
                "queryResultFormat": "json"
            },
            "code": null,
            "message": null,
            "success": true
        }))
}