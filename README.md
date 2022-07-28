# Snowflake Export Plugin

Export events to a Snowflake table regularly.

## What This Does

This plugin uses a Snowflake external stage to stage events in object storage - Amazon S3 or Google Cloud Storage. Staged events (stored in object storage as files containing event batches) are then copied into the final destination – your Snowflake table – once every 10 minutes (or a cadence of your choosing).

Prerequisites:

-   a Snowflake user
-   an S3 _or_ GCS bucket
-   an AWS _or_ GCP user with permissions to access that bucket in order to use this plugin

## How to Set This Up

### Snowflake Configuration

1. [Create a new user in Snowflake](https://docs.snowflake.com/en/sql-reference/sql/create-user.html) that this plugin can use.
    > ⚠️ Make sure it has appropriate permissions to **create stages**, as well as **create, modify, and copy into a table** in your desired database.
2. Make sure you have a [Snowflake Warehouse](https://docs.snowflake.com/en/user-guide/warehouses-overview.html) set up. Warehouses are needed to perform `COPY INTO` statements. We recommend a warehouse that will start up automatically when needed.
3. Fill in the configuration options with the user you just created, passing its username and password. For the Snowflake account ID, this can be found in your Snowflake URL. For example, if your URL is: `https://xxx11111.us-east-1.snowflakecomputing.com/`, your account ID will be `xxx11111.us-east-1`. You may also pass in the Cloud provider if that does not work e.g. `xxx11111.us-east-1.aws`.
4. **GCS-only.** Make sure the user available to the plugin has permissions on the storage integration you created at Step 2 of the GCS configuration instructions. You can do this like so:
    ```sql
    GRANT USAGE ON INTEGRATION <your_gcs_integration_name> TO ROLE <plugin_user_role>
    ```

If you're exporting from PostHog Cloud, do **NOT set any IP whitelist/blacklist** or other network policies. PostHog Cloud operates on a decentralized network of computing resources and therefore the IPs could change at any time.

### Amazon S3 Configuration

1. Create a new S3 bucket, preferably in the same AWS region as your Snowflake instance.
2. Follow [this Snowflake guide on S3](https://docs.snowflake.com/en/user-guide/data-load-s3-config-aws-iam-user.html) to configure AWS IAM User Credentials to Access Amazon S3. However, instead of doing step 3 yourself, input the AWS Key ID and Secret Key in the appropriate plugin configuration options. We'll take care of creating the stage for you.

### Google Cloud Storage Configuration

1. Create a new GCS bucket.
2. Follow [this Snowflake guide on GCS](https://docs.snowflake.com/en/user-guide/data-load-gcs-config.html) to create a storage integration and generate a user for Snowflake to use when accessing your bucket. Make sure not to skip over any part of the guide!
3. Download the service account credentials JSON file and upload it in the configuration step of this plugin.

## Troubleshooting

If you're running into connection issues please verify your login credentials, make sure **all** the permissions listed above have been granted to your user, and that you do not have any IP whitelist/blacklist policy (if exporting from PostHog Cloud).

## Development tips

You do not need a functional plugin server to start hacking on this plugin. The
tests provide local versions of the plugins dependencies so you can avoid the
setup cost of otherwise incurred. The plugin depends on:

1.  S3 for storing files somewhere that Snowflake can import from. For this we
    use localstack to simulate (we could have equally used minio)
1.  a cache for keeping track of files that need to be uploaded to Snowflake.
    For this we use redis.
1.  Snowflake REST API to which we issue commands to COPY the S3 files from
    above. For this we use a simple Express.js like mock server, with MSW
    providing the HTTP interception.

To start these stand-ins run:

```bash
docker-compose up
```

To run the tests run:

```
yarn test
```

We use Jest for tests, so you can use all the associated options.

Tests also run within GitHub Actions on PRs.

## Questions?

### [Join the PostHog Users Slack community.](https://posthog.com/slack)
