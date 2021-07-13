# Snowflake Export Plugin

Export events to a Snowflake table regularly.

## How This Works

This plugin uses a Snowflake external stage to stage events in object storage - Amazon S3 or Google Cloud Storage. Staged events (stored in object storage as files containing event batches) are then copied into the final destination – your Snowflake table – once an hour.

This requires you to have an S3/GCS bucket and an AWS/GCP user with permissions to access that bucket in order to use this plugin, in addition to your Snowflake credentials.

### Amazon S3 Configuration

1. Create a new S3 bucket, preferably in the same AWS region as your Snowflake instance.
2. Follow [this Snowflake guide on S3](https://docs.snowflake.com/en/user-guide/data-load-s3-config-aws-iam-user.html) to configure AWS IAM User Credentials to Access Amazon S3. However, instead of doing step 3 yourself, input the AWS Key ID and Secret Key in the appropriate plugin configuration options. We'll take care of creating the stage for you.

### Google Cloud Storage Configuration

1. Create a new GCS bucket.
2. Follow [this Snowflake guide on GCS](https://docs.snowflake.com/en/user-guide/data-load-gcs-config.html) to create a storage integration and generate a user for Snowflake to use when accessing your bucket. Make sure not to skip over any part of the guide!
3. Download the service accoun credentials JSON file and upload it in the configuration step of this plugin.

### Snowflake Configuration

1. [Create a new user in Snowflake](https://docs.snowflake.com/en/sql-reference/sql/create-user.html) that this plugin can use. Make sure it has appropriate permissions to create stages, as well as create, modify, and copy into a table in your desired database.
2. Make sure you have a [Snowflake Warehouse](https://docs.snowflake.com/en/user-guide/warehouses-overview.html) set up. Warehouses are needed to perform `COPY INTO` statements. We recommend a warehouse that will start up automatically when needed.
3. Fill in the configuration options with the user you just created, passing its username and password. For the Snowflake account ID, this can be found in your Snowflake URL. For example, if your URL is: `https://xxx11111.us-east-1.snowflakecomputing.com/`, your account ID will be `xxx11111.us-east-1`. You may also pass in the Cloud provider if that does not work e.g. `xxx11111.us-east-1.aws`.
4. **GCS-only.** Make sure the user available to the plugin has permissions on the storage integration you created at Step 2 of the GCS configuration instructions. You can do this like so:
    ```sql
    GRANT USAGE ON INTEGRATION <your_gcs_integration_name> TO ROLE <plugin_user_role>
    ```

## Questions?

### [Join the PostHog Users Slack community.](https://posthog.com/slack)
