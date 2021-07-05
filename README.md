# SnowFlake Export Plugin

Sends events to a SnowFlake instance on ingestion.

## Installation Instructions

This plugin uses an external S3 stage to stage files that are copied into Snowflake every hour.

This requires you to have an S3 bucket and an AWS user with the required permissions to access that bucket to use this plugin, in addition to your Snowflake credentials.

### AWS Configuration

1. Create a new S3 bucket **in the same AWS region** as your Snowflake instance
2. Follow [this Snowflake guide](https://docs.snowflake.com/en/user-guide/data-load-s3-config-aws-iam-user.html) to configure AWS IAM User Credentials to Access Amazon S3. However, instead of doing step 3 yourself, input the AWS Key ID and Secret Key in the appropriate plugin configuration options. We'll take care of creating the stage for you.


### Snowflake Configuration

1. [Create a new user in Snowflake](https://docs.snowflake.com/en/sql-reference/sql/create-user.html) that this plugin can use. Make sure it has appropriate permissions to create stages, as well as create, modify, and copy into a table in your desired database.
2. Make sure you have a [Snowflake Warehouse](https://docs.snowflake.com/en/user-guide/warehouses-overview.html) set up. Warehouses are needed to perform `COPY INTO` statements. We recommend a warehouse that will start up automatically when needed.
3. Fill in the configuration options with the user you just created, passing its username and password. For the Snowflake account ID, this can be found in your Snowflake URL. For example, if your URL is: `https://xxx11111.us-east-1.snowflakecomputing.com/`, your account ID will be `xxx11111.us-east-1`. You may also pass in the Cloud provider if that does not work e.g. `xxx11111.us-east-1.aws`.
## Questions?

### [Join the PostHog Users Slack community.](https://posthog.com/slack)
