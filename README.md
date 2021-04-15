# SnowFlake Export Plugin

Sends events to a SnowFlake on ingestion.

## Installation

1. Visit 'Plugins' in PostHog.
1. Find this plugin from the repository or install `https://github.com/PostHog/snowflake-export-plugin`
1. Configure the plugin by entering your database credentials and details.
1. Watch events roll into SnowFlake.

## Temporary Tables

Because Snowflake has a [20 concurrent write statement limit](https://community.snowflake.com/s/article/Your-statement-was-aborted-because-the-number-of-waiters-for-this-lock-exceeds-the-20-statements-limit) per table,
we were forced to implement a workaround which creates temporary tables (one table per worker thread), uploads events to those tables
and then periodically merges them back into the main export table.

You can choose if this merging takes place every minute or every hour.

Your database might contain tables such as these:

```bash
# the table all exports end up in, specified in the plugin configuration
POSTHOG_EXPORT

# these tables will be merged every minute, skipping the last 2 minutes
POSTHOG_EXPORT__BUFFER_20210414-2154M_2ce661b56754db70
POSTHOG_EXPORT__BUFFER_20210414-2155M_35b6643368b6fde5
POSTHOG_EXPORT__BUFFER_20210414-2153M_f5a7490365c98be4

# these tables will be merged every hour, after the 10th minute of the hour
POSTHOG_EXPORT__BUFFER_20210414-2100H_ee391940f27fe64a
POSTHOG_EXPORT__BUFFER_20210414-2100H_1d8b6717e83d8a27
POSTHOG_EXPORT__BUFFER_20210414-2100H_ee47c16c7bffffa4
```

## Authentication

This plugin currently supports only user/pass authentication.

## Questions?

### [Join the PostHog Users Slack community.](https://posthog.com/slack)
