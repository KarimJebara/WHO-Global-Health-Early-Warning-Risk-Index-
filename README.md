
# Global Health Early-Warning Risk Index (WHO)

## Snowflake setup

This project can optionally load WHO indicator data into Snowflake during ingestion.

1. Create a local .env file at the project root with your Snowflake credentials:

```env
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
