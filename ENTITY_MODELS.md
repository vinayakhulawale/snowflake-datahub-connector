# DataHub Entity Models for Platform Users and Groups

This connector uses custom DataHub entity types `PlatformUser` and `PlatformUserGroup` instead of the standard `corpuser` and `corpGroup` entities. This design allows for better support of multiple data platforms like Snowflake, Databricks, BigQuery, etc.

## PlatformUser Entity

### Entity Type
- **Entity Type**: `platformUser`
- **Aspect Name**: `platformUserInfo`
- **URN Format**: `urn:li:platformUser:(urn:li:dataPlatform:{platform},{username})`

### Properties
```json
{
  "platform": "snowflake",           // Data platform (snowflake, databricks, bigquery, etc.)
  "username": "john.doe",            // Platform-specific username
  "email": "john.doe@company.com",   // User's email address
  "displayName": "John Doe",         // User's display name
  "active": true,                    // Whether the user is active
  "roles": ["ANALYST", "DBA"],       // List of assigned roles/groups
  "createdAt": "2023-01-15T10:30:00Z", // User creation timestamp (ISO format)
  "lastLogin": "2024-12-15T09:45:00Z", // Last login timestamp (ISO format)
  "customProperties": {              // Additional platform-specific properties
    "department": "Data Engineering",
    "manager": "jane.smith"
  }
}
```

### Example URN
```
urn:li:platformUser:(urn:li:dataPlatform:snowflake,john.doe)
```

## PlatformUserGroup Entity

### Entity Type
- **Entity Type**: `platformUserGroup`
- **Aspect Name**: `platformUserGroupInfo`
- **URN Format**: `urn:li:platformUserGroup:(urn:li:dataPlatform:{platform},{groupName})`

### Properties
```json
{
  "platform": "snowflake",           // Data platform (snowflake, databricks, bigquery, etc.)
  "groupName": "DATA_ANALYSTS",      // Platform-specific group/role name
  "displayName": "Data Analysts",    // Human-readable group name
  "description": "Analytics team with read access to data", // Group description
  "active": true,                    // Whether the group is active
  "members": ["john.doe", "jane.smith"], // List of group members
  "owner": "admin.user",             // Group owner/creator
  "createdAt": "2023-01-10T14:20:00Z", // Group creation timestamp (ISO format)
  "customProperties": {              // Additional platform-specific properties
    "permissions": "READ,WRITE",
    "cost_center": "Engineering"
  }
}
```

### Example URN
```
urn:li:platformUserGroup:(urn:li:dataPlatform:snowflake,DATA_ANALYSTS)
```

## Platform Support

The connector is designed to support multiple data platforms by changing the `platform` configuration:

### Snowflake
```json
{
  "platform": "snowflake"
}
```

### Databricks (future support)
```json
{
  "platform": "databricks"
}
```

### BigQuery (future support)
```json
{
  "platform": "bigquery"
}
```

## Benefits of Custom Entity Types

1. **Platform Isolation**: Users and groups from different platforms are kept separate in DataHub
2. **Platform-Specific Properties**: Each platform can have unique attributes without conflicts
3. **Clear Lineage**: Easy to trace which platform a user or group belongs to
4. **Extensibility**: Simple to add new platforms without changing existing data
5. **Namespace Management**: Prevents username conflicts between platforms

## Migration from Standard Entities

If you were previously using `corpuser` and `corpGroup` entities, you can migrate by:

1. Updating your DataHub schema to support the new entity types
2. Running a data migration script to transform existing entities
3. Updating any downstream applications to use the new URN format

## Example Integration

```python
from datahub_client import DataHubClient
from models import UserMetadata, GroupMetadata

# Initialize client with platform
client = DataHubClient(config, platform="snowflake")

# Ingest user
user = UserMetadata(
    name="john.doe",
    email="john.doe@company.com",
    display_name="John Doe",
    roles=["ANALYST", "DBA"]
)
client.ingest_user(user)

# Ingest group
group = GroupMetadata(
    name="DATA_ANALYSTS",
    description="Analytics team",
    members=["john.doe", "jane.smith"]
)
client.ingest_group(group)
```

This creates entities with URNs:
- `urn:li:platformUser:(urn:li:dataPlatform:snowflake,john.doe)`
- `urn:li:platformUserGroup:(urn:li:dataPlatform:snowflake,DATA_ANALYSTS)`