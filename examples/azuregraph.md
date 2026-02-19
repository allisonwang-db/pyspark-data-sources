# Azure Graph API (Microsoft Graph) Data Source Example

Read from Microsoft Graph API (Azure Entra ID, M365) with support for multiple endpoints: users, groups, organization, me, me/memberOf, and more. Uses OAuth2 bearer token.

## Setup Credentials

You need an Azure AD (Entra ID) app registration and an OAuth2 access token for `https://graph.microsoft.com`.

### Option 1: Azure CLI (quick local testing)

```bash
az login
az account get-access-token --resource https://graph.microsoft.com --query accessToken -o tsv
```

### Option 2: App registration (service principal)

1. Register an app in [Azure Portal](https://portal.azure.com) → Azure Active Directory → App registrations.
2. Create a client secret.
3. Grant API permissions (e.g. `User.Read.All`, `Group.Read.All`).
4. Use MSAL or similar to obtain a token via client credentials flow.

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
import os
from pyspark.sql import SparkSession
from pyspark_datasources import AzureGraphDataSource

spark = SparkSession.builder.appName("azuregraph-example").getOrCreate()
spark.dataSource.register(AzureGraphDataSource)

# Token from env or Azure CLI output
token = os.environ.get("AZURE_GRAPH_TOKEN")  # or your token variable
```

### Step 2: Read Users (default endpoint)

```python
df = (
    spark.read.format("azuregraph")
    .option("token", token)
    .option("path", "users")
    .load()
)
df.select("id", "displayName", "mail", "userPrincipalName", "jobTitle").show(10)
```

### Step 3: Read Groups

```python
df_groups = (
    spark.read.format("azuregraph")
    .option("token", token)
    .option("path", "groups")
    .load()
)
df_groups.select("id", "displayName", "description", "mail").show(10)
```

### Step 4: OData Options ($top, $select, $filter)

```python
df_filtered = (
    spark.read.format("azuregraph")
    .option("token", token)
    .option("path", "users")
    .option("top", "50")
    .option("select", "id,displayName,mail,jobTitle")
    .option("filter", "startswith(displayName,'A')")
    .load()
)
df_filtered.show()
```

### Step 5: Other Endpoints

```python
# Organization
df_org = spark.read.format("azuregraph").option("token", token).option("path", "organization").load()

# Current user's groups (requires delegated token for /me)
df_member_of = spark.read.format("azuregraph").option("token", token).option("path", "me/memberOf").load()
```

### Example Output

```
+----------------------------------+------------------+------------------------+---------------------------+
|id                                |displayName       |mail                    |userPrincipalName          |
+----------------------------------+------------------+------------------------+---------------------------+
|6ea91a8d-e32e-41a1-b7bd-d2d185... |Conf Room Adams   |Adams@contoso.com       |Adams@contoso.com         |
|4562bcc8-c436-4f95-b7c0-4f8ce8... |MOD Administrator |null                    |admin@contoso.com        |
+----------------------------------+------------------+------------------------+---------------------------+
```
