# Azure Naming Conventions Guide (Unguided Capstone – TMDB + Discogs)

## 🔖 General Principles

- **Lowercase only**: no spaces or underscores.
- **Prefix by service type**: `kv-`, `st-`, `dbw-`, `adf-`, `log-`, `rg-`.
- **Include project root**: `tmdbdiscogs` for consistency across assets.
- **Optional environment suffix**: `-dev`, `-test`, `-prod` for lifecycle stages.
- **Max 24 characters** total for most Azure resources.

------

## 📦 Recommended Resource Names

| Resource                       | Purpose                             | Recommended Name      | Notes                                           |
| ------------------------------ | ----------------------------------- | --------------------- | ----------------------------------------------- |
| **Storage Account**            | Data Lake housing raw/silver/gold   | `sttmdbdiscogsdl01`   | Prefix `st` = Storage; suffix `dl` = data lake. |
| **Container (within storage)** | Raw tier                            | `raw`                 | Keep containers lowercase, no hyphens.          |
|                                | Silver tier                         | `silver`              |                                                 |
|                                | Gold tier                           | `gold`                |                                                 |
| **Databricks Workspace**       | Spark processing workspace          | `dbw-tmdbdiscogs-dev` | Prefix `dbw`; include env suffix.               |
| **Resource Group**             | Logical grouping                    | `rg-tmdbdiscogs-dev`  | Prefix `rg`; aligns with other resources.       |
| **Key Vault**                  | Secrets & API keys                  | `kv-tmdbdiscogs-dev`  | Prefix `kv`.                                    |
| **Data Factory**               | Pipeline orchestration              | `adf-tmdbdiscogs-dev` | Prefix `adf`.                                   |
| **Log Analytics Workspace**    | Monitoring & diagnostics            | `log-tmdbdiscogs-dev` | Prefix `log`.                                   |
| **Managed Identity**           | Service identity for Databricks/ADF | `id-tmdbdiscogs-dev`  | Optional naming, used in RBAC.                  |

------

## ⚙️ Template Parameter Patterns

Define resource names dynamically using ARM parameters:

```json
"parameters": {
  "resourcePrefix": { "type": "string", "defaultValue": "tmdbdiscogs" },
  "environment": { "type": "string", "defaultValue": "dev" }
}
```

Then concatenate:

```json
"name": "[concat('kv-', parameters('resourcePrefix'), '-', parameters('environment'))]"
```

------

## 🖱️ Folder Layout Reference

```
/infrastructure/
   ├── storage_template.json
   ├── databricks_template.json
   ├── keyvault_template.json
   ├── adf_template.json
   ├── monitoring_template.json
   └── naming_conventions.md   ← (this file)
```