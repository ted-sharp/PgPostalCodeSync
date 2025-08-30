# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Build and Run
```bash
# Build the project
dotnet build

# Run the application  
dotnet run

# Publish for Windows
dotnet publish -c Release -r win-x64 --self-contained

# Publish for Linux
dotnet publish -c Release -r linux-x64 --self-contained
```

### Project Structure
- Solution file: `src/PgPostalCodeSync.sln`
- Main project: `src/PgPostalCodeSync/PgPostalCodeSync.csproj`
- Entry point: `src/PgPostalCodeSync/Program.cs`

## Architecture Overview

This is a PostgreSQL postal code synchronization console application that downloads Japanese postal code data from Japan Post and imports it into a PostgreSQL database.

### Core Components (Planned Architecture)

The application follows this high-level architecture:

1. **CLI Layer** - Handles command-line arguments (`--full`, `--yymm=YYMM`, `--workdir`)
2. **Download Layer** - Downloads UTF-8 ZIP files from Japan Post website
3. **Import Layer** - Uses PostgreSQL `COPY FROM STDIN` for efficient data loading
4. **Processing Layer** - Handles differential updates (upsert/delete) or full replacement

### Key Design Patterns

- **Differential vs Full Mode**: 
  - Differential: Downloads monthly add/delete files and applies changes
  - Full: Downloads complete dataset and performs atomic table swap via RENAME
- **Atomic Switching**: Full imports use temporary tables + RENAME for zero-downtime updates
- **PostgreSQL Optimization**: Uses `COPY FROM STDIN` and specialized indexing strategies

### Database Schema

Located in `sql/pg_setup/`:
- `ext.postal_codes` - Main production table
- `ext.postal_codes_landed` - Temporary staging table for imports
- `ext.ingestion_runs` - Execution metadata and history
- `ext.ingestion_files` - File metadata (SHA-256, size, etc.)

### Data Sources

- **Full dataset**: `utf_ken_all.zip`
- **Monthly additions**: `utf_add_YYMM.zip`
- **Monthly deletions**: `utf_del_YYMM.zip`
- **Base URL**: https://www.post.japanpost.jp/zipcode/utf/zip/

### Configuration

The application uses `appsettings.json` for:
- Database connection strings
- Work directory paths
- Download URL patterns
- Serilog configuration
- Cleanup policies

### Key Implementation Details

- Uses .NET 9 / C# 13
- Composite logical key: `(postal_code, prefecture, city, town)`
- Non-transactional differential processing for performance
- PostgreSQL MERGE statements for upsert operations
- Comprehensive error handling with JSONB error storage
- Asynchronous Serilog with file rotation

### Work Directory Structure
```
WorkDir/
├── downloads/     # ZIP file storage
├── extracted/     # Extracted CSV files  
└── logs/         # Log files
```

This application is designed for monthly execution via external schedulers and prioritizes PostgreSQL-specific optimizations for high-performance data loading.