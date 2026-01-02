# GitLab Stat Fetcher

A high-performance Go tool to fetch and export GitLab project statistics (commits, merge requests, and discussions) to CSV format.

## Features

- Fetches commits with detailed statistics (additions, deletions, author info)
- Retrieves merge requests with metadata (state, branches, authors)
- Extracts discussion notes and comments
- Concurrent processing with configurable worker pools
- Date filtering to fetch only recent data
- Supports both individual projects and entire groups (with recursive subgroup scanning)
- Tracks already-fetched projects to avoid duplicates
- Exports data to CSV files for easy analysis

## Prerequisites

- Go 1.25.5 or higher
- GitLab personal access token with API read access

## Installation

```bash
git clone https://github.com/AlexeyBelezeko/GitlabStatFetcher.git
cd gitlab-stat-fetcher
go mod download
go build -o gitlab-stat-fetcher
```

## Configuration

Create a `.env` file based on `.env.example`:

```env
GITLAB_URL=https://gitlab.com
GITLAB_TOKEN=your-personal-access-token
WORKERS=20
DATA_FOLDER=data
SINCE_DATE=2025-01-01
```

### Environment Variables

- `GITLAB_URL` (required): Your GitLab instance URL
- `GITLAB_TOKEN` (required): GitLab personal access token
- `WORKERS` (optional): Number of concurrent workers (default: 1)
- `DATA_FOLDER` (required): Output directory for CSV files
- `SINCE_DATE` (optional): Fetch data from this date onwards in YYYY-MM-DD format (default: 2 years ago)

## Usage

Run the tool by providing one or more GitLab project or group URLs:

```bash
./gitlab-stat-fetcher https://gitlab.com/group/project
```

Multiple URLs can be provided:

```bash
./gitlab-stat-fetcher https://gitlab.com/group1 https://gitlab.com/group2/project
```

## Output

The tool generates CSV files in the specified `DATA_FOLDER`:

- `commits.csv` - Commit history with statistics
- `merge_requests.csv` - Merge request details
- `notes.csv` - Discussion notes and comments

A hidden `.fetched_projects` file tracks processed projects to prevent re-fetching.

## Performance

- Uses concurrent workers for parallel API requests
- Optimized for large repositories and groups
- Progress tracking for long-running operations
- Target: Complete fetch within 5 minutes (performance indicator displayed)

## License

MIT License - See [LICENSE](LICENSE) file for details

## Author

Copyright (c) 2026 Aleksei