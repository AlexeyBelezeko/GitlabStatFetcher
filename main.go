package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/joho/godotenv"
	gitlab "gitlab.com/gitlab-org/api/client-go"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	// Parse date filter (default: 2 years ago)
	sinceDate := gitlab.Ptr(time.Now().AddDate(-2, 0, 0))
	if sinceDateEnv := os.Getenv("SINCE_DATE"); sinceDateEnv != "" {
		parsedDate, err := time.Parse("2006-01-02", sinceDateEnv)
		if err != nil {
			log.Printf("Warning: Invalid SINCE_DATE format (use YYYY-MM-DD), using default (2 years ago)")
		} else {
			sinceDate = gitlab.Ptr(parsedDate)
		}
	}

	log.Printf("Fetching data since: %s", sinceDate.Format("2006-01-02"))

	gitlabBaseURL := os.Getenv("GITLAB_URL")
	token := os.Getenv("GITLAB_TOKEN")

	if gitlabBaseURL == "" || token == "" {
		log.Fatal("Missing required environment variables: GITLAB_URL, GITLAB_TOKEN")
	}

	workersCount := 1
	if workersCountEnv := os.Getenv("GITLAB_WORKERS"); workersCountEnv != "" {
		workersCount, err = strconv.Atoi(workersCountEnv)
	}

	dataFolder := os.Getenv("DATA_FOLDER")
	if dataFolder == "" {
		log.Fatal("Missing required environment variables: DATA_FOLDER")
	}

	if err := os.MkdirAll(dataFolder, 0755); err != nil {
		log.Fatalf("Error creating data directory: %v", err)
	}

	// Load already fetched projects
	fetchedProjects := loadFetchedProjects(dataFolder)
	log.Printf("Found %d already fetched projects", len(fetchedProjects))

	skipDiscussions := flag.Bool("skip-discussions", false, "Skip fetching merge request discussions")
	flag.Parse()
	urls := flag.Args()
	if len(urls) == 0 {
		log.Fatal("Error: At least one project or group URL is required\n\nUsage: gitlab-stat-fetcher <url> [url...]")
	}

	client, err := gitlab.NewClient(token, gitlab.WithBaseURL(gitlabBaseURL))
	if err != nil {
		log.Fatal(err)
	}

	var allProjects []*gitlab.Project
	for _, url := range urls {
		path, err := extractPath(url, gitlabBaseURL)
		if err != nil {
			log.Fatal(err)
		}
		projects, err := recursivelyFetchProjectFromPath(client, path)
		if err != nil {
			log.Fatal(err)
		}
		allProjects = append(allProjects, projects...)
	}

	projectsToFetch := make([]*gitlab.Project, 0)
	for _, project := range allProjects {
		if !fetchedProjects[project.ID] {
			projectsToFetch = append(projectsToFetch, project)
		}
	}

	for _, project := range projectsToFetch {
		fmt.Println("\n" + strings.Repeat("=", 80))
		fmt.Printf("Fetching: %s (ID: %d)\n", project.PathWithNamespace, project.ID)
		fmt.Println(strings.Repeat("=", 80))

		fetchAll(client, project.ID, workersCount, dataFolder, sinceDate, *skipDiscussions)
		markProjectFetched(dataFolder, project.ID, project.PathWithNamespace)
	}

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("ALL PROJECTS COMPLETED!")
	fmt.Println(strings.Repeat("=", 80))
}

// recursivelyFetchProjectFromURL returns list of projects from group path and all subgroups. If path is a project
// them returns one project.
func recursivelyFetchProjectFromPath(client *gitlab.Client, path string) ([]*gitlab.Project, error) {
	project, _, err := client.Projects.GetProject(path, nil)
	if err == nil {
		return []*gitlab.Project{project}, err
	}

	group, _, err := client.Groups.GetGroup(path, nil)
	if err != nil {
		return nil, fmt.Errorf("error fetching group: %v", err)
	}

	listSubgroupsOpts := &gitlab.ListSubGroupsOptions{
		ListOptions: gitlab.ListOptions{
			PerPage: 100,
			Page:    1,
		},
	}

	var allProjects []*gitlab.Project
	for {
		groups, resp, err := client.Groups.ListSubGroups(group.ID, listSubgroupsOpts)
		if err != nil {
			return nil, fmt.Errorf("error fetching list subgroups: %v", err)
		}

		for _, subgroup := range groups {
			subgroupProjects, err := listGroupProjects(client, subgroup.ID)
			if err != nil {
				return nil, fmt.Errorf("error fetching list subgroup projects: %v", err)
			}
			allProjects = append(allProjects, subgroupProjects...)
		}

		if resp.CurrentPage >= resp.TotalPages {
			break
		}
		listSubgroupsOpts.Page = resp.NextPage
	}

	groupProjects, err := listGroupProjects(client, group.ID)
	if err != nil {
		return nil, fmt.Errorf("error fetching list group projects: %v", err)
	}
	allProjects = append(allProjects, groupProjects...)

	return allProjects, nil
}

func listGroupProjects(client *gitlab.Client, groupID int64) ([]*gitlab.Project, error) {
	listProjectsOpts := &gitlab.ListGroupProjectsOptions{
		ListOptions: gitlab.ListOptions{
			PerPage: 100,
			Page:    1,
		},
	}
	var allProjects []*gitlab.Project
	for {
		projects, resp, err := client.Groups.ListGroupProjects(groupID, listProjectsOpts)
		if err != nil {
			return nil, fmt.Errorf("error fetching list projects: %v", err)
		}

		allProjects = append(allProjects, projects...)
		if resp.NextPage == 0 {
			break
		}
		listProjectsOpts.Page = resp.NextPage
	}
	return allProjects, nil
}

// extractPath extracts the path from a GitLab URL (works for both projects and groups)
func extractPath(inputURL, baseURL string) (string, error) {
	parsedURL, err := url.Parse(inputURL)
	if err != nil {
		return "", fmt.Errorf("invalid URL: %v", err)
	}
	parsedBase, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("invalid base URL: %v", err)
	}
	if parsedURL.Host != parsedBase.Host {
		return "", fmt.Errorf("URL host %s does not match GitLab base URL %s", parsedURL.Host, parsedBase.Host)
	}
	path := strings.TrimPrefix(parsedURL.Path, "/")
	return path, nil
}

// loadFetchedProjects reads the list of already fetched project IDs
func loadFetchedProjects(dataDir string) map[int64]bool {
	fetched := make(map[int64]bool)
	indexFile := fmt.Sprintf("%s/.fetched_projects", dataDir)

	file, err := os.Open(indexFile)
	if err != nil {
		return fetched
	}
	defer file.Close()

	var projectID int64
	var projectPath string
	for {
		_, err := fmt.Fscanf(file, "%d %s\n", &projectID, &projectPath)
		if err != nil {
			break
		}
		fetched[projectID] = true
	}

	return fetched
}

// markProjectFetched adds a project to the fetched projects index
func markProjectFetched(dataDir string, projectID int64, projectPath string) {
	indexFile := fmt.Sprintf("%s/.fetched_projects", dataDir)

	file, err := os.OpenFile(indexFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Error marking project as fetched: %v", err)
		return
	}
	defer file.Close()

	_, err = fmt.Fprintf(file, "%d %s\n", projectID, projectPath)
	if err != nil {
		log.Fatalf("Error marking project as fetched: %v", err)
	}
}

// sanitizeFilename creates a safe filename from project name
func sanitizeFilename(name string) string {
	// Replace problematic characters
	replacer := strings.NewReplacer(
		"/", "_",
		"\\", "_",
		":", "_",
		"*", "_",
		"?", "_",
		"\"", "_",
		"<", "_",
		">", "_",
		"|", "_",
		" ", "_",
	)
	return replacer.Replace(name)
}

// fetchAll fetches all per user stats.
func fetchAll(client *gitlab.Client, projectID int64, workers int, dataDir string, sinceDate *time.Time, skipDiscussions bool) {
	startTime := time.Now()

	commits := fetchCommits(client, projectID, workers, sinceDate)
	mrs := fetchMRs(client, projectID, workers, sinceDate)

	var notes []*gitlab.Note
	if !skipDiscussions {
		// Extract MR IDs for discussions
		mrIIDs := make([]int64, len(mrs))
		for i, mr := range mrs {
			mrIIDs[i] = mr.IID
		}
		notes = fetchDiscussions(client, projectID, mrIIDs, workers)
	} else {
		log.Println("Skipping discussions (--skip-discussions flag set)")
	}

	log.Println("Writing output files...")

	commitsFile := fmt.Sprintf("%s/commits.csv", dataDir)
	if err := writeCommitsCSV(commits, commitsFile); err != nil {
		log.Printf("Error writing commits: %v", err)
	} else {
		fmt.Printf("✓ Saved: %s\n", commitsFile)
	}

	mrsFile := fmt.Sprintf("%s/merge_requests.csv", dataDir)
	if err := writeMRsCSV(mrs, mrsFile); err != nil {
		log.Printf("Error writing MRs: %v", err)
	} else {
		fmt.Printf("✓ Saved: %s\n", mrsFile)
	}

	if len(notes) > 0 {
		notesFile := fmt.Sprintf("%s/notes.csv", dataDir)
		if err := writeNotesCSV(notes, notesFile); err != nil {
			log.Printf("Error writing notes: %v", err)
		} else {
			fmt.Printf("✓ Saved: %s\n", notesFile)
		}
	}

	// Summary
	totalTime := time.Since(startTime)
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("FETCH COMPLETE!")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("Total time: %v\n", totalTime)
	fmt.Printf("Commits: %d\n", len(commits))
	fmt.Printf("Merge Requests: %d\n", len(mrs))
	fmt.Printf("Notes: %d\n", len(notes))
	fmt.Println(strings.Repeat("=", 60))

	if totalTime > 5*time.Minute {
		fmt.Println("⚠️  Exceeded 5-minute target")
	} else {
		fmt.Println("✓ Completed within 5-minute target")
	}
}

// Write CSV files (append mode for consolidated output)
func writeCommitsCSV(commits []*gitlab.Commit, filename string) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("could not stat file: %v", err)
	}
	hasHeader := stat.Size() > 0

	w := csv.NewWriter(file)
	defer w.Flush()

	if !hasHeader {
		w.Write([]string{"project_id", "id", "author_name", "author_email", "date", "message",
			"additions", "deletions", "total"})
	}

	// Rows
	for _, c := range commits {
		// Clean message: replace newlines with spaces for better CSV compatibility
		cleanMessage := strings.ReplaceAll(c.Message, "\n", " ")
		cleanMessage = strings.ReplaceAll(cleanMessage, "\r", " ")

		w.Write([]string{
			strconv.FormatInt(c.ProjectID, 10),
			c.ID,
			c.AuthorName,
			c.AuthorEmail,
			formatDate(c.CommittedDate),
			cleanMessage,
			strconv.FormatInt(c.Stats.Additions, 10),
			strconv.FormatInt(c.Stats.Deletions, 10),
			strconv.FormatInt(c.Stats.Total, 10),
		})
	}

	return nil
}

func writeMRsCSV(mrs []*gitlab.BasicMergeRequest, filename string) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("could not stat file: %v", err)
	}
	hasHeader := stat.Size() > 0

	w := csv.NewWriter(file)
	defer w.Flush()

	if !hasHeader {
		w.Write([]string{"project_id", "mr_id", "title", "state", "author_username", "author_name",
			"created_at", "merged_at", "source_branch", "target_branch",
			"sha", "merge_commit_sha", "squash_commit_sha"})
	}

	for _, m := range mrs {
		w.Write([]string{
			strconv.FormatInt(m.ProjectID, 10),
			strconv.FormatInt(m.ID, 10),
			m.Title,
			m.State,
			m.Author.Username,
			m.Author.Name,
			formatDate(m.CreatedAt),
			formatDate(m.MergedAt),
			m.SourceBranch,
			m.TargetBranch,
			m.SHA,
			m.MergeCommitSHA,
			m.SquashCommitSHA,
		})
	}

	return nil
}

func writeNotesCSV(notes []*gitlab.Note, filename string) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("could not stat file: %v", err)
	}
	hasHeader := stat.Size() > 0

	w := csv.NewWriter(file)
	defer w.Flush()

	if !hasHeader {
		w.Write([]string{"project_id", "note_id", "author_name", "author_username",
			"created_at", "updated_at", "body", "system"})
	}

	// Rows
	for _, n := range notes {
		// Clean body: replace newlines for better CSV compatibility
		cleanBody := strings.ReplaceAll(n.Body, "\n", " ")
		cleanBody = strings.ReplaceAll(cleanBody, "\r", " ")

		w.Write([]string{
			strconv.FormatInt(n.ProjectID, 10),
			strconv.FormatInt(n.ID, 10),
			n.Author.Name,
			n.Author.Username,
			formatDate(n.CreatedAt),
			formatDate(n.UpdatedAt),
			cleanBody,
			strconv.FormatBool(n.System),
		})
	}

	return nil
}

func fetchCommits(client *gitlab.Client, projectID int64, workers int, sinceData *time.Time) []*gitlab.Commit {
	log.Println("Fetching commits...")
	start := time.Now()

	var (
		nextPage int64 = 1 // shared counter
		stop     int32 = 0 // atomic flag: 0 = keep going, 1 = stop
		results        = make(chan []*gitlab.Commit, workers*2)
		wg       sync.WaitGroup
	)

	// Start workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				// If someone already found the end, stop.
				if atomic.LoadInt32(&stop) == 1 {
					return
				}
				page := atomic.AddInt64(&nextPage, 1) - 1

				commits := fetchCommitPage(client, projectID, page, sinceData)

				// Empty page => no more commits.
				if len(commits) == 0 {
					atomic.StoreInt32(&stop, 1)
					return
				}
				results <- commits
			}
		}()
	}

	// Close results when workers exit
	go func() {
		wg.Wait()
		close(results)
	}()

	var allCommits []*gitlab.Commit
	for commits := range results {
		allCommits = append(allCommits, commits...)
	}

	log.Printf("Fetched %d commits in %v", len(allCommits), time.Since(start))
	return allCommits
}

func fetchCommitPage(client *gitlab.Client, projectID, page int64, sinceDate *time.Time) []*gitlab.Commit {
	opts := &gitlab.ListCommitsOptions{
		WithStats:   gitlab.Ptr(true),
		ListOptions: gitlab.ListOptions{Page: page, PerPage: 100},
		Since:       sinceDate,
	}

	glCommits, _, err := client.Commits.ListCommits(projectID, opts)
	if err != nil {
		log.Printf("Error fetching commits: %v", err)
		return nil
	}
	return glCommits
}

func fetchMRs(client *gitlab.Client, projectID int64, workers int, sinceDate *time.Time) []*gitlab.BasicMergeRequest {
	log.Println("Fetching merge requests...")
	start := time.Now()

	var (
		nextPage int64 = 1 // shared counter
		stop     int32 = 0 // atomic flag: 0 = keep going, 1 = stop
		results        = make(chan []*gitlab.BasicMergeRequest, workers*2)
		wg       sync.WaitGroup
	)

	// Start workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				// If someone already found the end, stop.
				if atomic.LoadInt32(&stop) == 1 {
					return
				}
				page := atomic.AddInt64(&nextPage, 1) - 1

				mrs := fetchMRPage(client, projectID, page, sinceDate)

				if len(mrs) == 0 {
					atomic.StoreInt32(&stop, 1)
					return
				}
				results <- mrs
			}
		}()
	}

	// Close results when workers exit
	go func() {
		wg.Wait()
		close(results)
	}()
	var allMRs []*gitlab.BasicMergeRequest
	for mrs := range results {
		allMRs = append(allMRs, mrs...)
	}

	log.Printf("Fetched %d MRs in %v", len(allMRs), time.Since(start))
	return allMRs
}

func fetchMRPage(client *gitlab.Client, projectID, page int64, sinceDate *time.Time) []*gitlab.BasicMergeRequest {
	opts := &gitlab.ListProjectMergeRequestsOptions{
		State:        gitlab.Ptr("all"),
		ListOptions:  gitlab.ListOptions{Page: page, PerPage: 100},
		CreatedAfter: sinceDate,
	}

	mrs, _, err := client.MergeRequests.ListProjectMergeRequests(projectID, opts)
	if err != nil {
		log.Printf("Error fetching merge requests: %v", err)
		return nil
	}
	return mrs
}

func fetchDiscussions(client *gitlab.Client, projectID int64, mrIIDs []int64, workers int) []*gitlab.Note {
	log.Println("Fetching discussions...")
	start := time.Now()

	var (
		nextIdx int64 = 0
		results       = make(chan []*gitlab.Note, workers*2)

		wg    sync.WaitGroup
		done  int64 // how many MRs processed (for progress)
		total = int64(len(mrIIDs))
	)

	// Start workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				idx := atomic.AddInt64(&nextIdx, 1) - 1
				if idx >= total {
					return
				}

				mrIID := mrIIDs[idx]
				notes := fetchMRDiscussions(client, projectID, mrIID)
				results <- notes

				processed := atomic.AddInt64(&done, 1)
				if processed%100 == 0 {
					log.Printf("Progress: %d/%d MRs", processed, total)
				}
			}
		}()
	}

	// Close results when workers exit
	go func() {
		wg.Wait()
		close(results)
	}()

	var allNotes []*gitlab.Note
	for notes := range results {
		allNotes = append(allNotes, notes...)
	}

	log.Printf("Fetched %d notes in %v", len(allNotes), time.Since(start))
	return allNotes
}

func fetchMRDiscussions(client *gitlab.Client, projectID, mrIID int64) []*gitlab.Note {
	glDiscussions, _, err := client.Discussions.ListMergeRequestDiscussions(projectID, mrIID, nil)
	if err != nil {
		log.Printf("Error fetching discussions: %v", err)
		return nil
	}
	notes := make([]*gitlab.Note, 0, len(glDiscussions))

	for _, gd := range glDiscussions {
		if len(gd.Notes) == 0 {
			continue
		}
		notes = append(notes, gd.Notes...)
	}

	return notes
}

func formatDate(date *time.Time) string {
	if date == nil {
		return ""
	}
	return date.Format(time.RFC3339)
}
