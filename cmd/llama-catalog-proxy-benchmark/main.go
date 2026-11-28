package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/benchmark"
)

func main() {
	if err := execute(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "benchmark:", err)
		os.Exit(1)
	}
}

func execute(args []string) error {
	if len(args) == 0 {
		return errors.New("expected run or compare subcommand")
	}
	switch args[0] {
	case "run":
		return runCommand(args[1:])
	case "compare":
		return compareCommand(args[1:])
	default:
		return errors.New("expected run or compare subcommand")
	}
}

func runCommand(args []string) error {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	endpoint := flags.String("endpoint", os.Getenv("LLAMA_CATALOG_DIAGNOSTICS_URL"), "private proxy origin")
	backend := flags.String("backend", "", "configured backend ID")
	model := flags.String("model", "", "public model ID already observed ready")
	repetitions := flags.Int("repetitions", 1, "serial repetitions per synthetic workload (max 3)")
	temperature := flags.Float64("temperature", 0, "requested temperature")
	topP := flags.Float64("top-p", 1, "requested top-p")
	maxOutput := flags.Int("max-output-tokens", 64, "per-request output cap (max 128)")
	tokenBudget := flags.Int("token-budget", benchmark.MaxTotalTokenBudget, "total reserved output-token budget (max 4096)")
	timeBudget := flags.Duration("time-budget", benchmark.MaxRunDuration, "overall run deadline (max 5m)")
	cacheState := flags.String("cache-state", "unknown", "operator-declared cache state: unknown, warm, or cold")
	allowInterference := flags.Bool("allow-active-inference-interference", false, "required opt-in to generate inference load")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if !*allowInterference {
		return errors.New("live benchmark requires --allow-active-inference-interference")
	}
	if *endpoint == "" {
		return errors.New("set --endpoint or LLAMA_CATALOG_DIAGNOSTICS_URL")
	}
	if *repetitions < 1 || *maxOutput < 1 || *tokenBudget < 1 || *timeBudget <= 0 {
		return errors.New("repetitions, output-token limit, token budget, and time budget must be positive")
	}
	cache := benchmark.CacheState(*cacheState)
	switch cache {
	case benchmark.CacheUnknown, benchmark.CacheCold, benchmark.CacheWarm:
	default:
		return errors.New("cache state must be unknown, warm, or cold")
	}
	key := os.Getenv("LLAMA_CATALOG_DIAGNOSTICS_API_KEY")
	client, err := benchmark.NewAPIClient(*endpoint, key)
	if err != nil {
		return err
	}
	directory, err := reportDirectory()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	record, err := benchmark.Run(ctx, benchmark.RunOptions{
		Client:            client,
		Backend:           *backend,
		Model:             *model,
		Temperature:       *temperature,
		TopP:              *topP,
		MaxOutput:         *maxOutput,
		Repetitions:       *repetitions,
		CacheState:        cache,
		TimeBudget:        *timeBudget,
		TokenBudget:       *tokenBudget,
		AllowInterference: *allowInterference,
	})
	if err != nil {
		return err
	}
	path, err := benchmark.WriteRun(directory, record)
	if err != nil {
		return err
	}
	fmt.Printf("run %s (%s) saved to %s\n", record.RunID, record.Status, path)
	if record.Status == benchmark.RunCanceled {
		return errors.New("benchmark canceled; partial report saved")
	}
	return nil
}

func compareCommand(args []string) error {
	flags := flag.NewFlagSet("compare", flag.ContinueOnError)
	baselineID := flags.String("baseline", "", "baseline run ID")
	candidateID := flags.String("candidate", "", "candidate run ID")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *baselineID == "" || *candidateID == "" {
		return errors.New("compare requires --baseline and --candidate run IDs")
	}
	directory, err := reportDirectory()
	if err != nil {
		return err
	}
	baseline, err := benchmark.ReadRun(directory, *baselineID)
	if err != nil {
		return err
	}
	candidate, err := benchmark.ReadRun(directory, *candidateID)
	if err != nil {
		return err
	}
	report, err := benchmark.Compare(baseline, candidate)
	if err != nil {
		return err
	}
	path, err := benchmark.WriteComparison(directory, report)
	if err != nil {
		return err
	}
	fmt.Printf("comparison saved to %s\n", path)
	return nil
}

func reportDirectory() (string, error) {
	directory, err := os.Getwd()
	if err != nil {
		return "", errors.New("could not determine the working directory")
	}
	for {
		gitPath := filepath.Join(directory, ".git")
		if _, err := os.Stat(gitPath); err == nil {
			ignoreFile, err := os.ReadFile(filepath.Join(directory, ".gitignore"))
			if err != nil || !containsIgnoreRule(string(ignoreFile), benchmark.DefaultOutputDir+"/") {
				return "", errors.New("repository .gitignore must exclude benchmark-runs/")
			}
			return filepath.Join(directory, benchmark.DefaultOutputDir), nil
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			return "", errors.New("could not locate the repository root")
		}
		directory = parent
	}
}

func containsIgnoreRule(contents, rule string) bool {
	for _, line := range strings.Split(contents, "\n") {
		if strings.TrimSpace(line) == rule {
			return true
		}
	}
	return false
}
