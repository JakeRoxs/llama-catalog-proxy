package main

import (
	"context"
	"fmt"
	"os"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/agenttools"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func main() {
	endpoint := os.Getenv("LLAMA_CATALOG_DIAGNOSTICS_URL")
	apiKey := os.Getenv("LLAMA_CATALOG_DIAGNOSTICS_API_KEY")
	if endpoint == "" || apiKey == "" {
		fmt.Fprintln(os.Stderr, "LLAMA_CATALOG_DIAGNOSTICS_URL and LLAMA_CATALOG_DIAGNOSTICS_API_KEY are required")
		os.Exit(2)
	}

	server, err := agenttools.NewServer(endpoint, apiKey)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		fmt.Fprintln(os.Stderr, "MCP server stopped:", err)
		os.Exit(1)
	}
}
