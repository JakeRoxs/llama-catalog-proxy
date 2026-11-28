package benchmark

type syntheticWorkload struct {
	Name            string
	Category        string
	Version         string
	SystemPrompt    string
	UserPrompt      string
	Expected        string
	ReasoningEffort string
	ToolRoundTrip   bool
}

var syntheticWorkloads = []syntheticWorkload{
	{
		Name:          "tool_round_trip",
		Category:      "tool_behavior",
		Version:       WorkloadVersion,
		UserPrompt:    `Call the benchmark_echo tool with message "TOOL_ROUND_TRIP_OK", then return its result exactly.`,
		Expected:      "TOOL_ROUND_TRIP_OK",
		ToolRoundTrip: true,
	},
	{
		Name:            "reasoning_control_low",
		Category:        "reasoning_control",
		Version:         WorkloadVersion,
		UserPrompt:      "Return exactly REASONING_CONTROL_OK and nothing else.",
		Expected:        "REASONING_CONTROL_OK",
		ReasoningEffort: "low",
	},
	{
		Name:            "reasoning_control_high",
		Category:        "reasoning_control",
		Version:         WorkloadVersion,
		UserPrompt:      "Return exactly REASONING_CONTROL_OK and nothing else.",
		Expected:        "REASONING_CONTROL_OK",
		ReasoningEffort: "high",
	},
	{
		Name:       "literal_closing_tag",
		Category:   "literal_text",
		Version:    WorkloadVersion,
		UserPrompt: `Return exactly the literal text "</think>" and nothing else.`,
		Expected:   "</think>",
	},
	{
		Name:       "system_defaults",
		Category:   "system_default",
		Version:    WorkloadVersion,
		UserPrompt: "Return exactly SYSTEM_DEFAULTS_OK and nothing else.",
		Expected:   "SYSTEM_DEFAULTS_OK",
	},
	{
		Name:       "fenced_examples",
		Category:   "fenced_example",
		Version:    WorkloadVersion,
		UserPrompt: "Count the numbered examples in this fenced block. Return only the number.\n\n```text\n1. alpha\n2. beta\n```",
		Expected:   "2",
	},
}
