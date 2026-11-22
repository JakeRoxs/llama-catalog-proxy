package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/auditlog"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/discovery"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/federation"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/proxy"
)

const defaultPeerConfigPath = "/config/config.yaml"

// peerConfigHelpText is the help text for the --config flag used by peer subcommands.
const peerConfigHelpText = "path to YAML configuration"

// peerNodeIDFlag is the CLI flag name for the peer node identity used by peer subcommands.
const peerNodeIDFlag = "node-id"

func runPeerCommand(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: llama-catalog-proxy peer {catalog-grants|config|discover|inference-grants|invite|join|members|grants} ...")
	}
	switch args[0] {
	case "invite":
		if len(args) < 2 {
			return errors.New("usage: llama-catalog-proxy peer invite {create|list|revoke} ...")
		}
		switch args[1] {
		case "create":
			return finishPeerCommand(runPeerInviteCreate(args[2:], stdout, stderr))
		case "list":
			return finishPeerCommand(runPeerInviteList(args[2:], stdout, stderr))
		case "revoke":
			return finishPeerCommand(runPeerInviteRevoke(args[2:], stdout, stderr))
		default:
			return errors.New("unknown peer invite action")
		}
	case "members":
		if len(args) < 2 {
			return errors.New("usage: llama-catalog-proxy peer members {list|revoke} ...")
		}
		switch args[1] {
		case "list":
			return finishPeerCommand(runPeerMembersList(args[2:], stdout, stderr))
		case "revoke":
			return finishPeerCommand(runPeerMemberRevoke(args[2:], stdout, stderr))
		default:
			return errors.New("unknown peer members action")
		}
	case "join":
		return finishPeerCommand(runPeerJoin(args[1:], stdout, stderr))
	case "discover":
		return finishPeerCommand(runPeerDiscover(args[1:], stdout, stderr))
	case "config":
		if len(args) < 2 || args[1] != "plan" {
			return errors.New("usage: llama-catalog-proxy peer config plan [--config path]")
		}
		return finishPeerCommand(runPeerConfigPlan(args[2:], stdout, stderr))
	case "grants":
		if len(args) < 2 {
			return errors.New("usage: llama-catalog-proxy peer grants {create|list|revoke} ...")
		}
		switch args[1] {
		case "create":
			return finishPeerCommand(runPeerGrantCreate(args[2:], stdout, stderr))
		case "list":
			return finishPeerCommand(runPeerGrantList(args[2:], stdout, stderr))
		case "revoke":
			return finishPeerCommand(runPeerGrantRevoke(args[2:], stdout, stderr))
		default:
			return errors.New("unknown peer grants action")
		}
	case "catalog-grants":
		if len(args) < 2 {
			return errors.New("usage: llama-catalog-proxy peer catalog-grants {create|list|revoke} ...")
		}
		switch args[1] {
		case "create":
			return finishPeerCommand(runPeerCatalogGrantCreate(args[2:], stdout, stderr))
		case "list":
			return finishPeerCommand(runPeerCatalogGrantList(args[2:], stdout, stderr))
		case "revoke":
			return finishPeerCommand(runPeerCatalogGrantRevoke(args[2:], stdout, stderr))
		default:
			return errors.New("unknown peer catalog-grants action")
		}
	case "inference-grants":
		if len(args) < 2 {
			return errors.New("usage: llama-catalog-proxy peer inference-grants {create|list|revoke} ...")
		}
		switch args[1] {
		case "create":
			return finishPeerCommand(runPeerInferenceGrantCreate(args[2:], stdout, stderr))
		case "list":
			return finishPeerCommand(runPeerInferenceGrantList(args[2:], stdout, stderr))
		case "revoke":
			return finishPeerCommand(runPeerInferenceGrantRevoke(args[2:], stdout, stderr))
		default:
			return errors.New("unknown peer inference-grants action")
		}
	default:
		return errors.New("unknown peer action")
	}
}

func runPeerInviteCreate(args []string, stdout, stderr io.Writer) error {
	flags := peerFlags("peer invite create", stderr)
	configPath := flags.String("config", defaultPeerConfigPath, peerConfigHelpText)
	nodeID := flags.String(peerNodeIDFlag, "", "expected node ID to invite")
	if err := parsePeerFlags(flags, args); err != nil {
		return err
	}
	if *nodeID == "" {
		return errors.New("--node-id is required")
	}
	return withPeerCLIAudit(*configPath, "pairing.invitation.create", *nodeID, "", "", stdout, func(out io.Writer) (string, string, string, error) {
		store, err := openConfiguredPairingStore(*configPath)
		if err != nil {
			return "", "", "", err
		}
		defer store.Close()
		invite, err := store.CreateInvitation(*nodeID, time.Now())
		if err != nil {
			return "", "", "", err
		}
		return invite.NodeID, "", "", json.NewEncoder(out).Encode(invite)
	})
}

func runPeerInviteList(args []string, stdout, stderr io.Writer) error {
	flags := peerFlags("peer invite list", stderr)
	configPath := flags.String("config", defaultPeerConfigPath, peerConfigHelpText)
	if err := parsePeerFlags(flags, args); err != nil {
		return err
	}
	return withPeerCLIAudit(*configPath, "pairing.invitation.list", "", "", "", stdout, func(out io.Writer) (string, string, string, error) {
		store, err := openConfiguredPairingStore(*configPath)
		if err != nil {
			return "", "", "", err
		}
		defer store.Close()
		invitations, err := store.ListInvitations(time.Now())
		if err != nil {
			return "", "", "", err
		}
		return "", "", "", json.NewEncoder(out).Encode(invitations)
	})
}

func runPeerInviteRevoke(args []string, stdout, stderr io.Writer) error {
	flags := peerFlags("peer invite revoke", stderr)
	configPath := flags.String("config", defaultPeerConfigPath, peerConfigHelpText)
	nodeID := flags.String(peerNodeIDFlag, "", "expected node ID whose invitation to cancel")
	if err := parsePeerFlags(flags, args); err != nil {
		return err
	}
	if *nodeID == "" {
		return errors.New("--node-id is required")
	}
	return withPeerCLIAudit(*configPath, "pairing.invitation.revoke", *nodeID, "", "", stdout, func(out io.Writer) (string, string, string, error) {
		store, err := openConfiguredPairingStore(*configPath)
		if err != nil {
			return "", "", "", err
		}
		defer store.Close()
		if err := store.RemoveInvitation(*nodeID); err != nil {
			return "", "", "", err
		}
		_, err = io.WriteString(out, "invitation canceled\n")
		return *nodeID, "", "", err
	})
}

func runPeerMembersList(args []string, stdout, stderr io.Writer) error {
	flags := peerFlags("peer members list", stderr)
	configPath := flags.String("config", defaultPeerConfigPath, peerConfigHelpText)
	if err := parsePeerFlags(flags, args); err != nil {
		return err
	}
	return withPeerCLIAudit(*configPath, "pairing.members.list", "", "", "", stdout, func(out io.Writer) (string, string, string, error) {
		store, err := openConfiguredPairingStore(*configPath)
		if err != nil {
			return "", "", "", err
		}
		defer store.Close()
		members, err := store.ListMembers()
		if err != nil {
			return "", "", "", err
		}
		return "", "", "", json.NewEncoder(out).Encode(members)
	})
}

func runPeerMemberRevoke(args []string, stdout, stderr io.Writer) error {
	flags := peerFlags("peer members revoke", stderr)
	configPath := flags.String("config", defaultPeerConfigPath, peerConfigHelpText)
	nodeID := flags.String(peerNodeIDFlag, "", "paired node ID to revoke")
	if err := parsePeerFlags(flags, args); err != nil {
		return err
	}
	if *nodeID == "" {
		return errors.New("--node-id is required")
	}
	return withPeerCLIAudit(*configPath, "pairing.member.revoke", *nodeID, "", "", stdout, func(out io.Writer) (string, string, string, error) {
		store, err := openConfiguredPairingStore(*configPath)
		if err != nil {
			return "", "", "", err
		}
		defer store.Close()
		if err := store.RemoveMember(*nodeID, time.Now()); err != nil {
			return "", "", "", err
		}
		_, err = io.WriteString(out, "membership revoked; re-pairing requires a newly issued identity certificate\n")
		return *nodeID, "", "", err
	})
}

func runPeerGrantCreate(args []string, stdout, stderr io.Writer) error {
	flags := peerFlags("peer grants create", stderr)
	configPath := flags.String("config", defaultPeerConfigPath, peerConfigHelpText)
	requester := flags.String("requester-node", "", "paired requester node ID")
	backend := flags.String("backend", "", "locally owned backend ID")
	models := flags.String("models", "", "comma-separated local model IDs")
	expiresIn := flags.String("expires-in", "30d", "grant lifetime from 5m to 90d")
	acknowledge := flags.Bool("acknowledge", false, "confirm this owner authorization change")
	if err := parsePeerFlags(flags, args); err != nil {
		return err
	}
	if *requester == "" || *backend == "" || *models == "" || !*acknowledge {
		return errors.New("--requester-node, --backend, --models, and --acknowledge are required")
	}
	return withPeerCLIAudit(*configPath, "owner_grant.create", "", "", "", stdout, func(out io.Writer) (string, string, string, error) {
		grantConfig, store, err := openOwnerGrantConfig(*configPath)
		if err != nil {
			return "", "", "", err
		}
		defer store.Close()
		peerGrant, ok := grantConfig.Peer.Peers[*requester]
		if !ok || !peerGrant.ManageSettings || !containsPeerCLIString(grantConfig.Peer.LocalBackends, *backend) {
			return "", "", "", errors.New("requester lacks the static owner scope or backend is not locally owned")
		}
		if !store.IsMember(*requester) {
			return "", "", "", errors.New("requester is not a paired node identity")
		}
		targetID, ok := grantConfig.Peer.ManagementTargets[*backend]
		if !ok || grantConfig.Management == nil {
			return "", "", "", errors.New("backend has no local settings target")
		}
		target, ok := grantConfig.Management.Targets[targetID]
		if !ok {
			return "", "", "", errors.New("local management target is unavailable")
		}
		modelIDs := splitOwnerGrantModels(*models)
		if len(modelIDs) == 0 || len(modelIDs) > modelconfig.MaxOwnerGrantModels {
			return "", "", "", errors.New("--models must name between one and 64 models")
		}
		for _, modelID := range modelIDs {
			if _, err := modelconfig.ReadSettingsTarget(target.Directory, target.Filename, targetID, modelID); err != nil {
				return "", "", "", errors.New("a requested model is not safely configurable on the target")
			}
		}
		sort.Strings(modelIDs)
		lifetime, err := time.ParseDuration(strings.TrimSpace(*expiresIn))
		if err != nil || lifetime < 5*time.Minute || lifetime > modelconfig.MaxOwnerGrantLifetime {
			return "", "", "", errors.New("--expires-in must be between 5m and 90d")
		}
		id, err := randomCLIGrantID()
		if err != nil {
			return "", "", "", errors.New("could not create owner grant")
		}
		created := time.Now().UTC()
		grant := modelconfig.OwnerGrant{
			ID: id, RequesterNodeID: *requester, Action: "manage_settings", BackendID: *backend, TargetID: targetID,
			ModelIDs: modelIDs, CreatedAt: created, ExpiresAt: created.Add(lifetime), ApprovedBy: "local_cli",
		}
		if err := modelconfig.CreateOwnerGrant(grantConfig.Management.DelegatedStore.GrantDirectory, grant); err != nil {
			return "", "", "", err
		}
		return targetID, "", grant.ID, json.NewEncoder(out).Encode(grant)
	})
}

func runPeerGrantList(args []string, stdout, stderr io.Writer) error {
	flags := peerFlags("peer grants list", stderr)
	configPath := flags.String("config", defaultPeerConfigPath, peerConfigHelpText)
	if err := parsePeerFlags(flags, args); err != nil {
		return err
	}
	return withPeerCLIAudit(*configPath, "owner_grant.list", "", "", "", stdout, func(out io.Writer) (string, string, string, error) {
		grantConfig, store, err := openOwnerGrantConfig(*configPath)
		if err != nil {
			return "", "", "", err
		}
		defer store.Close()
		grants, err := modelconfig.ListOwnerGrants(grantConfig.Management.DelegatedStore.GrantDirectory)
		if err != nil {
			return "", "", "", err
		}
		return "", "", "", json.NewEncoder(out).Encode(grants)
	})
}

func runPeerGrantRevoke(args []string, stdout, stderr io.Writer) error {
	flags := peerFlags("peer grants revoke", stderr)
	configPath := flags.String("config", defaultPeerConfigPath, peerConfigHelpText)
	grantID := flags.String("grant-id", "", "owner grant ID to revoke")
	if err := parsePeerFlags(flags, args); err != nil {
		return err
	}
	if *grantID == "" {
		return errors.New("--grant-id is required")
	}
	return withPeerCLIAudit(*configPath, "owner_grant.revoke", "", "", *grantID, stdout, func(out io.Writer) (string, string, string, error) {
		grantConfig, store, err := openOwnerGrantConfig(*configPath)
		if err != nil {
			return "", "", *grantID, err
		}
		defer store.Close()
		grants, err := modelconfig.ListOwnerGrants(grantConfig.Management.DelegatedStore.GrantDirectory)
		if err != nil {
			return "", "", *grantID, err
		}
		var targetID, modelID string
		for _, grant := range grants {
			if grant.ID == *grantID {
				targetID, modelID = grant.TargetID, strings.Join(grant.ModelIDs, ",")
				break
			}
		}
		if err := modelconfig.RevokeOwnerGrant(grantConfig.Management.DelegatedStore.GrantDirectory, *grantID); err != nil {
			return targetID, modelID, *grantID, err
		}
		_, err = io.WriteString(out, "owner grant revoked\n")
		return targetID, modelID, *grantID, err
	})
}

func openOwnerGrantConfig(configPath string) (config.Config, *federation.PairingStore, error) {
	cfg, err := config.Load(configPath)
	if err != nil {
		return config.Config{}, nil, err
	}
	if cfg.Peer == nil || cfg.Peer.FederationID == "" || cfg.Management == nil || cfg.Management.DelegatedStore == nil ||
		cfg.Management.DelegatedStore.GrantDirectory == "" {
		return config.Config{}, nil, errors.New("federation grant policy is not configured")
	}
	store, err := federation.OpenPairingStore(cfg.Peer.MembershipDirectory, cfg.Peer.FederationID, cfg.Peer.NodeID)
	if err != nil {
		return config.Config{}, nil, errors.New("pairing store unavailable; run as the proxy service account")
	}
	return cfg, store, nil
}

func beginPeerCLIAudit(configPath, action, targetID, modelID, correlationID string) (*auditlog.Store, *auditlog.Transaction, error) {
	cfg, err := config.Load(configPath)
	if err != nil || cfg.Peer == nil || cfg.Peer.FederationID == "" || cfg.Peer.AuditDirectory == "" {
		return nil, nil, errors.New("protected federation audit store is not configured")
	}
	store, err := auditlog.Open(cfg.Peer.AuditDirectory, cfg.Peer.NodeID)
	if err != nil {
		return nil, nil, errors.New("protected federation audit store unavailable")
	}
	event := auditlog.Event{
		LocalNodeID: cfg.Peer.NodeID, PrincipalType: "local_cli", UserIdentityState: "local_process",
		LocalAccount: auditlog.LocalAccountID(), Action: action, TargetID: targetID, ModelID: modelID,
		CorrelationID: correlationID,
	}
	transaction, err := store.Begin(event)
	if err != nil {
		_ = store.Close()
		return nil, nil, err
	}
	return store, transaction, nil
}

func finishPeerCLIAudit(transaction *auditlog.Transaction, outcome, targetID, modelID, correlationID string, operationErr error) error {
	if err := transaction.FinishWith(outcome, targetID, modelID, correlationID); err != nil {
		return fmt.Errorf("audit completion failed; operation outcome may be unknown: %w", err)
	}
	return operationErr
}

func withPeerCLIAudit(configPath, action, targetID, modelID, correlationID string, stdout io.Writer,
	operation func(io.Writer) (string, string, string, error)) error {
	audit, transaction, err := beginPeerCLIAudit(configPath, action, targetID, modelID, correlationID)
	if err != nil {
		return err
	}
	defer audit.Close()
	var output bytes.Buffer
	target, model, correlation, operationErr := operation(&output)
	outcome := "succeeded"
	if operationErr != nil {
		outcome = "failed"
		if errors.Is(operationErr, federation.ErrPairingStore) || errors.Is(operationErr, federation.ErrInvitation) ||
			errors.Is(operationErr, federation.ErrCatalogGrantStore) || errors.Is(operationErr, modelconfig.ErrOwnerGrantStore) ||
			errors.Is(operationErr, auditlog.ErrUnavailable) || errors.Is(operationErr, proxy.ErrComfyUIReleaseUnknown) {
			outcome = "unknown"
		}
	}
	if err := finishPeerCLIAudit(transaction, outcome, target, model, correlation, operationErr); err != nil {
		return err
	}
	if operationErr != nil {
		return operationErr
	}
	_, err = io.Copy(stdout, &output)
	return err
}

func splitOwnerGrantModels(value string) []string {
	seen := make(map[string]bool)
	var models []string
	for _, part := range strings.Split(value, ",") {
		model := strings.TrimSpace(part)
		if model == "" || seen[model] {
			return nil
		}
		seen[model] = true
		models = append(models, model)
	}
	return models
}

func containsPeerCLIString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func randomCLIGrantID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

func runPeerJoin(args []string, stdout, stderr io.Writer) error {
	flags := peerFlags("peer join", stderr)
	configPath := flags.String("config", defaultPeerConfigPath, peerConfigHelpText)
	peerURL := flags.String("url", "", "HTTPS origin of the node issuing the invitation")
	discoverNodeID := flags.String("discover-node-id", "", "select the mDNS candidate by exact node ID")
	expectedNodeID := flags.String("expected-node-id", "", "require this node ID from the peer TLS certificate")
	invitationFile := flags.String("invitation-file", "", "owner-only file containing the invitation code")
	if err := parsePeerFlags(flags, args); err != nil {
		return err
	}
	if (*peerURL == "") == (*discoverNodeID == "") || *invitationFile == "" {
		return errors.New("provide exactly one of --url or --discover-node-id and an --invitation-file; do not pass invitation codes as command arguments")
	}
	if *discoverNodeID != "" {
		if *expectedNodeID != "" && *expectedNodeID != *discoverNodeID {
			return errors.New("--expected-node-id must match --discover-node-id")
		}
		*expectedNodeID = *discoverNodeID
	}
	code, err := readPeerInvitation(*invitationFile)
	if err != nil {
		return err
	}
	return withPeerCLIAudit(*configPath, "pairing.join", "", "", "", stdout, func(out io.Writer) (string, string, string, error) {
		cfg, err := config.Load(*configPath)
		if err != nil {
			return "", "", "", err
		}
		if cfg.Peer == nil || cfg.Peer.FederationID == "" {
			return "", "", "", errors.New("peer federation identity is not configured")
		}
		targetURL := *peerURL
		if *discoverNodeID != "" {
			if cfg.Peer.Discovery == nil || !cfg.Peer.Discovery.Enabled {
				return "", "", "", errors.New("mDNS discovery is not enabled in configuration")
			}
			candidates, err := discovery.Browse(context.Background(), discovery.Settings{
				NodeID: cfg.Peer.NodeID, FederationID: cfg.Peer.FederationID,
				Interface: cfg.Peer.Discovery.Interface, AdvertiseURL: cfg.Peer.Discovery.AdvertiseURL,
			}, 3*time.Second)
			if err != nil {
				return "", "", "", err
			}
			targetURL, err = selectPeerCandidate(candidates, *discoverNodeID)
			if err != nil {
				return "", "", "", err
			}
		}
		store, err := federation.OpenPairingStore(cfg.Peer.MembershipDirectory, cfg.Peer.FederationID, cfg.Peer.NodeID)
		if err != nil {
			return "", "", "", err
		}
		defer store.Close()
		certificate, err := tls.LoadX509KeyPair(cfg.Peer.TLSCertFile, cfg.Peer.TLSKeyFile)
		if err != nil {
			return "", "", "", errors.New("could not load local peer identity certificate")
		}
		roots, err := federation.LoadTrustRoots(cfg.Peer.ClientCAFile)
		if err != nil {
			return "", "", "", errors.New("could not load federation CA bundle")
		}
		crls, err := federation.LoadCRLs(cfg.Peer.ClientCRLFile, cfg.Peer.RevocationMaxAge())
		if err != nil {
			return "", "", "", errors.New("could not load a fresh federation CRL bundle")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		defer cancel()
		member, err := store.JoinExpected(ctx, targetURL, federation.JoinExpectedOptions{
			ExpectedNodeID: *expectedNodeID,
			Code:           code,
			Certificate:    certificate,
			Roots:          roots,
			CRLs:           crls,
			MaxCRLAge:      cfg.Peer.RevocationMaxAge(),
		})
		if err != nil {
			return "", "", "", fmt.Errorf("pairing failed: %w", err)
		}
		return member.NodeID, "", "", json.NewEncoder(out).Encode(map[string]any{"state": "paired", "member": member})
	})
}

func openConfiguredPairingStore(configPath string) (*federation.PairingStore, error) {
	cfg, err := config.Load(configPath)
	if err != nil {
		return nil, err
	}
	if cfg.Peer == nil || cfg.Peer.FederationID == "" {
		return nil, errors.New("peer federation identity is not configured")
	}
	store, err := federation.OpenPairingStore(cfg.Peer.MembershipDirectory, cfg.Peer.FederationID, cfg.Peer.NodeID)
	if err != nil {
		return nil, errors.New("pairing store unavailable; run as the proxy service account")
	}
	return store, nil
}

func readPeerInvitation(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 || info.Size() > 4096 {
		return "", errors.New("invitation file must be a protected regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil || len(data) > 4096 {
		return "", errors.New("could not read invitation file")
	}
	value := strings.TrimSpace(string(data))
	if strings.HasPrefix(value, "{") {
		decoder := json.NewDecoder(strings.NewReader(value))
		decoder.DisallowUnknownFields()
		var invite federation.Invitation
		if decoder.Decode(&invite) != nil || decoder.Decode(new(any)) != io.EOF {
			return "", errors.New("invalid invitation file")
		}
		return invite.Code, nil
	}
	return value, nil
}

func peerFlags(name string, stderr io.Writer) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(stderr)
	return flags
}

func parsePeerFlags(flags *flag.FlagSet, args []string) error {
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return flag.ErrHelp
		}
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional argument")
	}
	return nil
}

func finishPeerCommand(err error) error {
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	return err
}

func peerCommandError(stderr io.Writer, err error) {
	_, _ = fmt.Fprintf(stderr, "peer command failed: %v\n", err)
}
