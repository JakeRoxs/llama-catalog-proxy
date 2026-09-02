# Docker Deployment Instructions

These instructions apply when deploying this project or related custom services to the Docker host.

## Docker Host

- Hostname: `<hostname>`
- LAN address: `<lan-address>`
- SSH user: `<user>`
- Connect with `ssh <lan-address>` from the development workstation.
- Docker and Docker Compose are available to `<user>` through membership in the `docker` group.
- The Docker root is the host directory referenced as `$DOCKERDIR`.

## Directory Layout

```text
$DOCKERDIR/
├── .env                              # Master Compose variables; privileged access
├── docker-compose-<hostname>.yml     # Master Compose file
├── build/                            # Source/build contexts for custom images
│   └── <service>/
├── compose/
│   └── <hostname>/                   # One Compose fragment per service
│       └── <service>.yml
├── appdata/                          # Persistent data and runtime configuration
│   └── <service>/
├── logs/
│   └── <hostname>/
├── secrets/                          # Restricted secrets; do not inspect or copy
└── shared/
    └── config/                       # Shared host configuration
```

For `llama-catalog-proxy`, the deployed paths are:

```text
$DOCKERDIR/build/llama-catalog-proxy/
$DOCKERDIR/appdata/llama-catalog-proxy/config.yaml
$DOCKERDIR/compose/<hostname>/llama-catalog-proxy.yml
```

Do not deploy custom projects under `/opt` unless the user explicitly requests an exception.

## Compose Organization

- Define each service in its own `compose/<hostname>/<service>.yml` fragment.
- Add the fragment to the `include:` list in the master Compose file.
- Use the existing form: `compose/$HOSTNAME/<service>.yml`.
- Place related services next to each other in the include list.
- Use `container_name`, an explicit restart policy, explicit networks, volumes, and ports.
- AI services normally use `profiles: ["ai", "all"]`.
- Prefer `restart: unless-stopped` for long-running services unless an existing related service establishes another convention.
- Use `${VARIABLE:-default}` for configurable host ports, such as `${LLAMA_CATALOG_PROXY_PORT:-9293}`.
- Use `$DOCKERDIR/appdata/<service>` for persistent state and runtime configuration.
- Use `$DOCKERDIR/build/<service>` as the build context for locally built images.
- Do not use host networking unless explicitly required.
- Add a healthcheck when the image has a practical health command.

Representative fragment structure:

```yaml
services:
  example:
    image: example:local
    build:
      context: $DOCKERDIR/build/example
    container_name: example
    profiles: ["ai", "all"]
    restart: unless-stopped
    networks:
      - default
    ports:
      - "${EXAMPLE_PORT:-8080}:8080"
    environment:
      - TZ=$TZ
    volumes:
      - $DOCKERDIR/appdata/example/config.yaml:/config/config.yaml:ro
```

## Networks

- The master stack defines and uses `docker_default` as its default bridge network.
- Fragments included through the master file normally use `networks: [default]`.
- When a fragment must be managed independently without access to the master `.env`, do not let standalone Compose own or reconcile `docker_default`.
- In that case, give the service a distinct external network alias:

```yaml
services:
  example:
    networks:
      - example_default

networks:
  example_default:
    external: true
    name: docker_default
```

- Never run `docker compose ... --remove-orphans` against a fragment in the shared `docker` project.

## Environment And Secrets

- The master `.env` is `$DOCKERDIR/.env`.
- It is root-owned and may not be readable by the deploy user.
- Do not change `.env` ownership, permissions, or ACLs automatically.
- Do not read or display secret values.
- Variables referenced throughout existing fragments include `DOCKERDIR`, `HOSTNAME`, `TZ`, `PUID`, `PGID`, and storage-mount variables.
- Add service-specific variables only when useful; provide defaults in fragments when practical.
- Store runtime API keys in the service's protected appdata configuration or the existing secrets mechanism, never in source files or logs.

## Deployment Workflow

1. Inspect related fragments and the master include list before editing.
2. Verify the destination parent directories before creating paths.
3. Transfer source files to `build/<service>` and exclude tests when they are not required by the image build.
4. Place runtime configuration under `appdata/<service>`.
5. Add or update `compose/<hostname>/<service>.yml`.
6. Add the fragment to the master Compose file near related services.
7. Validate the fragment before starting it.
8. Build and start only the target service.
9. Verify container health, logs, network attachment, and externally exposed endpoints.
10. Never remove, restart, or recreate unrelated containers.

For a fragment using the external network alias, unprivileged validation can use:

```bash
env DOCKERDIR=$DOCKERDIR TZ=$TZ \
  docker compose \
  --project-name docker \
  --env-file /dev/null \
  -f $DOCKERDIR/compose/<hostname>/<service>.yml \
  --profile ai \
  config --quiet
```

The normal privileged stack-management path should validate the complete master file because it can read `.env`.

## llama-catalog-proxy

- Multi-backend router: every backend may use a configured public prefix, while unmatched model IDs fall back to the named default backend.
- Published proxy endpoint: `http://<lan-address>:<proxy-port>`
- Image: `llama-catalog-proxy:local`
- Container: `llama-catalog-proxy`
- Profiles: `ai`, `all`
- Network: external alias to `docker_default`
- Runtime config: `$DOCKERDIR/appdata/llama-catalog-proxy/config.yaml`
- Hot reload: mount the config **directory** (`$DOCKERDIR/appdata/llama-catalog-proxy/:/config:ro`), not the file. A single-file bind mount captures the inode at container start and does not follow file replacements, so config edits (e.g. `sed -i`) would not be seen.
