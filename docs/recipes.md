# Recipe authoring

Recipes live under the controller's recipes directory
(`SPARK_FLEET_RECIPES_DIR`, default `/var/lib/spark-fleet/recipes`). They are
YAML files versionable in Git and editable from the UI's structured form or raw
YAML tab.

## Skeleton

```yaml
apiVersion: spark-fleet/v1
kind: Recipe
metadata:
  name: demo-glm            # lowercase DNS label, also the default deployment name
  displayName: GLM 5.3 Flash
  description: Two-node EXL3 deployment
spec:
  mode: deployment             # deployment | service
  runtime: vllm                # vllm | sglang | llamacpp | tensorfold | comfyui | custom
  model:                       # required for gateway routing
    repo: org/model
    servedName: Demo-Model-B
    revision: null
    extraRepos: []
  gateway:
    aliases: [demo-glm]           # must be unique among loaded deployments
  topology:
    preset: custom             # single | custom | tensorfold
    masterPort: 25000
    tensorParallelSize: 2
  roles: []
  prepare: []
```

## Roles

Every role needs `name`, `node`, `class` (`heavy` or `light`), and `service`.
A deployment must contain at least one heavy role, and no node may host two heavy
roles in the same recipe. Service recipes have exactly one light role and no
gateway aliases.

```yaml
roles:
  - name: worker
    node: spark-02
    class: heavy
    startOrder: 10             # lower starts first
    readiness:
      type: running
      timeoutSeconds: 3600
    service:
      image: vllm/vllm-openai:latest
      network_mode: host
      gpus: all
      ipc: host
      shm_size: 32gb
      environment:
        MASTER_ADDR: "{{ node.spark-01.fabricIp }}"
        MASTER_PORT: "{{ topology.masterPort }}"
        NODE_RANK: "1"
      command: ["vllm", "serve", "{{ model.repo }}", "--port", "8888"]
  - name: head
    node: spark-01
    class: heavy
    startOrder: 20
    inferencePort: 8888
    readiness:
      type: http
      path: /v1/models
      timeoutSeconds: 3600
    service:
      image: vllm/vllm-openai:latest
      network_mode: host
      gpus: all
      ipc: host
      shm_size: 32gb
      environment:
        MASTER_ADDR: "{{ node.spark-01.fabricIp }}"
        MASTER_PORT: "{{ topology.masterPort }}"
        NODE_RANK: "0"
      command: ["vllm", "serve", "{{ model.repo }}", "--port", "8888"]
```

Conventions:

- Start workers first (`startOrder` 10), head last (20).
- The head declares `inferencePort`; gateway traffic routes there.
- Use the fabric IP for distributed master addresses.
- `gpus: all`, `ipc: host`, and a large `shm_size` are typical for heavy serving.

## Readiness

| Type | Required fields | Meaning |
| --- | --- | --- |
| `running` | none | Compose reports a running container |
| `container-health` | none | Docker healthcheck is healthy |
| `http` | `path`, optional `port` (defaults to `inferencePort`) | GET returns 2xx/3xx |
| `tcp` | `port` | TCP connection succeeds |
| `log-pattern` | `pattern` | Container logs match the regex |

All types accept `timeoutSeconds` (default 3600) and `intervalSeconds` (default 5).

## Placeholders

The renderer resolves:

- `{{ node.<id>.lanIp }}` and `{{ node.<id>.fabricIp }}`
- `{{ topology.masterPort }}`
- `{{ model.repo }}` and `{{ model.servedName }}`

Use quoted placeholders so YAML parses them as strings.

## Prepare jobs

Prepare jobs run once before roles start, with captured logs and a timeout. Use
them for image pulls, model staging, or fetch helpers:

```yaml
prepare:
  - name: stage-model
    node: spark-02
    image: alpine/git:latest
    command: ["sh", "-c", "echo staging"]
    volumes: ["/srv/spark-fleet/hub:/models"]
    environment:
      HF_HOME: /models
    timeoutSeconds: 600
```

Prepare nodes do not have to host a role; unload ignores prepare-only nodes.

## Gateway aliases

Aliases and the served model name are both routable. `spark-active` is reserved
for the primary deployment. Duplicate aliases among **loaded** deployments are
rejected; unloaded recipes may share aliases while being drafted, but loading the
second one fails until the conflict is resolved.

## Model swaps

The UI's Swap model action edits only `spec.model.repo`, `servedName`, and
`revision`, preserving roles, flags, aliases, and topology. It previews a diff
before applying; redeploy the deployment to activate the new model.

## Authoring workflow

1. Create or duplicate a recipe in the UI.
2. Use the structured form for common fields; switch to Raw YAML for Compose keys
   not exposed in the form (`healthcheck`, `ulimits`, `devices`, `cap_add`, etc.).
3. Validate: the controller renders per-role Compose documents with config hashes.
4. Save. Fix any node, alias, port, or schema errors reported.
5. Create a deployment from the recipe, then Load. Watch the job phases in
   Activity and the deployment card.
6. On failure, inspect role logs, fix the recipe, and redeploy.

## Examples

- `recipes/examples/vllm-multi-node.yaml` — two-node vLLM over the fabric
- `recipes/examples/tensorfold-single.yaml` — TensorFold EXL3 single node
- `recipes/examples/sglang-single.yaml` — SGLang OpenAI-compatible server
- `recipes/examples/llamacpp-single.yaml` — GGUF model with a mounted cache
- `recipes/examples/openwebui-service.yaml` — independent chat UI service
- `recipes/examples/comfyui-service.yaml` — independent image-generation service

All examples parse against the schema and target `spark-01`/`spark-02`.

## Common pitfalls

- `masterPort` must be an integer; leaving it blank in the UI defaults to 25000.
- `http` readiness requires `path`; `tcp` requires `port`; `log-pattern` requires
  `pattern`.
- Published host ports must not collide within a role; the schema rejects
  duplicate ports per node.
- The gateway skips deployments with no heavy role that exposes `inferencePort`.
- Service recipes cannot be primary and cannot declare gateway aliases.
- Model repos are cached per node; a deployment fails validation if any role's
  node lacks the model. Download it first from the Model Vault.
