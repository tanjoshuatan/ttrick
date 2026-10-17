# ==============================================================================
# terminal/.bashrc
# Shared cache configuration for /project/fyp26_lz1/
# ==============================================================================

# 1. Define paths (uses USERNAME injected by client.py)
BASE_DIR="/project/fyp26_lz1"
USER_DIR="$BASE_DIR/${USERNAME:-$(whoami)}"

# 2. Ensure the user's personal folder and all shared cache dirs exist
mkdir -p "$USER_DIR"
mkdir -p "$BASE_DIR/.cache/tmp"
mkdir -p "$BASE_DIR/.hf_cache/hub"
mkdir -p "$BASE_DIR/.hf_cache/xet"
mkdir -p "$BASE_DIR/.vllm"
mkdir -p "$BASE_DIR/.vllm-config"

# 3. Set UMASK so group members (31890) can read/write each other's files
umask 002

# 4. Standard Linux XDG paths (mapped to your existing shared folders)
export XDG_CACHE_HOME="$BASE_DIR/.cache"
export XDG_CONFIG_HOME="$BASE_DIR/.config"
export XDG_DATA_HOME="$BASE_DIR/.local"

# 5. Hugging Face (including the Xet cache)
export HF_HOME="$BASE_DIR/.hf_cache"
export HF_HUB_CACHE="$BASE_DIR/.hf_cache/hub"
export HF_XET_CACHE="$BASE_DIR/.hf_cache/xet"

# 6. UV caches (mapped to your existing shared folders)
export UV_CACHE_DIR="$BASE_DIR/.uv-cache"
export UV_PYTHON_INSTALL_DIR="$BASE_DIR/.uv-python"

# 7. Python & ML tool caches
export PIP_CACHE_DIR="$BASE_DIR/.cache/pip"
export TORCH_HOME="$BASE_DIR/.cache/torch"
export TRITON_CACHE_DIR="$BASE_DIR/.triton"

# 8. vLLM caches
export VLLM_CACHE_ROOT="$BASE_DIR/.vllm"
export VLLM_CONFIG_ROOT="$BASE_DIR/.vllm-config"

# 9. Node / NPM
export NPM_CONFIG_CACHE="$BASE_DIR/.npm"

# 10. Temporary files (prevents writing to system /tmp)
export TMPDIR="$BASE_DIR/.cache/tmp"