# ==============================================================================
# terminal/.bashrc
# Shared cache configuration for /project/fyp26_lz1/
# ==============================================================================

# 1. Define paths using the USERNAME injected by client.py
BASE_DIR="/project/fyp26_lz1"
USER_DIR="$BASE_DIR/$USERNAME"

# 2. Set UMASK so group members can read/write each other's files
umask 002

# 3. Standard Linux XDG paths
export XDG_CACHE_HOME="$BASE_DIR/.cache"
export XDG_CONFIG_HOME="$BASE_DIR/.config"
export XDG_DATA_HOME="$BASE_DIR/.local"

# 4. Hugging Face
export HF_HOME="$BASE_DIR/.hf_cache"
export HF_HUB_CACHE="$BASE_DIR/.hf_cache/hub"
export HF_XET_CACHE="$BASE_DIR/.hf_cache/xet"

# 5. UV caches
export UV_CACHE_DIR="$BASE_DIR/.uv-cache"
export UV_PYTHON_INSTALL_DIR="$BASE_DIR/.uv-python"

# 6. Python & ML tool caches
export PIP_CACHE_DIR="$BASE_DIR/.cache/pip"
export TORCH_HOME="$BASE_DIR/.cache/torch"
export TRITON_CACHE_DIR="$BASE_DIR/.triton"

# 7. vLLM caches
export VLLM_CACHE_ROOT="$BASE_DIR/.vllm"
export VLLM_CONFIG_ROOT="$BASE_DIR/.vllm-config"

# 8. Node / NPM
export NPM_CONFIG_CACHE="$BASE_DIR/.npm"

# 9. Temporary files
export TMPDIR="$BASE_DIR/.cache/tmp"