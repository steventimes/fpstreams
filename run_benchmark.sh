#!/usr/bin/env bash

set -euo pipefail

#
# fpstreams benchmark 快捷启动脚本
# 只改下面的参数，不用每次在命令行反复输入选项。
#

# =========================
# 1) 基础参数（改这里就行）
# =========================

# release        = 常规模式（原生场景、可做回归检查）
# competitive    = 跨库对比（fpstreams vs python / numpy / pandas）
MODE="competitive"

# Python 解释器；留空会优先使用项目的 .venv/bin/python，不存在时再用 python3。
# 如需固定其他环境，可直接填写绝对路径，例如 "/opt/python/bin/python3"。
PYTHON_BIN=""

# 规模（每个case的数据条数）
SIZE=1000000

# 每个case重复次数（越大越稳定）
REPEATS=5

# 常规模式专用：int / float / both
DOMAIN="int"

# 1 开启快速模式，0 关闭
QUICK=0

# 1 仅列场景，不跑基准
LIST_SCENARIOS=0

# 仅常规模式可用，1 开启回归门禁（必须是 release 原生构建）
FAIL_ON_REGRESSION=0

# 输出 JSON 文件路径，留空表示不写；建议留空或填 "artifacts/benchmark.json"
JSON_OUTPUT="artifacts/benchmark.json"

# include 过滤；写法是通配符，可写多行（按需增减）
# 例如："rows.join.*"、"flow.*"、"python_builtin/list/*"
INCLUDES=(
  # "rows.join.*"
  # "flow.map_filter/*"
)

# =========================
# 2) 预设（直接改 PRESET）
# =========================
# 可选: competitive_quick / competitive_full / release_quick / release_full / join_only / rows_only / mapfilter_only
PRESET=""

case "${PRESET}" in
  competitive_quick)
    MODE="competitive"
    SIZE=100000
    REPEATS=5
    QUICK=1
    INCLUDES=()
    ;;
  competitive_full)
    MODE="competitive"
    SIZE=100000
    REPEATS=5
    QUICK=0
    INCLUDES=()
    ;;
  release_quick)
    MODE="release"
    SIZE=100000
    REPEATS=5
    QUICK=1
    DOMAIN="int"
    INCLUDES=()
    ;;
  release_full)
    MODE="release"
    SIZE=100000
    REPEATS=5
    QUICK=0
    DOMAIN="int"
    INCLUDES=()
    ;;
  join_only)
    MODE="competitive"
    # include 已经把范围限制为 join；关闭 quick 才会覆盖 inner/left/m:m 三类 join。
    QUICK=0
    SIZE=100000
    REPEATS=5
    INCLUDES=("rows.join.*")
    ;;
  rows_only)
    MODE="competitive"
    QUICK=0
    SIZE=100000
    REPEATS=5
    INCLUDES=("rows.*")
    ;;
  mapfilter_only)
    MODE="competitive"
    QUICK=1
    SIZE=100000
    REPEATS=5
    INCLUDES=("*map_filter*")
    ;;
  "")
    :;;
  *)
    echo "未知 PRESET=${PRESET}，仅支持: competitive_quick, competitive_full, release_quick, release_full, join_only, rows_only, mapfilter_only" >&2
    exit 1
    ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_PATH="${SCRIPT_DIR}/benchmark.py"
SOURCE_DIR="${SCRIPT_DIR}/src"
if [[ -z "${PYTHON_BIN}" ]]; then
  if [[ -x "${SCRIPT_DIR}/.venv/bin/python" ]]; then
    PYTHON_BIN="${SCRIPT_DIR}/.venv/bin/python"
  else
    PYTHON_BIN="python3"
  fi
fi
export PYTHONPATH="${SOURCE_DIR}:${PYTHONPATH-}"

if [[ ! -f "${SCRIPT_PATH}" ]]; then
  echo "找不到 ${SCRIPT_PATH}，请在 fpstreams 仓库根目录运行该脚本" >&2
  exit 1
fi
if [[ ! -d "${SOURCE_DIR}" ]]; then
  echo "找不到源码目录 ${SOURCE_DIR}，请确认路径正确" >&2
  exit 1
fi
if ! "${PYTHON_BIN}" - <<'PY'
import fpstreams
if not hasattr(fpstreams, "item"):
    raise SystemExit(1)
print(f"Using fpstreams from {getattr(fpstreams, '__file__', None)}")
PY
then
  echo "环境变量仍未正确加载 fpstreams 源码；请确认 python 路径和构建环境" >&2
  exit 1
fi

if [[ "${MODE}" == "competitive" ]] && ! "${PYTHON_BIN}" -c 'import numpy, pandas'; then
  echo "competitive 模式需要当前 Python 环境安装 NumPy 和 pandas" >&2
  exit 1
fi

if [[ "${MODE}" != "competitive" && "${MODE}" != "release" ]]; then
  echo "MODE 只能是 competitive 或 release（当前是 ${MODE}）" >&2
  exit 1
fi

ARGS=( "--size" "${SIZE}" "--repeats" "${REPEATS}" )

if [[ "${MODE}" == "competitive" ]]; then
  ARGS+=(--competitive)
else
  ARGS+=(--domain "${DOMAIN}")
  if [[ "${FAIL_ON_REGRESSION}" == "1" ]]; then
    ARGS+=(--fail-on-regression)
  fi
fi

if [[ "${QUICK}" == "1" ]]; then
  ARGS+=(--quick)
fi
if [[ "${LIST_SCENARIOS}" == "1" ]]; then
  ARGS+=(--list-scenarios)
fi

for pattern in "${INCLUDES[@]}"; do
  if [[ -n "${pattern}" ]]; then
    ARGS+=(--include "${pattern}")
  fi
done

if [[ -n "${JSON_OUTPUT}" ]]; then
  if [[ "${JSON_OUTPUT}" == /* ]]; then
    JSON_PATH="${JSON_OUTPUT}"
  else
    JSON_PATH="${SCRIPT_DIR}/${JSON_OUTPUT}"
  fi
  mkdir -p "$(dirname "${JSON_PATH}")"
  ARGS+=(--json "${JSON_PATH}")
fi

echo "即将执行："
printf '  %q' "${PYTHON_BIN}" "${SCRIPT_PATH}" "${ARGS[@]}"
echo
echo

exec "${PYTHON_BIN}" "${SCRIPT_PATH}" "${ARGS[@]}"
