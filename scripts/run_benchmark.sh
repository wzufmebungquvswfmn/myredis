#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_BASE="${ROOT_DIR}/benchmark_logs"
MODE="quick"
DRY_RUN=0

usage() {
  cat <<'USAGE'
用法:
  scripts/run_benchmark.sh [--quick|--full] [--dry-run] [--output-dir PATH]

参数:
  --quick             快速档（默认）：更短时长，适合日常回归。
  --full              完整档：更大请求量，适合最终报告。
  --dry-run           仅预览命令，不执行。
  --output-dir PATH   指定输出目录（默认: benchmark_logs）。
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --quick)
      MODE="quick"
      shift
      ;;
    --full)
      MODE="full"
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --output-dir)
      [[ $# -lt 2 ]] && { echo "--output-dir 需要路径" >&2; exit 1; }
      OUT_BASE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "未知参数: $1" >&2
      usage
      exit 1
      ;;
  esac
done

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${OUT_BASE}/run_${TIMESTAMP}"
RAW_DIR="${RUN_DIR}/raw"
SUMMARY_MD="${RUN_DIR}/summary.md"
MASTER_LOG="${RUN_DIR}/run.log"

mkdir -p "$RAW_DIR"

if ! command -v redis-benchmark >/dev/null 2>&1; then
  echo "redis-benchmark 不在 PATH 中" >&2
  exit 1
fi

if [[ "$MODE" == "full" ]]; then
  N_SMALL=100000
  N_BIG=1000000
  CONC_HIGH=200
else
  N_SMALL=10000
  N_BIG=50000
  CONC_HIGH=100
fi

# 统一场景清单
readarray -t SCENARIOS <<SCENARIOS
1|默认配置|-c 50 -n ${N_SMALL} -t set,get -q|基础吞吐
2|默认+1KB value|-c 50 -n ${N_SMALL} -t set,get -d 1024 -q|大 value 影响
3|Pipeline 1|-c 50 -n ${N_SMALL} -t set,get -P 1 -q|无 pipeline 基线
4|Pipeline 16|-c 50 -n ${N_SMALL} -t set,get -P 16 -q|常用 pipeline
5|Pipeline 64|-c 50 -n ${N_SMALL} -t set,get -P 64 -q|高 pipeline
6|Pipeline 96|-c 50 -n ${N_SMALL} -t set,get -P 96 -q|极限 pipeline
7|高并发 ${CONC_HIGH}|-c ${CONC_HIGH} -n ${N_BIG} -t set,get -q|并发压力
8|随机 Key 基础|-c 50 -n ${N_BIG} -r 1000000 -t set,get -q|打散 key 空间
9|随机 Key + P16|-c 50 -n ${N_BIG} -r 1000000 -P 16 -t set,get -q|组合测试
10|随机 Key + P16 + 1KB|-c 50 -n ${N_BIG} -r 1000000 -P 16 -d 1024 -t set,get -q|综合场景
11|混合命令 set/get/incr|-c 50 -n ${N_SMALL} -t set,get,incr -q|多命令混合
12|Hash 命令 hset/hget|-c 50 -n ${N_SMALL} -t hset,hget -q|Hash 性能
13|List 命令 lpush/lpop|-c 50 -n ${N_SMALL} -t lpush,lpop -q|List 性能
14|Pipeline 1 + 1KB|-c 50 -n ${N_SMALL} -t set,get -P 1 -d 1024 -q|历史补充
15|Pipeline 16 + 1KB|-c 50 -n ${N_SMALL} -t set,get -P 16 -d 1024 -q|历史补充
16|Pipeline 64 + 1KB|-c 50 -n ${N_SMALL} -t set,get -P 64 -d 1024 -q|历史补充
SCENARIOS

PORTS=(6379 6380)

declare -A SC_NAME
declare -A METRIC_6379
declare -A METRIC_6380
declare -A FIRST_QPS_6379
declare -A FIRST_QPS_6380
declare -A NOTE_6379
declare -A NOTE_6380

extract_metrics() {
  local file="$1"
  local text=""
  local note=""

  mapfile -t metric_lines < <(tr '\r' '\n' < "$file" | grep -E '^[A-Z]+: [0-9.]+ requests per second$' || true)

  if [[ ${#metric_lines[@]} -eq 0 ]]; then
    printf '未提取到吞吐\t\t未提取到有效吞吐\n'
    return
  fi

  local first_num=""
  local max_num=0
  local min_num=999999999999

  for line in "${metric_lines[@]}"; do
    local cmd num
    cmd="$(echo "$line" | sed -E 's/^([A-Z]+):.*/\1/')"
    num="$(echo "$line" | sed -E 's/^[A-Z]+: ([0-9.]+).*/\1/')"

    if [[ -z "$first_num" ]]; then
      first_num="$num"
    fi

    text+="${cmd} ${num} rps ; "

    awk -v n="$num" -v cur="$max_num" 'BEGIN { exit !(n > cur) }' && max_num="$num"
    awk -v n="$num" -v cur="$min_num" 'BEGIN { exit !(n < cur) }' && min_num="$num"
  done

  text="${text% ; }"

  if awk -v min="$min_num" -v max="$max_num" 'BEGIN { exit !((min > 0) && (max/min >= 5.0)) }'; then
    note="同场景命令吞吐差距较大"
  fi

  printf '%s\t%s\t%s\n' "$text" "$first_num" "$note"
}

compare_ratio() {
  local official="$1"
  local mine="$2"
  if [[ -z "$official" || -z "$mine" ]]; then
    echo "-"
    return
  fi
  awk -v o="$official" -v m="$mine" 'BEGIN { if (o <= 0) print "-"; else printf "%.1f%%", (m/o)*100; }'
}

{
  echo "== MyRedis 性能测试日志 =="
  echo "输出目录: $RUN_DIR"
  echo "模式: $MODE"
  echo "端口: 6379(官方) vs 6380(myredis)"
  echo
} | tee "$MASTER_LOG"

run_case() {
  local id="$1"
  local name="$2"
  local args="$3"
  local note="$4"
  local port="$5"

  local out_file="${RAW_DIR}/s${id}_p${port}.log"
  local -a cmd=(redis-benchmark -p "$port")
  local -a arg_arr=()
  read -r -a arg_arr <<<"$args"
  cmd+=("${arg_arr[@]}")

  {
    echo "----------------------------------------"
    echo "场景 #$id | $name | 端口 $port"
    echo "参数: $args"
    echo "说明: $note"
    echo "命令: ${cmd[*]}"
    echo "----------------------------------------"
  } | tee -a "$MASTER_LOG"

  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[DRY-RUN] skipped execution" | tee "$out_file" | tee -a "$MASTER_LOG" >/dev/null
    if [[ "$port" == "6379" ]]; then
      METRIC_6379["$id"]="dry-run"
    else
      METRIC_6380["$id"]="dry-run"
    fi
    return
  fi

  set +e
  "${cmd[@]}" >"$out_file" 2>&1
  local status=$?
  set -e

  cat "$out_file" >> "$MASTER_LOG"

  local parsed text first note_text
  parsed="$(extract_metrics "$out_file")"
  IFS=$'\t' read -r text first note_text <<<"$parsed"

  if [[ "$port" == "6379" ]]; then
    METRIC_6379["$id"]="$text"
    FIRST_QPS_6379["$id"]="$first"
    NOTE_6379["$id"]="$note_text"
  else
    METRIC_6380["$id"]="$text"
    FIRST_QPS_6380["$id"]="$first"
    NOTE_6380["$id"]="$note_text"
  fi

  echo "退出码: $status" | tee -a "$MASTER_LOG"
  echo "摘要: $text" | tee -a "$MASTER_LOG"
  if [[ -n "$note_text" ]]; then
    echo "提示: $note_text" | tee -a "$MASTER_LOG"
  fi
}

for row in "${SCENARIOS[@]}"; do
  IFS='|' read -r id name args note <<<"$row"
  SC_NAME["$id"]="$name"

  for port in "${PORTS[@]}"; do
    run_case "$id" "$name" "$args" "$note" "$port"
  done
done

{
  echo "# 性能测试汇总"
  echo
  echo "## 场景说明"
  echo "- 统一场景清单: ${#SCENARIOS[@]} 个（已去重）"
  echo "- 对照端口: 6379(官方) / 6380(myredis)"
  echo "- 运行模式: $MODE"
  echo
  echo "## 关键结果"
  echo
  echo "| 场景 | 6379 摘要 | 6380 摘要 | 首条吞吐对比(6380/6379) |"
  echo "| --- | --- | --- | --- |"

  for id in $(seq 1 ${#SCENARIOS[@]}); do
    m6379="${METRIC_6379[$id]:-未执行}"
    m6380="${METRIC_6380[$id]:-未执行}"
    q6379="${FIRST_QPS_6379[$id]:-}"
    q6380="${FIRST_QPS_6380[$id]:-}"
    ratio="$(compare_ratio "$q6379" "$q6380")"
    flag=""
    if [[ -n "${NOTE_6379[$id]:-}" || -n "${NOTE_6380[$id]:-}" ]]; then
      flag=" ⚠"
    fi
    printf '| #%s %s%s | %s | %s | %s |\n' "$id" "${SC_NAME[$id]}" "$flag" "$m6379" "$m6380" "$ratio"
  done

  echo
  echo "## 日志文件"
  echo "- 全量日志: [run.log](run.log)"
  echo "- 全部原始输出: [raw](raw)"
  echo
  echo "## 注释"
  echo "- 标记 ⚠ 表示同场景内不同命令吞吐差距较大，建议单独重跑该场景并取中位值。"
} > "$SUMMARY_MD"

echo "summary: $SUMMARY_MD" | tee -a "$MASTER_LOG"
echo "done"
