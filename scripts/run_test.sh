#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_BASE="${ROOT_DIR}/test_logs"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${OUT_BASE}/run_${TIMESTAMP}"
RAW_DIR="${RUN_DIR}/raw"
MASTER_LOG="${RUN_DIR}/run.log"
SUMMARY_MD="${RUN_DIR}/summary.md"
DRY_RUN=0

usage() {
  cat <<'USAGE'
用法:
  scripts/run_test.sh [--dry-run] [--output-dir PATH]

参数:
  --dry-run           仅预览命令，不执行测试。
  --output-dir PATH   指定输出目录（默认: test_logs）。
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
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

RUN_DIR="${OUT_BASE}/run_${TIMESTAMP}"
RAW_DIR="${RUN_DIR}/raw"
MASTER_LOG="${RUN_DIR}/run.log"
SUMMARY_MD="${RUN_DIR}/summary.md"

mkdir -p "$RAW_DIR"

TESTS=(
  "functional|cargo test --lib -- --nocapture"
  "network_integration|cargo test --test network_integration -- --nocapture"
)

declare -A SUITE_STATUS
declare -A SUITE_PASS
declare -A SUITE_FAIL

action_mode="执行"
[[ "$DRY_RUN" -eq 1 ]] && action_mode="预览"

{
  echo "== MyRedis 测试日志 =="
  echo "输出目录: $RUN_DIR"
  echo "模式: $action_mode"
  echo
} | tee "$MASTER_LOG"

extract_counts() {
  local line="$1"
  local pass_count fail_count
  pass_count="$(echo "$line" | sed -n 's/.*ok\. \([0-9][0-9]*\) passed.*/\1/p')"
  fail_count="$(echo "$line" | sed -n 's/.*; \([0-9][0-9]*\) failed.*/\1/p')"
  [[ -z "$pass_count" ]] && pass_count="-"
  [[ -z "$fail_count" ]] && fail_count="-"
  echo "$pass_count|$fail_count"
}

run_suite() {
  local name="$1"
  local cmd="$2"
  local out_file="${RAW_DIR}/${name}.log"

  {
    echo "----------------------------------------"
    echo "套件: $name"
    echo "命令: $cmd"
    echo "----------------------------------------"
  } | tee -a "$MASTER_LOG"

  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[DRY-RUN] skipped execution" | tee "$out_file" | tee -a "$MASTER_LOG" >/dev/null
    SUITE_STATUS["$name"]="SKIP"
    SUITE_PASS["$name"]="-"
    SUITE_FAIL["$name"]="-"
    return
  fi

  set +e
  CARGO_TERM_COLOR=never bash -lc "cd '$ROOT_DIR' && $cmd" >"$out_file" 2>&1
  local status=$?
  set -e

  cat "$out_file" >> "$MASTER_LOG"

  local result_line
  result_line="$(grep -E 'test result:' "$out_file" | tail -n 1 || true)"
  [[ -z "$result_line" ]] && result_line="未捕获到测试汇总"

  local counts
  counts="$(extract_counts "$result_line")"

  SUITE_STATUS["$name"]="$status"
  SUITE_PASS["$name"]="${counts%%|*}"
  SUITE_FAIL["$name"]="${counts##*|}"

  echo "退出码: $status" | tee -a "$MASTER_LOG"
  echo "结果: $result_line" | tee -a "$MASTER_LOG"
}

for item in "${TESTS[@]}"; do
  IFS='|' read -r name cmd <<<"$item"
  run_suite "$name" "$cmd"
done

{
  echo "# 测试汇总"
  echo
  echo "## 覆盖内容"
  echo
  echo "### 功能测试（functional）"
  echo "- RESP 协议解析：完整命令、半包、非法输入、多命令连续解析"
  echo "- 命令解析：参数个数校验、变长参数命令、未知命令处理"
  echo "- 存储行为：String/Hash/List、TTL/EXPIRE、INCR、类型错误处理"
  echo
  echo "### 网络集成测试（network_integration）"
  echo "- 启动真实 TCP 服务，使用 RESP 端到端收发"
  echo "- 多连接共享状态、Pipeline 顺序一致性、参数错误不串扰"
  echo "- 基础命令与数据结构命令（String/Hash/List）"
  echo "- 过期语义、WRONGTYPE、二进制 key/value、空 key/value"
  echo "- EXISTS/DEL 多 key、FLUSHALL/DBSIZE、TYPE 命令、未实现命令优雅报错"
  echo
  echo "## 执行结果"
  echo
  echo "| 测试套件 | 退出码 | 通过 | 失败 | 原始日志 |"
  echo "| --- | --- | --- | --- | --- |"
  printf '| %s | %s | %s | %s | [%s](%s) |\n' \
    "functional" "${SUITE_STATUS[functional]:--}" "${SUITE_PASS[functional]:--}" "${SUITE_FAIL[functional]:--}" \
    "functional.log" "raw/functional.log"
  printf '| %s | %s | %s | %s | [%s](%s) |\n' \
    "network_integration" "${SUITE_STATUS[network_integration]:--}" "${SUITE_PASS[network_integration]:--}" "${SUITE_FAIL[network_integration]:--}" \
    "network_integration.log" "raw/network_integration.log"
  echo
  echo "## 日志文件"
  echo
  echo "- 全量日志: [run.log](run.log)"
  echo "- 功能测试原始日志: [raw/functional.log](raw/functional.log)"
  echo "- 网络集成原始日志: [raw/network_integration.log](raw/network_integration.log)"
} > "$SUMMARY_MD"

echo "summary: $SUMMARY_MD" | tee -a "$MASTER_LOG"
echo "done"
