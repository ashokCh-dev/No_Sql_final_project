#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────
# Interactive launcher for the NASA log ETL framework.
# Wraps main.py with a menu, a spinner during the run, and a paged report.
#
# Usage:  ./run.sh
# ──────────────────────────────────────────────────────────────────────────

set -u

# --- Config ---------------------------------------------------------------
PROJ_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJ_DIR"

PYTHON="${PYTHON:-/home/ashok_ubun/anaconda3/bin/python}"
[[ -x "$PYTHON" ]] || PYTHON="python3"

export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}"
export PYTHONUNBUFFERED=1

# --- Colors ---------------------------------------------------------------
if [[ -t 1 ]]; then
    BOLD=$'\033[1m'; DIM=$'\033[2m'; RESET=$'\033[0m'
    CYAN=$'\033[36m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; RED=$'\033[31m'
    BLUE=$'\033[34m'; MAGENTA=$'\033[35m'
else
    BOLD=""; DIM=""; RESET=""; CYAN=""; GREEN=""; YELLOW=""; RED=""; BLUE=""; MAGENTA=""
fi

hline() { printf '%s\n' "${DIM}──────────────────────────────────────────────────────────────────────${RESET}"; }
banner() {
    clear
    echo "${BOLD}${CYAN}╭──────────────────────────────────────────────────────────────────╮${RESET}"
    echo "${BOLD}${CYAN}│${RESET} ${BOLD}NASA Log ETL  ·  Multi-Pipeline Analytics${RESET}                       ${BOLD}${CYAN}│${RESET}"
    echo "${BOLD}${CYAN}│${RESET} ${DIM}DAS 839 · MapReduce · MongoDB · Pig · Hive${RESET}                       ${BOLD}${CYAN}│${RESET}"
    echo "${BOLD}${CYAN}╰──────────────────────────────────────────────────────────────────╯${RESET}"
    health_line
    echo
}

# --- Service health check -------------------------------------------------
service_status() {
    local hdfs_ok="${RED}down${RESET}"
    local pg_ok="${RED}down${RESET}"
    local mongo_ok="${RED}down${RESET}"
    if jps 2>/dev/null | grep -q NameNode; then hdfs_ok="${GREEN}up${RESET}"; fi
    if /usr/lib/postgresql/16/bin/pg_ctl -D /home/ashok_ubun/pgdata_nasa status >/dev/null 2>&1; then
        pg_ok="${GREEN}up${RESET}"
    fi
    if pgrep -x mongod >/dev/null 2>&1; then mongo_ok="${GREEN}up${RESET}"; fi
    echo -e "$hdfs_ok|$pg_ok|$mongo_ok"
}
health_line() {
    IFS='|' read -r h p m < <(service_status)
    printf "${DIM}Services:${RESET}  Hadoop: %s  ·  PostgreSQL: %s  ·  MongoDB: %s\n" "$h" "$p" "$m"
}

start_services() {
    echo
    hline
    echo "${BOLD}Starting services…${RESET}"
    hline

    # Hadoop
    if jps 2>/dev/null | grep -q NameNode; then
        echo "  ${GREEN}✓${RESET} Hadoop already running"
    else
        echo "  ${CYAN}→${RESET} Starting HDFS + YARN…"
        /home/ashok_ubun/hadoop/sbin/start-dfs.sh 2>&1 | tail -3 | sed 's/^/    /'
        /home/ashok_ubun/hadoop/sbin/start-yarn.sh 2>&1 | tail -3 | sed 's/^/    /'
    fi

    # PostgreSQL
    if /usr/lib/postgresql/16/bin/pg_ctl -D /home/ashok_ubun/pgdata_nasa status >/dev/null 2>&1; then
        echo "  ${GREEN}✓${RESET} PostgreSQL already running"
    else
        echo "  ${CYAN}→${RESET} Starting PostgreSQL…"
        /usr/lib/postgresql/16/bin/pg_ctl -D /home/ashok_ubun/pgdata_nasa \
            -o "-p 5433 -k /home/ashok_ubun/pgdata_nasa/socket" \
            -l /home/ashok_ubun/pgdata_nasa/pg.log start 2>&1 | tail -3 | sed 's/^/    /'
    fi

    # MongoDB (needs sudo since it was started with --fork)
    if pgrep -x mongod >/dev/null 2>&1; then
        echo "  ${GREEN}✓${RESET} MongoDB already running"
    else
        echo "  ${CYAN}→${RESET} Starting MongoDB ${DIM}(may prompt for sudo)${RESET}…"
        sudo mongod --dbpath /var/lib/mongodb \
                    --logpath /var/log/mongodb/mongod.log --fork 2>&1 | tail -3 | sed 's/^/    /'
    fi

    echo
    health_line
}

stop_services() {
    echo
    hline
    echo "${BOLD}Stopping services…${RESET}"
    hline
    echo "  ${CYAN}→${RESET} YARN…"
    /home/ashok_ubun/hadoop/sbin/stop-yarn.sh 2>&1 | tail -2 | sed 's/^/    /'
    echo "  ${CYAN}→${RESET} HDFS…"
    /home/ashok_ubun/hadoop/sbin/stop-dfs.sh  2>&1 | tail -2 | sed 's/^/    /'
    echo "  ${CYAN}→${RESET} PostgreSQL…"
    /usr/lib/postgresql/16/bin/pg_ctl -D /home/ashok_ubun/pgdata_nasa stop 2>&1 | tail -2 | sed 's/^/    /'
    echo "  ${CYAN}→${RESET} MongoDB…"
    mongosh --quiet --eval 'db.getSiblingDB("admin").shutdownServer()' 2>/dev/null \
        || sudo pkill mongod 2>/dev/null
    echo
    health_line
}

# --- Menus ----------------------------------------------------------------
choose_pipeline() {
    echo "${BOLD}Select pipeline:${RESET}"
    echo "  ${CYAN}1${RESET}) MapReduce"
    echo "  ${CYAN}2${RESET}) MongoDB"
    echo "  ${CYAN}3${RESET}) Pig"
    echo "  ${CYAN}4${RESET}) Hive"
    echo "  ${CYAN}5${RESET}) ${BOLD}All four${RESET} (sequential)"
    echo
    local choice
    while true; do
        read -rp "Choice [1-5]: " choice
        case "$choice" in
            1) PIPELINE="mapreduce"; return ;;
            2) PIPELINE="mongodb";   return ;;
            3) PIPELINE="pig";       return ;;
            4) PIPELINE="hive";      return ;;
            5) PIPELINE="all";       return ;;
            *) echo "${RED}Invalid choice.${RESET}" ;;
        esac
    done
}

choose_query() {
    echo
    echo "${BOLD}Select query:${RESET}"
    echo "  ${CYAN}1${RESET}) Q1  · Daily Traffic Summary"
    echo "  ${CYAN}2${RESET}) Q2  · Top 20 Requested Resources"
    echo "  ${CYAN}3${RESET}) Q3  · Hourly Error Analysis"
    echo "  ${CYAN}4${RESET}) ${BOLD}All three${RESET}"
    echo
    local choice
    while true; do
        read -rp "Choice [1-4]: " choice
        case "$choice" in
            1) QUERY="q1";  return ;;
            2) QUERY="q2";  return ;;
            3) QUERY="q3";  return ;;
            4) QUERY="all"; return ;;
            *) echo "${RED}Invalid choice.${RESET}" ;;
        esac
    done
}

choose_inputs() {
    echo
    echo "${BOLD}Select input file(s):${RESET} ${DIM}(each file = one batch)${RESET}"
    local opts=()
    local i=1
    for f in data/NASA_access_log_Jul95 data/NASA_access_log_Aug95 data/test.log; do
        if [[ -f "$f" ]]; then
            local size_mb
            size_mb=$(du -m "$f" | cut -f1)
            local lines
            lines=$(wc -l <"$f")
            printf "  ${CYAN}%d${RESET}) %-40s ${DIM}%6d MB · %10d lines${RESET}\n" \
                   "$i" "$f" "$size_mb" "$lines"
            opts+=("$f")
            i=$((i+1))
        fi
    done
    echo "  ${CYAN}c${RESET}) Custom path(s)"
    echo
    local choice
    read -rp "Pick one option, or multiple separated by spaces (e.g. '1 2'): " choice
    INPUTS=()
    if [[ "$choice" == "c" ]]; then
        read -rp "Enter space-separated paths: " custom
        # shellcheck disable=SC2206
        INPUTS=($custom)
    else
        for n in $choice; do
            if [[ "$n" =~ ^[0-9]+$ ]] && (( n >= 1 && n <= ${#opts[@]} )); then
                INPUTS+=("${opts[$((n-1))]}")
            else
                echo "${RED}Ignoring invalid pick: $n${RESET}"
            fi
        done
    fi
    if (( ${#INPUTS[@]} == 0 )); then
        echo "${RED}No valid inputs selected. Defaulting to data/test.log.${RESET}"
        INPUTS=("data/test.log")
    fi
}

# --- Spinner --------------------------------------------------------------
SPIN_FRAMES=("⠋" "⠙" "⠹" "⠸" "⠼" "⠴" "⠦" "⠧" "⠇" "⠏")

run_with_spinner() {
    local logfile="$1"; shift
    local cmd=("$@")
    : >"$logfile"

    "${cmd[@]}" >"$logfile" 2>&1 &
    local pid=$!

    local start_ts
    start_ts=$(date +%s)
    local i=0
    while kill -0 "$pid" 2>/dev/null; do
        local frame="${SPIN_FRAMES[i % ${#SPIN_FRAMES[@]}]}"
        i=$((i+1))
        local elapsed=$(( $(date +%s) - start_ts ))
        # Show last meaningful progress line if present
        local last
        last=$(grep -E "Batch [0-9]|Running|completed|inserted|Q[123]:|Done\.|Pipeline:" "$logfile" 2>/dev/null \
               | tail -1 | tr -d '\r' | cut -c1-60)
        [[ -z "$last" ]] && last="staging…"
        printf "\r${CYAN}%s${RESET}  ${BOLD}%3ds${RESET}  ${DIM}%-60s${RESET}" \
               "$frame" "$elapsed" "$last"
        sleep 0.15
    done
    wait "$pid"
    local rc=$?
    local elapsed=$(( $(date +%s) - start_ts ))
    if (( rc == 0 )); then
        printf "\r${GREEN}✓${RESET}  Completed in ${BOLD}%ds${RESET}%-60s\n" "$elapsed" " "
    else
        printf "\r${RED}✗${RESET}  Failed after ${BOLD}%ds${RESET} (exit %d)%-50s\n" "$elapsed" "$rc" " "
    fi
    return $rc
}

# --- Run + report ---------------------------------------------------------
run_pipeline() {
    local logfile="/tmp/nasa_etl_run_$$.log"
    echo
    hline
    echo "${BOLD}Running:${RESET} ${MAGENTA}$PIPELINE${RESET}  ${DIM}|${RESET}  query=${YELLOW}$QUERY${RESET}  ${DIM}|${RESET}  inputs=${BLUE}${INPUTS[*]}${RESET}"
    hline

    run_with_spinner "$logfile" \
        "$PYTHON" -u main.py \
            --pipeline "$PIPELINE" \
            --query    "$QUERY" \
            --inputs   "${INPUTS[@]}"
    local rc=$?
    echo
    echo "${DIM}Full log: $logfile${RESET}"

    # Pull all run_ids created in this invocation (handles --pipeline all)
    local run_ids
    run_ids=$(grep -oE "Run ID[:=][[:space:]]*[0-9]+|Run ID:[[:space:]]*[0-9]+" "$logfile" \
              | grep -oE "[0-9]+" | sort -u)

    if [[ -n "$run_ids" ]]; then
        echo
        echo "${BOLD}New run IDs:${RESET} $(echo "$run_ids" | tr '\n' ' ')"
        echo
        read -rp "View report? [Y/n]: " yn
        if [[ "${yn:-y}" =~ ^[Yy] ]]; then
            for rid in $run_ids; do
                show_report "$rid"
            done
        fi
    fi
    return $rc
}

show_report() {
    local rid="$1"
    echo
    hline
    echo "${BOLD}${GREEN}Report for run $rid${RESET}"
    hline
    {
        printf "${DIM}[ press q to return to menu  ·  ↓/↑ scroll  ·  /text search ]${RESET}\n\n"
        "$PYTHON" main.py --report --run-id "$rid" 2>&1
    } | less -R -F -X -P "[ q=quit  ·  ↓/↑=scroll  ·  /=search ]"
}

# --- Extras ---------------------------------------------------------------
list_runs() {
    echo
    hline
    echo "${BOLD}Past runs${RESET}"
    hline
    psql -U nasa_user -d nasa_logs -p 5433 -h /home/ashok_ubun/pgdata_nasa/socket \
         -c "SELECT run_id, pipeline_name, num_batches, total_records,
                    malformed_count, runtime_seconds, started_at::timestamptz(0)
             FROM pipeline_runs ORDER BY run_id DESC LIMIT 20;"
}

view_a_run() {
    list_runs
    local rid
    read -rp "Enter run_id to view (blank to cancel): " rid
    [[ -z "$rid" ]] && return
    show_report "$rid"
}

# --- Main loop ------------------------------------------------------------
main_menu() {
    while true; do
        banner
        echo "${BOLD}Main menu${RESET}"
        echo "  ${CYAN}1${RESET}) Run a pipeline"
        echo "  ${CYAN}2${RESET}) List past runs"
        echo "  ${CYAN}3${RESET}) View report for a run"
        echo "  ${CYAN}4${RESET}) ${BOLD}Start${RESET} services  ${DIM}(Hadoop + PostgreSQL + MongoDB)${RESET}"
        echo "  ${CYAN}5${RESET}) ${BOLD}Stop${RESET}  services"
        echo "  ${CYAN}q${RESET}) Quit"
        echo
        local choice
        read -rp "Choice: " choice
        case "$choice" in
            1)
                # Refuse to run a pipeline if services are obviously down
                IFS='|' read -r h p m < <(service_status)
                if [[ "$h" == *down* || "$p" == *down* ]]; then
                    echo "${RED}HDFS or PostgreSQL is down. Start services first (option 4).${RESET}"
                    sleep 1.5
                    continue
                fi
                choose_pipeline
                choose_query
                choose_inputs
                run_pipeline
                echo; read -rp "Press Enter to return to menu…"
                ;;
            2)
                list_runs
                echo; read -rp "Press Enter to return to menu…"
                ;;
            3)
                view_a_run
                ;;
            4)
                start_services
                echo; read -rp "Press Enter to return to menu…"
                ;;
            5)
                stop_services
                echo; read -rp "Press Enter to return to menu…"
                ;;
            q|Q) echo "${DIM}Bye.${RESET}"; exit 0 ;;
            *) echo "${RED}Invalid choice.${RESET}"; sleep 1 ;;
        esac
    done
}

main_menu
