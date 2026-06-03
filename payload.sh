#!/bin/bash
# =============================================================================
# payload.sh — Deadman Switch Payload Script (Example Skeleton)
# =============================================================================
# The payload script can be ANY executable script or binary. When the deadman
# timer expires without a disarm, this file is run directly. What it does is
# entirely up to you — send emails, trigger remote commands, publish files,
# wipe keys, notify contacts, or anything else.
#
# This file is a skeleton example. It ships with logic for sending queued
# email messages and running a FastMail-based canary test, but you are free
# to replace, extend, or completely rewrite it for your own use case.
#
# ---- Test Mode (--test flag) ------------------------------------------------
# The Deadman Switch cluster calls this script with --test on the first of
# every month (sequentially across all nodes) to verify the payload is still
# functional. Your test implementation should be non-destructive and mirror
# your real payload logic as closely as possible.
#
# The Go binary parses the following output lines to build the Telegram report:
#   STEP:<phase>:<name>:PASS
#   STEP:<phase>:<name>:FAIL[:<optional detail>]
#   STEP:<phase>:<name>:SKIP[:<optional detail>]
#   SUMMARY:PASS  or  SUMMARY:FAIL
#
# If you don't need a monthly test, have the --test branch print SUMMARY:PASS
# and exit 0. The cluster requires the flag to be handled.
#
# ---- Requirements (for this example skeleton) --------------------------------
#   Packages: curl, jq
#   Config:   payload.conf must exist alongside this script
#
# ---- Permissions -------------------------------------------------------------
#   chmod +x /var/lib/deadman/payload.sh
#   chown deadman:deadman /var/lib/deadman/payload.sh
# =============================================================================

set -euo pipefail


# Load Configuration
currentDir=$(dirname "$(realpath "$0")")
source "${currentDir}/payload.conf"

# Dependencies Check
for cmd in jq curl; do
    if ! command -v $cmd &> /dev/null; then
        echo "Error: Required command '$cmd' not found."
        exit 1
    fi
done


# =============================================================================
# SHARED FUNCTIONS
# =============================================================================

# send_email_file — Send a single formatted message file via SMTP.
# Message files use a simple header format:
#
#   TO: recipient@example.com, other@example.com
#   SUBJECT: Your subject line
#   BODY_START
#   Body text goes here.
#
# Returns curl's exit code.
send_email_file() {
    local msgFile="$1"
    local recipient_str subject_str body startLine

    recipient_str=$(grep "^TO:"       "${msgFile}" | head -n 1 | cut -d' ' -f2-)
    subject_str=$(grep  "^SUBJECT:"  "${msgFile}" | head -n 1 | cut -d' ' -f2-)
    startLine=$(grep -n "^BODY_START" "${msgFile}" | cut -d: -f1)
    [[ -n "${startLine}" ]] && body=$(tail -n +$((startLine + 1)) "${msgFile}")

    [[ -z "${recipient_str}" || -z "${subject_str}" ]] && return 1

    local curl_cmd=(
        curl --url "${smtpUrl}"
        --ssl-reqd
        --mail-from "${smtpUser}"
        --user "${smtpUser}:${smtpPassword}"
        --silent --show-error
    )

    IFS=',' read -ra ADDRS <<< "${recipient_str}"
    for addr in "${ADDRS[@]}"; do
        local clean_addr
        clean_addr=$(echo "$addr" | xargs)
        [[ -n "$clean_addr" ]] && curl_cmd+=(--mail-rcpt "$clean_addr")
    done

    printf 'From: %s\nTo: %s\nSubject: %s\n\n%s' \
        "${smtpUser}" "${recipient_str}" "${subject_str}" "${body}" | \
        "${curl_cmd[@]}" --upload-file -
}


# =============================================================================
# TEST MODE
# =============================================================================
# The Deadman Switch cluster calls this script with --test on the first of
# every month to verify the payload is still functional. Your implementation
# should be a non-destructive dry run that mirrors your real payload logic.
#
# Output format — the Go binary parses these lines to build the Telegram report:
#   STEP:<phase>:<name>:PASS
#   STEP:<phase>:<name>:FAIL[:<optional detail>]
#   STEP:<phase>:<name>:SKIP[:<optional detail>]
#   SUMMARY:PASS  or  SUMMARY:FAIL
# =============================================================================
if [[ "$1" == "--test" ]]; then

    OVERALL_PASS=true

    log_step() {
        local phase="$1" step="$2" result="$3" detail="${4:-}"
        local line="STEP:${phase}:${step}:${result}"
        [[ -n "$detail" ]] && line="${line}:${detail}"
        echo "$line"
        [[ "$result" == "FAIL" ]] && OVERALL_PASS=false
        return 0
    }

    echo "=== Deadman Switch Monthly Test: $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="


    # -------------------------------------------------------------------------
    # PHASE 1: SETUP
    # Create any ephemeral test resources needed to verify your payload.
    # -------------------------------------------------------------------------
    echo ""
    echo "--- Phase 1: Setup ---"

    # Example: verify a remote host is reachable
    # ping -c 1 "yourhost.example.com" &>/dev/null \
    #     && log_step "Setup" "RemoteReachable" "PASS" \
    #     || log_step "Setup" "RemoteReachable" "FAIL"

    # ---- Discover FastMail address via JMAP session ----
    SETUP_TEST_EMAIL=false
    jmap_session_json=$(curl -sf \
        -H "Authorization: Bearer ${fastmailToken}" \
        "https://api.fastmail.com/jmap/session" 2>/dev/null)
    jmap_api_url=$(echo    "$jmap_session_json" | jq -r '.apiUrl // empty')
    jmap_account_id=$(echo "$jmap_session_json" | jq -r '.primaryAccounts["urn:ietf:params:jmap:mail"] // empty')
    fastmail_addr=$(echo   "$jmap_session_json" | jq -r '.username // empty')
    [[ -z "$fastmail_addr" ]] && fastmail_addr="${fastmailAddress:-}"

    if [[ -n "$fastmail_addr" ]]; then
        log_step "Setup" "ResolveMailAddress" "PASS"
    else
        log_step "Setup" "ResolveMailAddress" "FAIL" "Could not determine FastMail address"
    fi

    # ---- Create canary email ----
    mkdir -p "${msgDirTest}"
    test_subject="Deadman Switch Canary - $(date +%Y-%m)"
    test_email_file="${msgDirTest}/canary_$(date +%Y%m).txt"

    if [[ -n "$fastmail_addr" ]]; then
        cat > "$test_email_file" <<EMAILEOF
TO: ${fastmail_addr}
SUBJECT: ${test_subject}
BODY_START
This is an automated canary message from the Deadman Switch system.
Test run: $(date -u +%Y-%m-%dT%H:%M:%SZ)

If you are reading this, the email delivery pipeline is functioning correctly.
This message will be deleted automatically upon verified receipt.
EMAILEOF
        [[ -f "$test_email_file" ]] \
            && { log_step "Setup" "CreateTestEmail" "PASS"; SETUP_TEST_EMAIL=true; } \
            || log_step   "Setup" "CreateTestEmail" "FAIL"
    else
        log_step "Setup" "CreateTestEmail" "SKIP" "No FastMail address available"
    fi


    # -------------------------------------------------------------------------
    # PHASE 2: VERIFY
    # Confirm everything created in Setup actually exists before tearing down.
    # -------------------------------------------------------------------------
    echo ""
    echo "--- Phase 2: Verify ---"

    # Add your own verification steps here, following the same pattern:
    # log_step "Verify" "StepName" "PASS"
    # log_step "Verify" "StepName" "FAIL" "reason"


    # -------------------------------------------------------------------------
    # PHASE 3: TEARDOWN
    # Destroy the test resources in the same way the live payload would.
    # -------------------------------------------------------------------------
    echo ""
    echo "--- Phase 3: Teardown ---"

    # Add your own teardown steps here.


    # -------------------------------------------------------------------------
    # PHASE 4: EMAIL TEST
    # Send the canary, poll FastMail to confirm receipt, then delete it.
    # -------------------------------------------------------------------------
    echo ""
    echo "--- Phase 4: Email Test ---"

    if [[ "$SETUP_TEST_EMAIL" == "true" && -n "$jmap_api_url" && -n "$jmap_account_id" ]]; then
        send_before_ts=$(date -u -d '30 seconds ago' +%Y-%m-%dT%H:%M:%SZ)

        if send_email_file "$test_email_file"; then
            log_step "Email" "Send" "PASS"

            echo "   Polling FastMail for receipt (up to 5 min)..."
            email_id=""
            for i in $(seq 1 30); do
                sleep 10
                query_resp=$(curl -s -X POST "${jmap_api_url}" \
                    -H "Authorization: Bearer ${fastmailToken}" \
                    -H "Content-Type: application/json" \
                    -d "$(jq -n \
                        --arg accId  "${jmap_account_id}" \
                        --arg subj   "${test_subject}" \
                        --arg from   "${smtpUser}" \
                        --arg after  "${send_before_ts}" \
                        '{
                          "using": ["urn:ietf:params:jmap:core","urn:ietf:params:jmap:mail"],
                          "methodCalls": [["Email/query", {
                            "accountId": $accId,
                            "filter": { "subject": $subj, "from": $from, "after": $after },
                            "limit": 1
                          }, "0"]]
                        }')" 2>/dev/null)
                email_id=$(echo "$query_resp" | jq -r '.methodResponses[0][1].ids[0] // empty')
                [[ -n "$email_id" ]] && { log_step "Email" "Receipt" "PASS" "Arrived after ~$((i * 10))s"; break; }
            done

            if [[ -z "$email_id" ]]; then
                log_step "Email" "Receipt" "FAIL" "Not received within 5 minutes"
            else
                del_resp=$(curl -s -X POST "${jmap_api_url}" \
                    -H "Authorization: Bearer ${fastmailToken}" \
                    -H "Content-Type: application/json" \
                    -d "$(jq -n \
                        --arg accId "${jmap_account_id}" \
                        --arg eid   "${email_id}" \
                        '{
                          "using": ["urn:ietf:params:jmap:core","urn:ietf:params:jmap:mail"],
                          "methodCalls": [["Email/set", {
                            "accountId": $accId,
                            "destroy": [$eid]
                          }, "0"]]
                        }')" 2>/dev/null)
                destroyed=$(echo "$del_resp" | jq -r '.methodResponses[0][1].destroyed[0] // empty')
                [[ -n "$destroyed" ]] \
                    && log_step "Email" "CanaryDeleted" "PASS" \
                    || log_step "Email" "CanaryDeleted" "FAIL" "JMAP destroy returned no confirmation"
            fi
        else
            log_step "Email" "Send" "FAIL" "SMTP curl returned non-zero"
        fi

        rm -f "$test_email_file"
    else
        log_step "Email" "Send" "SKIP" "Setup failed or JMAP session unavailable"
    fi
    rm -rf "${msgDirTest}"


    # -------------------------------------------------------------------------
    # PHASE 5: YOUTUBE LINK CHECK
    # Scan all queued message files for YouTube URLs and verify each video is
    # still publicly accessible.
    # -------------------------------------------------------------------------
    echo ""
    echo "--- Phase 5: YouTube Link Check ---"

    yt_urls=$(grep -rhoP \
        'https?://(www\.)?youtube\.com/watch\?[^\s]*v=[A-Za-z0-9_-]+[^\s]*' \
        "${msgDir}/"*.txt 2>/dev/null | sort -u || true)

    if [[ ! -d "${msgDir}" ]]; then
        log_step "YouTube" "ScanMsgDir" "SKIP" "msgDir does not exist: ${msgDir}"
    elif [[ -z "$yt_urls" ]]; then
        echo "   No YouTube URLs found in ${msgDir}."
    else
        while IFS= read -r url; do
            vid=$(echo "$url" | grep -oP '(?<=v=)[A-Za-z0-9_-]+' | head -1)
            [[ -z "$vid" ]] && continue

            status=$(curl -s -o /dev/null -w "%{http_code}" \
                "https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v=${vid}&format=json")

            [[ "$status" == "200" ]] \
                && log_step "YouTube" "${vid}" "PASS" \
                || log_step "YouTube" "${vid}" "FAIL" "HTTP ${status}"
        done <<< "$yt_urls"
    fi


    # -------------------------------------------------------------------------
    echo ""
    [[ "$OVERALL_PASS" == "true" ]] && echo "SUMMARY:PASS" || echo "SUMMARY:FAIL"
    exit 0
fi


# =============================================================================
# PAYLOAD MODE
# Only runs when the deadman timer expires without a disarm.
# =============================================================================

# ---------------------------------------------------------
# Part 1: Your custom destructive/notification logic here.
# ---------------------------------------------------------
echo "[1/2] Running payload actions..."

# Example:
# ssh -i /var/lib/deadman/ssh_key user@yourhost "bash -s" <<'EOF'
#     echo "Doing something irreversible..."
# EOF


# ---------------------------------------------------------
# Part 2: Email Queue
# Sends all .txt files in msgDir. Files are NOT deleted
# after sending so they can be re-sent if delivery fails.
# ---------------------------------------------------------
echo "[2/2] Processing email queue..."

if compgen -G "${msgDir}/*.txt" > /dev/null; then
    for msgFile in "${msgDir}"/*.txt; do
        recipient_str=$(grep "^TO:" "${msgFile}" | head -n 1 | cut -d' ' -f2-)
        if send_email_file "$msgFile"; then
            echo "   Sent to: ${recipient_str}"
        else
            echo "   Failed:  ${recipient_str}"
        fi
    done
else
    echo "   No messages found in ${msgDir}."
fi

echo "--- Payload Execution Complete ---"
