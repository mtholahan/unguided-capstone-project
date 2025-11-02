#!/usr/bin/env bash
# ===============================================================
#  Git Tag Audit Script (v3)
#  Purpose: Summarize and optionally convert lightweight tags
#  Author: Mark Holahan (unguided-capstone-project)
#  Flags:
#    --convert   → Convert all lightweight tags to annotated tags
#    --dry-run   → Show what would be converted (no changes)
# ===============================================================

convert_mode=false
dry_run=false

for arg in "$@"; do
  case $arg in
    --convert) convert_mode=true ;;
    --dry-run) dry_run=true ;;
    *) ;;
  esac
done

echo "🔍 Auditing Git tags in repo: $(basename "$(git rev-parse --show-toplevel)")"
echo "==============================================================="
echo

# Ensure we're in a Git repo
if ! git rev-parse --git-dir > /dev/null 2>&1; then
  echo "❌ Not a Git repository. Run from inside your repo."
  exit 1
fi

git fetch --tags > /dev/null 2>&1

# Header
printf "%-35s | %-10s | %-20s | %-25s\n" "TAG NAME" "TYPE" "TAGGER" "DATE"
printf -- "--------------------------------------------------------------------------------------------------------\n"

lightweight_tags=()

for tag in $(git tag --sort=creatordate); do
  obj_type=$(git cat-file -t "$tag" 2>/dev/null)
  if [[ "$obj_type" == "tag" ]]; then
    tagger=$(git for-each-ref --format='%(taggername)' refs/tags/"$tag")
    date=$(git for-each-ref --format='%(taggerdate:short)' refs/tags/"$tag")
    msg=$(git for-each-ref --format='%(contents)' refs/tags/"$tag" | head -n 3 | tr -d '\r')

    printf "%-35s | %-10s | %-20s | %-25s\n" "$tag" "annotated" "$tagger" "$date"
    echo "  📝 $msg"
  else
    commit_hash=$(git rev-list -n 1 "$tag")
    author=$(git log -1 --pretty=format:'%an' "$commit_hash")
    date=$(git log -1 --pretty=format:'%ad' --date=short "$commit_hash")
    printf "%-35s | %-10s | %-20s | %-25s\n" "$tag" "lightweight" "$author" "$date"
    echo "  ⚠️  No annotation (lightweight tag)"
    lightweight_tags+=("$tag")
  fi
  echo
done

echo "==============================================================="
echo "✅ Tag audit complete."
echo

# Handle lightweight tag conversions
if [[ "${#lightweight_tags[@]}" -gt 0 ]]; then
  echo "🧩 Found ${#lightweight_tags[@]} lightweight tag(s)."

  # DRY-RUN MODE
  if [[ "$dry_run" == true ]]; then
    echo "🧪 Dry-run mode — showing proposed conversions:"
    for tag in "${lightweight_tags[@]}"; do
      commit_hash=$(git rev-list -n 1 "$tag")
      commit_date=$(git log -1 --pretty=format:'%ad' --date=short "$commit_hash")
      echo "  ➡️  Would convert: $tag → annotated ('Auto-converted from lightweight tag (no message) — original date: $commit_date')"
    done

  # CONVERT MODE
elif [[ "$convert_mode" == true ]]; then
  echo "⚙️  Converting lightweight tags to annotated..."
  for tag in "${lightweight_tags[@]}"; do
    commit_hash=$(git rev-list -n 1 "$tag")
    # Get the commit’s original author date in ISO (YYYY-MM-DD)
    commit_date=$(git log -1 --pretty=format:'%ad' --date=short "$commit_hash")

    msg="Auto-converted from lightweight tag (no message) — original date: $commit_date"

    echo "  ➡️  Converting: $tag → annotated (original date: $commit_date)..."

    # Recreate the tag with the annotation message including original date
    git tag -d "$tag" > /dev/null 2>&1
    git tag -a "$tag" -m "$msg" "$commit_hash"
    sleep 0.1
  done
  echo
  echo "🚀 Conversion complete. Run ./git_tag_audit.sh again to verify."

  # NO FLAGS PROVIDED
  else
    echo "💡 Tip: run './git_tag_audit.sh --dry-run' to preview or './git_tag_audit.sh --convert' to apply."
  fi

else
  echo "✅ No lightweight tags found — all tags are annotated."
fi
