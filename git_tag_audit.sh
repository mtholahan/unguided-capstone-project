#!/usr/bin/env bash
# ===============================================================
#  Git Tag Audit Script (v2)
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
    msg=$(git for-each-ref --format='%(contents:subject)' refs/tags/"$tag")
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
  if [[ "$dry_run" == true ]]; then
    echo "🧪 Dry-run mode — showing proposed conversions:"
    for tag in "${lightweight_tags[@]}"; do
      echo "  ➡️  Would convert: $tag → annotated ('Auto-converted from lightweight tag (no message)')"
    done
  elif [[ "$convert_mode" == true ]]; then
    echo "⚙️  Converting lightweight tags to annotated..."
    for tag in "${lightweight_tags[@]}"; do
      commit_hash=$(git rev-list -n 1 "$tag")
      msg="Auto-converted from lightweight tag (no message)"
      echo "  ➡️  Converting: $tag → annotated..."
      git tag -a "$tag" -m "$msg" "$commit_hash" 2>/dev/null
    done
    echo
    echo "🚀 Conversion complete. Run ./git_tag_audit.sh again to verify."
  else
    echo "💡 Tip: run './git_tag_audit.sh --dry-run' to preview or './git_tag_audit.sh --convert' to apply."
  fi
else
  echo "✅ No lightweight tags found — all tags are annotated."
fi
