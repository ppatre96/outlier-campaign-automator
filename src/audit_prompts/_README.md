# Audit prompts (vendored from claude-ads)

These markdown files are verbatim copies of sub-skill prompts from
[`AgriciDaniel/claude-ads`](https://github.com/AgriciDaniel/claude-ads)
(MIT-licensed). They're vendored here so the weekly audit workflow on
GitHub Actions can run without depending on a local
`~/.claude/skills/` install.

| File | Source path |
|---|---|
| `ads-meta.md` | `~/.claude/skills/ads-meta/SKILL.md` |
| `ads-google.md` | `~/.claude/skills/ads-google/SKILL.md` |
| `ads-linkedin.md` | `~/.claude/skills/ads-linkedin/SKILL.md` |

## When to refresh

`claude-ads` releases periodically — check
<https://github.com/AgriciDaniel/claude-ads/releases> every few months.
To refresh:

```bash
cp ~/.claude/skills/ads-meta/SKILL.md     src/audit_prompts/ads-meta.md
cp ~/.claude/skills/ads-google/SKILL.md   src/audit_prompts/ads-google.md
cp ~/.claude/skills/ads-linkedin/SKILL.md src/audit_prompts/ads-linkedin.md
```

(First reinstall claude-ads via
`bash ~/.claude/plugins/marketplaces/claude-ads/install.sh` or pull
the marketplace repo directly — the upstream `install.sh` is currently
broken; see [feedback_outlier_meta_production_ids.md] memory for the
manual install steps.)

## License

MIT (inherited from upstream). See `LICENSE` in
`AgriciDaniel/claude-ads` for the full text.
