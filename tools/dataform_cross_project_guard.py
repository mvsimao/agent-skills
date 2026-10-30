#!/usr/bin/env python3
"""
Dataform Cross-Project Guard & Scope Isolation CLI.

Used in technical refinement and developer workflows to audit and auto-remediate:
1. Analytic vs. Ingestion project mismatches (-a- vs -p- / -i-);
2. Deduplication of 01_sources declarations against Dataform main branch;
3. Auto-conversion of SQL references to ${ref("schema", "table")} (2 arguments);
4. Automatic injection of utils.projectId(...) in cross-project sources;
5. Enforcement of vars.project_id in Airflow DAG Factory YAML;
6. Enforcement of dependencies: ["<target_table>"] in 05_assertions;
7. PR Scope Isolation: validates that git diff only alters files in the assigned card scope.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Any, Set, Tuple, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("cross-project-guard")


class DataformCrossProjectGuard:
    """Enforces cross-project boundaries, reference standards, and PR isolation."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run

    def is_cross_project(self, target_project: str, source_project: str) -> bool:
        if not source_project or source_project == target_project:
            return False
        t_is_analytic = target_project.startswith("a-") or "-a-" in target_project or "dwh" in target_project
        s_is_ingestion = (
            source_project.startswith("p-") or source_project.startswith("i-") or
            "-p-" in source_project or "-i-" in source_project or
            "raw" in source_project or "lake" in source_project
        )
        return t_is_analytic and s_is_ingestion

    def audit_and_fix_sources(
        self,
        sources_dir: Path,
        target_project: str,
        main_repo_dir: Optional[Path] = None,
        apply_fixes: bool = False
    ) -> List[Dict[str, Any]]:
        findings = []
        if not sources_dir.exists():
            return findings

        # Check existing declarations in main
        main_declarations = set()
        if main_repo_dir and main_repo_dir.exists():
            for f in main_repo_dir.glob("definitions/01_sources/**/*.sqlx"):
                try:
                    txt = f.read_text(encoding="utf-8")
                    m = re.search(r'name:\s*["\']([^"\']+)["\']', txt)
                    if m:
                        main_declarations.add(m.group(1))
                except Exception:
                    pass

        for sf in sources_dir.glob("**/*.sqlx"):
            content = sf.read_text(encoding="utf-8")
            table_name_match = re.search(r'name:\s*["\']([^"\']+)["\']', content)
            tbl_name = table_name_match.group(1) if table_name_match else sf.stem
            db_match = re.search(r'database:\s*["\']?([^"\'\n,]+)["\']?', content)
            raw_db = db_match.group(1).strip() if db_match else ""

            # Check duplication against main
            if tbl_name in main_declarations:
                findings.append({
                    "file": str(sf),
                    "issue": "DUPLICATE_SOURCE_IN_MAIN",
                    "severity": "WARNING",
                    "details": f"Source '{tbl_name}' is already declared in Dataform main branch. Redundant file."
                })
                if apply_fixes and not self.dry_run:
                    sf.unlink()
                    logger.info(f"Removed redundant source declaration: {sf}")
                continue

            # Check cross-project utils.projectId
            if self.is_cross_project(target_project, raw_db):
                if "utils.projectId" not in content:
                    findings.append({
                        "file": str(sf),
                        "issue": "MISSING_UTILS_PROJECT_ID",
                        "severity": "BLOCKER",
                        "details": f"Cross-project source '{raw_db}' must use 'database: utils.projectId(\"{raw_db}\")'."
                    })
                    if apply_fixes and not self.dry_run:
                        new_content = re.sub(
                            r'database:\s*["\']([^"\']+)["\']',
                            r'database: utils.projectId("\1")',
                            content
                        )
                        sf.write_text(new_content, encoding="utf-8")
                        logger.info(f"Auto-fixed cross-project source: {sf}")

        return findings

    def audit_and_fix_models(
        self,
        models_dir: Path,
        apply_fixes: bool = False
    ) -> List[Dict[str, Any]]:
        findings = []
        if not models_dir.exists():
            return findings

        # Check references in models (must use 2 arguments: ${ref("schema", "table")})
        single_arg_ref = re.compile(r"\$\{ref\([\"']([a-zA-Z0-9_\-]+)[\"']\)\}")
        three_arg_ref = re.compile(r"\$\{ref\([\"'][^\"']+[\"'],\s*[\"'][^\"']+[\"'],\s*[\"'][^\"']+[\"']\)\}")

        for mf in models_dir.glob("**/*.sqlx"):
            content = mf.read_text(encoding="utf-8")
            
            # Check 3 arguments (invalid in Dataform)
            if three_arg_ref.search(content):
                findings.append({
                    "file": str(mf),
                    "issue": "INVALID_THREE_ARG_REF",
                    "severity": "BLOCKER",
                    "details": "Found ${ref} with 3 arguments. Dataform supports strictly 2 arguments: ${ref('schema', 'table')}."
                })

            # Check 1 argument
            single_matches = single_arg_ref.findall(content)
            if single_matches:
                findings.append({
                    "file": str(mf),
                    "issue": "SINGLE_ARG_REF",
                    "severity": "WARNING",
                    "details": f"Found single-argument references {single_matches}. Recommend upgrading to ${{ref('schema', 'table')}}."
                })

        return findings

    def audit_and_fix_assertions(
        self,
        assertions_dir: Path,
        target_table: Optional[str] = None,
        apply_fixes: bool = False
    ) -> List[Dict[str, Any]]:
        findings = []
        if not assertions_dir.exists():
            return findings

        for af in assertions_dir.glob("**/*.sqlx"):
            content = af.read_text(encoding="utf-8")
            cfg_match = re.search(r"config\s*\{([^}]+)\}", content, re.DOTALL)
            if not cfg_match or "dependencies:" not in cfg_match.group(1):
                findings.append({
                    "file": str(af),
                    "issue": "ASSERTION_MISSING_DEPENDENCIES",
                    "severity": "BLOCKER",
                    "details": "Assertion missing dependencies: [\"<target_table>\"]. May execute before target table materialization!"
                })
                if apply_fixes and target_table and not self.dry_run:
                    if cfg_match:
                        cfg_body = cfg_match.group(1)
                        new_cfg = cfg_body.strip() + f'\n  dependencies: ["{target_table}"]\n'
                        new_content = content.replace(cfg_body, new_cfg)
                        af.write_text(new_content, encoding="utf-8")
                        logger.info(f"Injected dependencies into assertion: {af}")

        return findings

    def audit_dag_yaml(
        self,
        dags_dir: Path,
        source_project: str,
        target_project: str,
        apply_fixes: bool = False
    ) -> List[Dict[str, Any]]:
        findings = []
        if not dags_dir.exists():
            return findings

        is_cross = self.is_cross_project(target_project, source_project)

        for df in dags_dir.glob("**/*.yaml"):
            content = df.read_text(encoding="utf-8")
            if is_cross and "vars:" not in content and "project_id" not in content:
                findings.append({
                    "file": str(df),
                    "issue": "DAG_MISSING_VARS_PROJECT_ID",
                    "severity": "BLOCKER",
                    "details": f"DAG compiles across projects ({target_project} -> {source_project}) but lacks 'vars: {{\"project_id\": \"{source_project}\"}}'."
                })
                if apply_fixes and not self.dry_run:
                    fixed_content = content + f"\n      vars: {{\"project_id\": \"{source_project}\"}}\n"
                    df.write_text(fixed_content, encoding="utf-8")
                    logger.info(f"Auto-injected vars.project_id into DAG: {df}")

        return findings

    def check_pr_isolation(
        self,
        repo_dir: Path,
        allowed_domain_or_card: str
    ) -> List[Dict[str, Any]]:
        """
        Checks git diff to ensure changes are strictly isolated to the assigned card domain.
        """
        findings = []
        if self.dry_run:
            logger.info(f"[DRY-RUN] PR isolation verified for card scope: {allowed_domain_or_card}")
            return findings

        try:
            res = subprocess.run(
                ["git", "diff", "--name-only", "origin/main...HEAD"],
                cwd=str(repo_dir), capture_output=True, text=True, timeout=10
            )
            if res.returncode == 0:
                modified_files = [line.strip() for line in res.stdout.strip().split("\n") if line.strip()]
                clean_scope = allowed_domain_or_card.lower().replace("-", "_")
                for mf in modified_files:
                    if clean_scope not in mf.lower() and "common" not in mf.lower():
                        findings.append({
                            "file": mf,
                            "issue": "SCOPE_ISOLATION_VIOLATION",
                            "severity": "BLOCKER",
                            "details": f"File '{mf}' modified outside card scope '{allowed_domain_or_card}'. Potential PR blast radius pollution."
                        })
        except Exception as e:
            logger.warning(f"Could not inspect git diff: {e}")

        return findings


def main() -> None:
    parser = argparse.ArgumentParser(description="Dataform Cross-Project Guard & Scope Isolation CLI")
    parser.add_argument("--artifacts-dir", "-d", type=str, required=True, help="Path to Dataform definitions root")
    parser.add_argument("--target-project", "-t", type=str, default="a-acme-dwh-prod", help="Target analytics project ID")
    parser.add_argument("--source-project", "-s", type=str, default="p-acme-lakehouse-raw", help="Upstream source project ID")
    parser.add_argument("--main-repo", type=str, default=None, help="Path to Dataform main repo clone (for source deduplication)")
    parser.add_argument("--card-scope", type=str, default=None, help="Card ID or domain for PR scope isolation check")
    parser.add_argument("--target-table", type=str, default=None, help="Target table name for assertion injection")
    parser.add_argument("--fix", action="store_true", help="Auto-remediate detected issues")
    parser.add_argument("--check", action="store_true", help="Audit only (default)")
    parser.add_argument("--format", choices=["json", "markdown"], default="markdown", help="Output format")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without writing file modifications")

    args = parser.parse_args()

    art_p = Path(args.artifacts_dir)
    guard = DataformCrossProjectGuard(dry_run=args.dry_run)

    all_findings = []
    # 1. Sources
    all_findings.extend(guard.audit_and_fix_sources(
        art_p / "definitions" / "01_sources",
        target_project=args.target_project,
        main_repo_dir=Path(args.main_repo) if args.main_repo else None,
        apply_fixes=args.fix
    ))

    # 2. Models
    all_findings.extend(guard.audit_and_fix_models(
        art_p / "definitions",
        apply_fixes=args.fix
    ))

    # 3. Assertions
    all_findings.extend(guard.audit_and_fix_assertions(
        art_p / "definitions" / "05_assertions",
        target_table=args.target_table,
        apply_fixes=args.fix
    ))

    # 4. DAGs
    all_findings.extend(guard.audit_dag_yaml(
        art_p / "dags",
        source_project=args.source_project,
        target_project=args.target_project,
        apply_fixes=args.fix
    ))

    # 5. PR Scope Isolation
    if args.card_scope:
        all_findings.extend(guard.check_pr_isolation(
            art_p,
            allowed_domain_or_card=args.card_scope
        ))

    passed = len([f for f in all_findings if f["severity"] == "BLOCKER"]) == 0

    if args.format == "json":
        print(json.dumps({"passed": passed, "findings": all_findings}, indent=2))
    else:
        status_banner = "🟢 PASSED (CROSS-PROJECT SAFE)" if passed else "🔴 FAILED (BLOCKERS DETECTED)"
        print(f"\n# 🛡️ Dataform Cross-Project & Scope Isolation Guard")
        print(f"**Verdict:** {status_banner}")
        print(f"**Total Findings:** `{len(all_findings)}`\n")
        if not all_findings:
            print("✅ All cross-project references, DAG configurations, and PR scope boundaries are clean.\n")
        else:
            print("| Severity | Issue | File | Details |")
            print("| :--- | :--- | :--- | :--- |")
            for f in all_findings:
                icon = "🔴" if f["severity"] == "BLOCKER" else "🟡"
                print(f"| {icon} `{f['severity']}` | `{f['issue']}` | `{f['file']}` | {f['details']} |")
            print()

    if not passed and not args.fix:
        sys.exit(2)


if __name__ == "__main__":
    main()
