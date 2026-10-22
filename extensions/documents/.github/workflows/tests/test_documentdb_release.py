"""Contract tests for the draft GitHub release workflow step."""

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml


WORKFLOW = Path(__file__).resolve().parents[1] / "documentdb_release.yml"
REPOSITORY = "documentdb/documentdb"
TAG = "v0.117-0"
TARGET = "0123456789abcdef0123456789abcdef01234567"
SIDE_EFFECT_CONDITION = (
    "(github.event_name == 'push' && startsWith(github.ref, 'refs/tags/')) || "
    "(github.event_name == 'workflow_dispatch' && inputs.create_release)"
)


def workflow_step(name: str) -> dict[str, object]:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    steps = workflow["jobs"]["create-release"]["steps"]
    return next(step for step in steps if step["name"] == name)


def write_executable(path: Path, content: str) -> None:
    path.write_text(content)
    path.chmod(0o755)


class DocumentDbReleaseTests(unittest.TestCase):
    def run_release_step(
        self, scenario: str, *, allow_missing_tag: bool = False
    ) -> tuple[subprocess.CompletedProcess[str], list[list[str]]]:
        script = workflow_step("Create or update draft GitHub Release")["run"]

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            bundle = root / "bundle"
            bundle.mkdir()
            (bundle / "documentdb.deb").write_text("package")
            (bundle / "SHA256SUMS").write_text("checksums")
            (root / "changelog_excerpt.md").write_text("release notes")

            bin_path = root / "bin"
            bin_path.mkdir()
            write_executable(
                bin_path / "gh",
                """#!/usr/bin/env python3
import json
import os
import sys

args = sys.argv[1:]
with open(os.environ["MOCK_GH_LOG"], "a", encoding="utf-8") as log:
    log.write(json.dumps(args) + "\\n")

scenario = os.environ["MOCK_SCENARIO"]
if args[:1] == ["api"]:
    endpoint = args[1]
    if "/releases/tags/" in endpoint:
        if scenario in (
            "new-draft",
            "new-draft-wrong-tag",
            "manual-new-draft-no-tag",
            "tag-trigger-missing-tag",
        ):
            print("gh: Not Found (HTTP 404)", file=sys.stderr)
            raise SystemExit(1)
        if scenario == "auth-error":
            print("gh: Resource not accessible by integration (HTTP 403)",
                  file=sys.stderr)
            raise SystemExit(1)
        draft = "false" if scenario == "published" else "true"
        print(f"{draft}\\t{os.environ['MOCK_TAG']}")
        raise SystemExit(0)
    if "/git/ref/tags/" in endpoint:
        if scenario in (
            "manual-new-draft-no-tag",
            "manual-existing-draft-no-tag",
            "tag-trigger-missing-tag",
        ):
            print("gh: Not Found (HTTP 404)", file=sys.stderr)
            raise SystemExit(1)
        if scenario == "tag-api-error":
            print("gh: API unavailable (HTTP 500)", file=sys.stderr)
            raise SystemExit(1)
        print(f"refs/tags/{os.environ['MOCK_TAG']}")
        raise SystemExit(0)
    if "/commits/refs/tags/" in endpoint:
        if scenario == "commit-resolution-error":
            print("gh: No commit found for SHA (HTTP 422)", file=sys.stderr)
            raise SystemExit(1)
        target = (
            "ffffffffffffffffffffffffffffffffffffffff"
            if scenario in ("target-mismatch", "new-draft-wrong-tag")
            else os.environ["EXPECTED_TARGET"]
        )
        print(target)
        raise SystemExit(0)
    print(f"unexpected API endpoint: {endpoint}", file=sys.stderr)
    raise SystemExit(64)

if args[:2] == ["release", "upload"] and scenario == "upload-failure":
    print("mock upload failed", file=sys.stderr)
    raise SystemExit(1)
if args[:2] in (
    ["release", "create"],
    ["release", "edit"],
    ["release", "upload"],
):
    raise SystemExit(0)

print(f"unexpected gh arguments: {args!r}", file=sys.stderr)
raise SystemExit(64)
""",
            )

            log_path = root / "gh.log"
            env = os.environ.copy()
            env.update(
                {
                    "PATH": f"{bin_path}{os.pathsep}{env['PATH']}",
                    "VERSION_DASH": "0.117-0",
                    "RELEASE_NAME": "v0.117-0 (September 9, 2026)",
                    "RELEASE_REPOSITORY": REPOSITORY,
                    "RELEASE_TARGET": TARGET,
                    "ALLOW_MISSING_RELEASE_TAG": (
                        "true" if allow_missing_tag else "false"
                    ),
                    "MOCK_SCENARIO": scenario,
                    "MOCK_GH_LOG": str(log_path),
                    "MOCK_TAG": TAG,
                    "EXPECTED_TARGET": TARGET,
                }
            )
            result = subprocess.run(
                ["bash", "-c", script],
                cwd=root,
                env=env,
                text=True,
                capture_output=True,
            )
            calls = (
                [json.loads(line) for line in log_path.read_text().splitlines()]
                if log_path.exists()
                else []
            )
            return result, calls

    @staticmethod
    def release_calls(calls: list[list[str]], command: str) -> list[list[str]]:
        return [call for call in calls if call[:2] == ["release", command]]

    def test_side_effect_jobs_require_tag_push_or_confirmed_manual_release(self):
        workflow = yaml.safe_load(WORKFLOW.read_text())
        jobs = workflow["jobs"]

        self.assertEqual(
            jobs["build-gateway-images"]["with"]["push_images"],
            f"${{{{ {SIDE_EFFECT_CONDITION} }}}}",
        )
        self.assertEqual(jobs["create-release"]["if"], SIDE_EFFECT_CONDITION)
        self.assertEqual(
            workflow_step("Create or update draft GitHub Release")["env"][
                "ALLOW_MISSING_RELEASE_TAG"
            ],
            "${{ github.event_name == 'workflow_dispatch' && "
            "inputs.create_release && github.ref_type == 'branch' }}",
        )

    def test_side_effect_policy_cases(self):
        def allowed(event_name: str, ref: str, create_release: bool) -> bool:
            return (
                event_name == "push" and ref.startswith("refs/tags/")
            ) or (
                event_name == "workflow_dispatch" and create_release
            )

        cases = (
            ("workflow_dispatch", "refs/heads/main", False, False),
            ("workflow_dispatch", "refs/tags/v0.117-0", False, False),
            ("workflow_dispatch", "refs/heads/main", True, True),
            ("workflow_dispatch", "refs/tags/v0.117-0", True, True),
            ("push", "refs/tags/v0.117-0", False, True),
            ("push", "refs/heads/main", False, False),
        )
        for event_name, ref, create_release, expected in cases:
            with self.subTest(
                event_name=event_name, ref=ref, create_release=create_release
            ):
                self.assertEqual(
                    allowed(event_name, ref, create_release),
                    expected,
                )

    def test_existing_draft_is_updated_and_assets_are_reuploaded(self):
        result, calls = self.run_release_step("existing-draft")

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(len(self.release_calls(calls, "create")), 0)
        edits = self.release_calls(calls, "edit")
        self.assertEqual(len(edits), 1)
        self.assertIn("--draft", edits[0])
        self.assertIn("--prerelease", edits[0])
        self.assertIn("--verify-tag", edits[0])
        self.assertEqual(edits[0][edits[0].index("--target") + 1], TARGET)
        self.assertEqual(edits[0][edits[0].index("-R") + 1], REPOSITORY)
        self.assertIn(
            [
                "api",
                f"repos/{REPOSITORY}/git/ref/tags/{TAG}",
                "--jq",
                ".ref",
            ],
            calls,
        )
        self.assertIn(
            [
                "api",
                f"repos/{REPOSITORY}/commits/refs/tags/{TAG}",
                "--jq",
                ".sha",
            ],
            calls,
        )

        uploads = self.release_calls(calls, "upload")
        self.assertEqual(len(uploads), 1)
        self.assertIn("./bundle/documentdb.deb", uploads[0])
        self.assertIn("./bundle/SHA256SUMS", uploads[0])
        self.assertIn("--clobber", uploads[0])

    def test_missing_release_creates_a_draft_at_the_exact_target(self):
        result, calls = self.run_release_step("new-draft")

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        creates = self.release_calls(calls, "create")
        self.assertEqual(len(creates), 1)
        self.assertIn("--draft", creates[0])
        self.assertIn("--prerelease", creates[0])
        self.assertIn("--verify-tag", creates[0])
        self.assertEqual(creates[0][creates[0].index("--target") + 1], TARGET)
        self.assertEqual(creates[0][creates[0].index("-R") + 1], REPOSITORY)
        self.assertEqual(len(self.release_calls(calls, "upload")), 1)

    def test_published_release_is_never_changed_back_to_draft(self):
        result, calls = self.run_release_step("published")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("already published", result.stdout + result.stderr)
        self.assertEqual(self.release_calls(calls, "edit"), [])
        self.assertEqual(self.release_calls(calls, "upload"), [])

    def test_api_error_is_not_treated_as_release_not_found(self):
        result, calls = self.run_release_step("auth-error")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "refusing to treat an API or authentication error as a missing release",
            result.stdout + result.stderr,
        )
        self.assertEqual(self.release_calls(calls, "create"), [])
        self.assertEqual(self.release_calls(calls, "upload"), [])

    def test_existing_tag_must_target_the_release_commit(self):
        result, calls = self.run_release_step("target-mismatch")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "Refusing to create, update, or retarget the release",
            result.stdout + result.stderr,
        )
        self.assertEqual(self.release_calls(calls, "create"), [])
        self.assertEqual(self.release_calls(calls, "edit"), [])
        self.assertEqual(self.release_calls(calls, "upload"), [])

    def test_missing_release_with_wrong_tag_target_does_not_create(self):
        result, calls = self.run_release_step("new-draft-wrong-tag")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "Refusing to create, update, or retarget the release",
            result.stdout + result.stderr,
        )
        self.assertEqual(self.release_calls(calls, "create"), [])
        self.assertEqual(self.release_calls(calls, "edit"), [])
        self.assertEqual(self.release_calls(calls, "upload"), [])

    def test_manual_branch_release_can_prepare_draft_before_tag_exists(self):
        result, calls = self.run_release_step(
            "manual-new-draft-no-tag", allow_missing_tag=True
        )

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        creates = self.release_calls(calls, "create")
        self.assertEqual(len(creates), 1)
        self.assertNotIn("--verify-tag", creates[0])
        self.assertEqual(creates[0][creates[0].index("--target") + 1], TARGET)
        self.assertEqual(len(self.release_calls(calls, "upload")), 1)

    def test_manual_branch_can_update_untagged_draft_metadata(self):
        result, calls = self.run_release_step(
            "manual-existing-draft-no-tag", allow_missing_tag=True
        )

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        edits = self.release_calls(calls, "edit")
        self.assertEqual(len(edits), 1)
        self.assertNotIn("--verify-tag", edits[0])
        self.assertEqual(edits[0][edits[0].index("--target") + 1], TARGET)
        self.assertEqual(self.release_calls(calls, "create"), [])
        self.assertEqual(len(self.release_calls(calls, "upload")), 1)

    def test_tag_trigger_with_deleted_tag_does_not_recreate_it(self):
        result, calls = self.run_release_step("tag-trigger-missing-tag")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "Required remote tag",
            result.stdout + result.stderr,
        )
        self.assertEqual(self.release_calls(calls, "create"), [])
        self.assertEqual(self.release_calls(calls, "edit"), [])
        self.assertEqual(self.release_calls(calls, "upload"), [])

    def test_tag_lookup_api_error_is_not_treated_as_missing(self):
        result, calls = self.run_release_step("tag-api-error")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "refusing to treat an API or authentication error as a missing tag",
            result.stdout + result.stderr,
        )
        self.assertEqual(self.release_calls(calls, "create"), [])
        self.assertEqual(self.release_calls(calls, "edit"), [])
        self.assertEqual(self.release_calls(calls, "upload"), [])

    def test_commit_resolution_error_does_not_allow_an_untagged_draft(self):
        result, calls = self.run_release_step(
            "commit-resolution-error", allow_missing_tag=True
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "Failed to resolve the commit for remote tag",
            result.stdout + result.stderr,
        )
        self.assertEqual(self.release_calls(calls, "create"), [])
        self.assertEqual(self.release_calls(calls, "edit"), [])
        self.assertEqual(self.release_calls(calls, "upload"), [])

    def test_asset_upload_failure_fails_the_release_job(self):
        result, calls = self.run_release_step("upload-failure")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "Failed to upload the verified release assets",
            result.stdout + result.stderr,
        )
        self.assertEqual(len(self.release_calls(calls, "edit")), 1)
        self.assertEqual(len(self.release_calls(calls, "upload")), 1)


if __name__ == "__main__":
    unittest.main()
