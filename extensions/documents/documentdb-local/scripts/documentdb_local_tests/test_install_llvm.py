# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


INSTALLER = Path(__file__).resolve().parents[3] / "scripts" / "install_llvm.sh"


class InstallLlvmTests(unittest.TestCase):
    def run_installer(self, status="200", probe_exit="0", download_exit="0"):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            commands = directory / "commands"
            commands.touch()
            mocks = {
                "curl": """#!/bin/sh
printf 'curl %s\\n' "$*" >> "$COMMANDS"
case "$*" in
    *llvm.sh*)
        [ "$DOWNLOAD_EXIT" = 0 ] || exit "$DOWNLOAD_EXIT"
        printf '#!/bin/sh\\nexit 0\\n' > llvm.sh
        ;;
    *)
        printf '%s' "$HTTP_STATUS"
        exit "$PROBE_EXIT"
        ;;
esac
""",
                "sudo": """#!/bin/sh
printf 'sudo %s\\n' "$*" >> "$COMMANDS"
""",
                "wget": """#!/bin/sh
printf 'wget %s\\n' "$*" >> "$COMMANDS"
exit 1
""",
            }
            for name, contents in mocks.items():
                executable = directory / name
                executable.write_text(contents)
                executable.chmod(0o755)
            environment = {
                **os.environ,
                "PATH": f"{directory}:{os.environ['PATH']}",
                "COMMANDS": str(commands),
                "HTTP_STATUS": status,
                "PROBE_EXIT": probe_exit,
                "DOWNLOAD_EXIT": download_exit,
            }
            result = subprocess.run(
                ["bash", str(INSTALLER)],
                cwd=directory,
                env=environment,
                capture_output=True,
                text=True,
                timeout=10,
            )
            return result, commands.read_text()

    def test_available_repository_uses_upstream_installer(self):
        result, commands = self.run_installer()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("sudo ./llvm.sh 20 all", commands)
        self.assertNotIn("apt-get", commands)
        requests = [line for line in commands.splitlines() if line.startswith("curl ")]
        self.assertEqual(len(requests), 2)
        for request in requests:
            self.assertIn("--retry 3", request)
            self.assertIn("--retry-connrefused", request)
            self.assertIn("--connect-timeout 15", request)
            self.assertIn("--max-time", request)

    def test_missing_repository_uses_distro_packages(self):
        result, commands = self.run_installer(status="404")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("sudo apt-get update", commands)
        self.assertIn("sudo apt-get install -y clang-20", commands)
        self.assertNotIn("sudo ./llvm.sh", commands)

    def test_http_errors_do_not_fall_back_to_distro(self):
        for status in ("403", "429", "500", "503"):
            with self.subTest(status=status):
                result, commands = self.run_installer(status=status)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(f"HTTP {status}", result.stderr)
                self.assertNotIn("sudo ", commands)

    def test_transport_failure_does_not_fall_back_to_distro(self):
        result, commands = self.run_installer(status="000", probe_exit="7")
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("sudo ", commands)

    def test_installer_download_failure_is_fatal(self):
        result, commands = self.run_installer(download_exit="22")
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("sudo ", commands)


if __name__ == "__main__":
    unittest.main()
