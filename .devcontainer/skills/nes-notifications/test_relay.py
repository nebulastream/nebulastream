import json
import os
from pathlib import Path
import select
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from relay import MAX_FRAME_BYTES, SCHEMA, parse_message


HERE = Path(__file__).resolve().parent


def frame(text="hello", kind="status"):
    return (json.dumps({"type": kind, "text": text}, ensure_ascii=False) + "\n").encode()


class RelayTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="nes-relay-test-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.pi_path = self.root / "pi.sock"
        self.messages = []
        self.connections = 0
        self.ack = b'{"ok":true}\n'
        self.errors = []
        self.done = threading.Event()
        self.listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.listener.bind(str(self.pi_path))
        self.listener.listen()
        self.listener.settimeout(0.05)
        self.server = threading.Thread(target=self.serve_pi)
        self.server.start()
        self.addCleanup(self.close_pi)

    def serve_pi(self):
        while not self.done.is_set():
            try:
                peer, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with peer:
                peer.settimeout(1)
                try:
                    data = bytearray()
                    while b"\n" not in data:
                        chunk = peer.recv(1024)
                        if not chunk:
                            break
                        data.extend(chunk)
                    self.connections += 1
                    self.messages.append(json.loads(data.decode()))
                    if self.ack == "timeout":
                        self.done.wait(0.5)
                    elif self.ack is not None:
                        peer.sendall(self.ack)
                except (ValueError, OSError) as error:
                    self.errors.append(error)

    def close_pi(self):
        self.done.set()
        self.listener.close()
        self.server.join(timeout=2)
        self.assertFalse(self.server.is_alive())
        self.assertEqual(self.errors, [])

    def spawn(self, *args):
        env = {**os.environ, "PI_NOTIFY_SOCKET": str(self.pi_path), "PYTHONDONTWRITEBYTECODE": "1"}
        process = subprocess.Popen([sys.executable, str(HERE / "relay.py"), *args],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
        self.addCleanup(self.reap, process)
        return process

    @staticmethod
    def reap(process):
        if process.poll() is None:
            process.terminate()
        try:
            process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=2)

    def start(self, *args):
        process = self.spawn(*args)
        ready, _, _ = select.select([process.stdout], [], [], 4)
        self.assertTrue(ready, "relay did not announce readiness")
        line = process.stdout.readline()
        if not line:
            self.fail(f"relay failed at startup: {process.stderr.read()}")
        status = json.loads(line)
        self.assertEqual(status["event"], "ready")
        path = Path(status["socket"])
        self.assertTrue(path.is_socket())
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        return process, path

    def send(self, path, data):
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as source:
            source.connect(str(path))
            source.sendall(data)

    def finished(self, process, path, success=True):
        output, error = process.communicate(timeout=6)
        self.assertEqual(process.returncode == 0, success, output + error)
        self.assertFalse(path.exists(), "remove only the relay input socket")
        self.assertTrue(self.pi_path.is_socket(), "never remove Pi's endpoint")
        return output, error

    def test_batched_messages_use_a_fresh_pi_connection_each(self):
        process, path = self.start("--once")
        self.assertEqual(path.parent.stat().st_mode & 0o777, 0o700)
        self.send(path, SCHEMA + b"\n" + frame("first") + frame("second", "notify"))
        output, _ = self.finished(process, path)
        self.assertEqual([message["text"] for message in self.messages], ["first", "second"])
        self.assertEqual(self.connections, 2)
        self.assertEqual(json.loads(output)["forwarded"], 2)

    def test_split_schema_and_split_utf8(self):
        process, path = self.start("--once")
        data = frame("euro €")
        split = data.index("€".encode()) + 1
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as source:
            source.connect(str(path))
            source.sendall(SCHEMA[:10])
            time.sleep(0.03)
            source.sendall(SCHEMA[10:] + b"\n" + data[:split])
            time.sleep(0.03)
            source.sendall(data[split:])
        self.finished(process, path)
        self.assertEqual(self.messages, [{"type": "status", "text": "euro €"}])

    def test_schema_only_is_not_a_notification(self):
        process, path = self.start("--once")
        self.send(path, SCHEMA + b"\n")
        self.finished(process, path)
        self.assertEqual(self.connections, 0)

    def test_header_is_optional(self):
        process, path = self.start("--once")
        self.send(path, frame())
        self.finished(process, path)
        self.assertEqual(self.connections, 1)

    def test_idle_nes_stream_survives_pi_connection_deadline(self):
        process, path = self.start("--once")
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as source:
            source.connect(str(path))
            source.sendall(SCHEMA + b"\n" + frame("before"))
            time.sleep(3.2)
            source.sendall(frame("after"))
        self.finished(process, path)
        self.assertEqual([message["text"] for message in self.messages], ["before", "after"])
        self.assertEqual(self.connections, 2)

    def test_frame_limit_is_not_a_total_stream_limit(self):
        process, path = self.start("--once")
        self.send(path, frame("x" * 8192, "notify") * 9)
        self.finished(process, path)
        self.assertEqual(self.connections, 9)

    def test_invalid_input_fails_without_leaking_payloads(self):
        for data, expected in [
            (b'"wrong":VARSIZED:NOT_NULLABLE\n', "Invalid notification"),
            (SCHEMA + b"\n" + SCHEMA + b"\n", "Invalid notification"),
            (b'{"private":"sensitive payload"}\n', "Invalid notification"),
            (b'\xff\n', "Invalid notification"),
            (b'{', "Incomplete trailing"),
            (b'x' * MAX_FRAME_BYTES, "64 KiB"),
        ]:
            with self.subTest(data=data[:30]):
                process, path = self.start("--once")
                self.send(path, data)
                _, error = self.finished(process, path, success=False)
                self.assertIn(expected, error)
                self.assertNotIn("sensitive payload", error)
        self.assertEqual(self.connections, 0)

    def test_pi_rejection_is_fatal_not_silently_dropped(self):
        self.ack = b'{"ok":false,"error":"rate_limited"}\n'
        process, path = self.start("--once")
        self.send(path, frame("private text"))
        _, error = self.finished(process, path, success=False)
        self.assertIn("rate_limited", error)
        self.assertNotIn("private text", error)
        self.assertEqual(self.connections, 1, "no automatic retry")

    def test_bad_missing_and_timed_out_acknowledgements_fail(self):
        for ack in (b'garbage\n', b'{}\n', None, "timeout"):
            with self.subTest(ack=ack):
                self.ack = ack
                process, path = self.start("--once", "--timeout", "0.15")
                self.send(path, frame())
                self.finished(process, path, success=False)
                # Let the fake server finish the intentionally stalled request.
                if ack == "timeout":
                    time.sleep(0.5)

    def test_signal_cleans_up_an_idle_listener_or_connection(self):
        for connected in (False, True):
            with self.subTest(connected=connected):
                process, path = self.start()
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as source:
                    if connected:
                        source.connect(str(path))
                        source.sendall(SCHEMA + b"\n")
                    process.send_signal(signal.SIGTERM)
                    self.finished(process, path)

    def test_accepts_sequential_nes_connections(self):
        process, path = self.start()
        self.send(path, SCHEMA + b"\n" + frame("one"))
        deadline = time.monotonic() + 2
        while len(self.messages) < 1 and time.monotonic() < deadline:
            time.sleep(0.01)
        self.send(path, SCHEMA + b"\n" + frame("two"))
        deadline = time.monotonic() + 2
        while len(self.messages) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        process.terminate()
        self.finished(process, path)
        self.assertEqual([message["text"] for message in self.messages], ["one", "two"])

    def test_does_not_replace_an_existing_input_path(self):
        path = self.root / "existing"
        path.write_text("keep this")
        process = self.spawn("--listen", str(path))
        process.communicate(timeout=3)
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(path.read_text(), "keep this")

    def worker(self, body):
        binary = self.root / "worker"
        binary.write_text("#!/usr/bin/env python3\n" + body)
        binary.chmod(0o700)
        sql = self.root / "query.sql"
        sql.write_text("'@NES_NOTIFY_SOCKET@' AS FILE_PATH")
        return binary, sql

    def test_supervised_sql_is_rendered_and_worker_exit_is_propagated(self):
        binary, sql = self.worker(
            "import re,socket,sys\n"
            "sql=sys.stdin.read()\n"
            "path=re.search(r\"'([^']*)'\", sql).group(1)\n"
            "data=" + repr(SCHEMA + b"\n" + frame("from worker")) + "\n"
            "with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as peer:\n"
            "    peer.connect(path)\n    peer.sendall(data)\n"
            "sys.exit(7)\n")
        process, path = self.start("--binary", str(binary), "--sql", str(sql))
        self.finished(process, path, success=False)
        self.assertEqual(process.returncode, 7)
        self.assertEqual(self.messages, [{"type": "status", "text": "from worker"}])

    def test_signal_terminates_and_reaps_supervised_worker_even_if_it_ignores_term(self):
        marker = self.root / "worker.pid"
        binary, sql = self.worker(
            "import os,pathlib,signal,time\n"
            "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
            f"pathlib.Path({str(marker)!r}).write_text(str(os.getpid()))\n"
            "time.sleep(60)\n")
        process, path = self.start("--binary", str(binary), "--sql", str(sql))
        deadline = time.monotonic() + 2
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(marker.exists())
        pid = int(marker.read_text())
        process.terminate()
        self.finished(process, path)
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)

    def test_requires_the_sql_socket_placeholder(self):
        binary, sql = self.worker("raise RuntimeError('must not launch')\n")
        sql.write_text("SELECT 1;")
        path = self.root / "input.sock"
        process = self.spawn("--listen", str(path), "--binary", str(binary), "--sql", str(sql))
        _, error = process.communicate(timeout=3)
        self.assertEqual(process.returncode, 1)
        self.assertIn("@NES_NOTIFY_SOCKET@", error)
        self.assertFalse(path.exists())

    def test_project_installer_copies_only_runtime_files_and_preserves_other_skills(self):
        agent = self.root / "agent"
        other = agent / "skills" / "unrelated" / "SKILL.md"
        other.parent.mkdir(parents=True)
        other.write_text("keep this skill")
        installer = HERE.parents[1] / "install-pi-skills.sh"
        for _ in range(2):
            result = subprocess.run(["bash", str(installer)], cwd=self.root,
                                    env={**os.environ, "PI_CODING_AGENT_DIR": str(agent)},
                                    capture_output=True, text=True, timeout=3)
            self.assertEqual(result.returncode, 0, result.stderr)
            installed = agent / "skills" / "nes-notifications"
            self.assertEqual({file.name for file in installed.iterdir()}, {"SKILL.md", "relay.py"})
            for file in installed.iterdir():
                self.assertEqual(file.read_bytes(), (HERE / file.name).read_bytes())
                self.assertEqual(file.stat().st_mode & 0o777, 0o644)
            self.assertEqual(other.read_text(), "keep this skill")

    def test_message_schema_and_utf8_byte_limits(self):
        self.assertEqual(parse_message(frame("€" * 341)), {"type": "status", "text": "€" * 341})
        for data in (frame("€" * 342), frame("x" * 8193, "notify"),
                     b'{"type":"status","text":"x","extra":1}',
                     b'{"type":"notify","text":"\\ud800"}'):
            with self.assertRaisesRegex(Exception, "Invalid notification"):
                parse_message(data)


if __name__ == "__main__":
    unittest.main(verbosity=2)
