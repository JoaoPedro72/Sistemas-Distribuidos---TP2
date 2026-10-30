"""Executa os estudos de caso do TP2 com processos Peer reais e resume os logs.

Para cada combinacao de arquivo, tamanho de bloco e quantidade de Peers, o
script cria um arquivo aleatorio, inicia o Seeder e os Leechers em processos
separados (``peer.py``), mede o tempo ate cada Leecher remontar o arquivo,
confere tamanho e SHA-256 e conta, a partir dos logs, de qual Peer veio cada
bloco.

Topologias (vizinhanca estatica):
  2 Peers: P1 (Seeder) <- P2
  4 Peers: P1 (Seeder) <- P2;  P3 -> [P1, P2];  P4 -> [P2, P3]
P4 nao conhece o Seeder, entao todo bloco que ele recebe foi servido por um
Leecher.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

PEER_SCRIPT = Path(__file__).resolve().with_name("peer.py")
HOST = "127.0.0.1"
FILES = {
    "A-10KB": 10 * 1024,
    "A-20KB": 20 * 1024,
    "B-1MB": 1024 * 1024,
    "B-5MB": 5 * 1024 * 1024,
    "C-10MB": 10 * 1024 * 1024,
    "C-20MB": 20 * 1024 * 1024,
}
TOPOLOGIES = {
    2: {"P2": ["P1"]},
    4: {"P2": ["P1"], "P3": ["P1", "P2"], "P4": ["P2", "P3"]},
}
RECEIVED = re.compile(r"\[(P\d)\] INFO: bloco \d+/\d+ recebido de [^:]+:(\d+)")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _wait_for(condition, timeout: float, interval: float = 0.02) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(interval)
    return False


def _launch(name: str, args: list[str], cwd: Path) -> tuple[subprocess.Popen, Any]:
    log = (cwd / f"{name}.log").open("w", encoding="utf-8")
    process = subprocess.Popen(
        [sys.executable, str(PEER_SCRIPT), *args, "--id", name],
        cwd=cwd,
        stdout=log,
        stderr=subprocess.STDOUT,
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )
    return process, log


def run_case(
    label: str,
    file_size: int,
    block_size: int,
    peer_count: int,
    run_dir: Path,
    base_port: int,
    timeout: float,
) -> list[dict[str, Any]]:
    ports = {f"P{i}": base_port + i for i in range(1, peer_count + 1)}
    names_by_port = {port: name for name, port in ports.items()}
    for name in ports:
        (run_dir / name).mkdir(parents=True)
    source = run_dir / "P1" / f"{label}.bin"
    source.write_bytes(os.urandom(file_size))
    original_hash = _sha256(source)
    metadata_path = run_dir / "metadata.json"

    processes: dict[str, tuple[subprocess.Popen, Any]] = {}
    try:
        processes["P1"] = _launch(
            "P1",
            ["seed", "--file", source.name, "--metadata", str(metadata_path),
             "--block-size", str(block_size), "--host", HOST, "--port", str(ports["P1"])],
            run_dir / "P1",
        )
        seed_log = run_dir / "P1" / "P1.log"
        # O Seeder grava todos os blocos antes de ouvir; com 20 MB/1 KB isso passa de 30 s.
        seed_ready = lambda: "ouvindo em" in seed_log.read_text(encoding="utf-8")
        _wait_for(lambda: seed_ready() or processes["P1"][0].poll() is not None, timeout)
        if not seed_ready():
            raise RuntimeError(f"Seeder nao iniciou: {seed_log.read_text(encoding='utf-8')}")

        outputs = {name: run_dir / name / f"{label}.bin" for name in TOPOLOGIES[peer_count]}
        started = time.monotonic()
        for name, neighbors in TOPOLOGIES[peer_count].items():
            processes[name] = _launch(
                name,
                ["peer", "--metadata", str(metadata_path), "--output", outputs[name].name,
                 "--host", HOST, "--port", str(ports[name]), "--timeout", str(timeout),
                 "--neighbors", *(f"{HOST}:{ports[neighbor]}" for neighbor in neighbors)],
                run_dir / name,
            )

        finished: dict[str, float] = {}
        deadline = started + timeout + 10
        while len(finished) < len(outputs) and time.monotonic() < deadline:
            for name, output in outputs.items():
                if name in finished:
                    continue
                if output.exists():
                    finished[name] = time.monotonic() - started
                elif processes[name][0].poll() is not None:
                    finished[name] = float("nan")
            time.sleep(0.02)
        # Garante que a linha final de log ("arquivo remontado") ja foi gravada.
        time.sleep(0.3)
    finally:
        for process, log in processes.values():
            process.terminate()
            process.wait()
            log.close()

    rows = []
    for name, output in outputs.items():
        log_text = (run_dir / name / f"{name}.log").read_text(encoding="utf-8")
        sources: dict[str, int] = {}
        for _, port in RECEIVED.findall(log_text):
            source_name = names_by_port[int(port)]
            sources[source_name] = sources.get(source_name, 0) + 1
        downloaded = output.exists()
        rows.append({
            "arquivo": label,
            "tamanho_bytes": file_size,
            "bloco_bytes": block_size,
            "blocos": -(-file_size // block_size),
            "peers": peer_count,
            "leecher": name,
            "vizinhos": "+".join(TOPOLOGIES[peer_count][name]),
            "tempo_s": round(finished.get(name, float("nan")), 3),
            "tamanho_ok": downloaded and output.stat().st_size == file_size,
            "sha256_ok": downloaded and _sha256(output) == original_hash,
            "fontes": ";".join(f"{key}={value}" for key, value in sorted(sources.items())),
            "falhas_bloco": log_text.count("falha ao obter bloco"),
            "esperas": log_text.count("aguardando blocos"),
            "sha256": original_hash,
        })
    return rows


def _summarize(rows: list[dict[str, Any]]) -> str:
    groups: dict[tuple, list[dict[str, Any]]] = {}
    for row in rows:
        key = (row["tamanho_bytes"], row["arquivo"], row["bloco_bytes"], row["peers"])
        groups.setdefault(key, []).append(row)

    lines = [
        "| Arquivo | Bloco | Blocos | Peers | Tempo (s) | MB/s | Integridade | Origem dos blocos |",
        "| :-- | --: | --: | --: | --: | --: | :-: | :-- |",
    ]
    for (size, label, block, peers), group in sorted(groups.items()):
        # Tempo de uma execucao = Leecher mais lento; media entre repeticoes.
        per_run: dict[int, float] = {}
        for row in group:
            per_run[row["repeticao"]] = max(per_run.get(row["repeticao"], 0.0), row["tempo_s"])
        elapsed = statistics.mean(per_run.values())
        spread = f" ± {statistics.stdev(per_run.values()):.2f}" if len(per_run) > 1 else ""
        integrity = "OK" if all(r["tamanho_ok"] and r["sha256_ok"] for r in group) else "FALHA"
        first_run = min(per_run)
        origin = "; ".join(
            f"{r['leecher']}: {r['fontes']}" for r in group if r["repeticao"] == first_run
        )
        lines.append(
            f"| {label} | {block} | {-(-size // block)} | {peers} | {elapsed:.2f}{spread} "
            f"| {size / 1024 / 1024 / elapsed:.2f} | {integrity} | {origin} |"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--files", nargs="*", default=list(FILES), choices=list(FILES))
    parser.add_argument("--block-sizes", type=int, nargs="*", default=[1024, 4096])
    parser.add_argument("--peers", type=int, nargs="*", default=[2, 4], choices=list(TOPOLOGIES))
    parser.add_argument("--repeat", type=int, default=1, help="repeticoes por configuracao")
    parser.add_argument("--work-dir", type=Path, help="onde guardar arquivos e logs (padrao: temporario)")
    parser.add_argument("--results", type=Path, default=Path("resultados"), help="pasta do resumo")
    parser.add_argument("--base-port", type=int, default=6000)
    parser.add_argument("--timeout", type=float, default=600.0)
    parser.add_argument("--keep-files", action="store_true", help="mantem blocos e arquivos baixados")
    args = parser.parse_args()

    work_dir = args.work_dir or Path(tempfile.mkdtemp(prefix="p2p-estudos-"))
    work_dir.mkdir(parents=True, exist_ok=True)
    args.results.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    case = 0
    for repetition in range(1, args.repeat + 1):
        for label in args.files:
            for block_size in args.block_sizes:
                for peer_count in args.peers:
                    run_dir = work_dir / f"{label}_b{block_size}_p{peer_count}_r{repetition}"
                    if run_dir.exists():
                        shutil.rmtree(run_dir)
                    case += 1
                    base_port = args.base_port + (case % 500) * 10
                    print(f"[{case}] {label} bloco={block_size} peers={peer_count} rep={repetition} ...",
                          end=" ", flush=True)
                    case_rows = run_case(label, FILES[label], block_size, peer_count,
                                         run_dir, base_port, args.timeout)
                    for row in case_rows:
                        row["repeticao"] = repetition
                    rows.extend(case_rows)
                    print(", ".join(
                        f"{r['leecher']} {r['tempo_s']}s sha={'ok' if r['sha256_ok'] else 'FALHA'} [{r['fontes']}]"
                        for r in case_rows
                    ), flush=True)
                    if not args.keep_files:
                        for path in run_dir.rglob("*"):
                            if path.is_file() and path.suffix == ".bin":
                                path.unlink()

    fields = ["repeticao", *[key for key in rows[0] if key != "repeticao"]]
    with (args.results / "resultados.csv").open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    (args.results / "resultados.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")
    summary = _summarize(rows)
    (args.results / "resumo.md").write_text(summary, encoding="utf-8")
    print(f"\nLogs em {work_dir}\n")
    print(summary)
    return 0 if all(r["sha256_ok"] and r["tamanho_ok"] for r in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
