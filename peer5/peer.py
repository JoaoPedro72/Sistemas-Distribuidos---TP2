"""Transferencia de arquivos P2P por blocos usando sockets TCP."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import socket
import socketserver
import struct
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

DEFAULT_BLOCK_SIZE = 1024
DEFAULT_TIMEOUT = 5.0
MAX_HEADER_SIZE = 16 * 1024 * 1024
LOGGER = logging.getLogger("p2p")


class PeerError(Exception):
    """Erro esperado de configuracao, protocolo ou transferencia."""


def _recv_exact(connection: socket.socket, size: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        chunk = connection.recv(size - len(chunks))
        if not chunk:
            raise ConnectionError("conexao encerrada antes do fim da mensagem")
        chunks.extend(chunk)
    return bytes(chunks)


def _send_json(connection: socket.socket, value: dict[str, Any]) -> None:
    encoded = json.dumps(value, separators=(",", ":")).encode("utf-8")
    if len(encoded) > MAX_HEADER_SIZE:
        raise PeerError("cabecalho excede o limite do protocolo")
    connection.sendall(struct.pack("!I", len(encoded)))
    connection.sendall(encoded)


def _recv_json(connection: socket.socket) -> dict[str, Any]:
    header_size = struct.unpack("!I", _recv_exact(connection, 4))[0]
    if header_size == 0 or header_size > MAX_HEADER_SIZE:
        raise PeerError(f"tamanho de cabecalho invalido: {header_size}")
    try:
        decoded = json.loads(_recv_exact(connection, header_size).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PeerError("cabecalho JSON invalido") from error
    if not isinstance(decoded, dict):
        raise PeerError("o cabecalho deve ser um objeto JSON")
    return decoded


def _rpc(
    address: tuple[str, int],
    request: dict[str, Any],
    timeout: float = DEFAULT_TIMEOUT,
) -> tuple[dict[str, Any], bytes]:
    with socket.create_connection(address, timeout=timeout) as connection:
        connection.settimeout(timeout)
        _send_json(connection, request)
        response = _recv_json(connection)
        payload_size = response.get("payload_size", 0)
        if not isinstance(payload_size, int) or payload_size < 0:
            raise PeerError("tamanho de payload invalido")
        payload = _recv_exact(connection, payload_size) if payload_size else b""
    if response.get("status") != "ok":
        raise PeerError(str(response.get("error", "erro remoto sem descricao")))
    return response, payload


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=path.parent, prefix=f".{path.name}.", delete=False
        ) as temporary:
            temporary.write(content)
            temporary_path = Path(temporary.name)
        os.replace(temporary_path, path)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


def create_metadata(source: Path, block_size: int = DEFAULT_BLOCK_SIZE) -> dict[str, Any]:
    if block_size <= 0:
        raise PeerError("o tamanho do bloco deve ser maior que zero")
    if not source.is_file():
        raise PeerError(f"arquivo de origem nao encontrado: {source}")

    file_hash = hashlib.sha256()
    block_hashes: list[str] = []
    file_size = 0
    with source.open("rb") as file:
        while True:
            block = file.read(block_size)
            if not block:
                break
            file_size += len(block)
            file_hash.update(block)
            block_hashes.append(hashlib.sha256(block).hexdigest())

    digest = file_hash.hexdigest()
    return {
        "version": 1,
        "file_id": digest,
        "filename": source.name,
        "file_size": file_size,
        "block_size": block_size,
        "block_count": math.ceil(file_size / block_size),
        "sha256": digest,
        "block_hashes": block_hashes,
    }


def save_metadata(path: Path, metadata: dict[str, Any]) -> None:
    encoded = json.dumps(metadata, indent=2, ensure_ascii=False).encode("utf-8")
    _atomic_write(path, encoded)


def load_metadata(path: Path) -> dict[str, Any]:
    try:
        metadata = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PeerError(f"nao foi possivel ler os metadados {path}: {error}") from error

    if not isinstance(metadata, dict):
        raise PeerError("os metadados devem ser um objeto JSON")
    required = ("file_id", "filename", "file_size", "block_size", "block_count", "sha256", "block_hashes")
    if any(key not in metadata for key in required):
        raise PeerError("metadados incompletos")
    if not isinstance(metadata["filename"], str) or not metadata["filename"]:
        raise PeerError("nome de arquivo invalido nos metadados")
    for key in ("file_size", "block_size", "block_count"):
        if not isinstance(metadata[key], int) or isinstance(metadata[key], bool):
            raise PeerError(f"campo {key} deve ser inteiro")
    if metadata["file_size"] < 0 or metadata["block_size"] <= 0:
        raise PeerError("tamanho de arquivo ou de bloco invalido")
    expected_count = math.ceil(metadata["file_size"] / metadata["block_size"])
    if metadata["block_count"] != expected_count:
        raise PeerError("quantidade de blocos inconsistente")
    block_hashes = metadata["block_hashes"]
    if not isinstance(block_hashes, list) or len(block_hashes) != expected_count:
        raise PeerError("lista de hashes dos blocos inconsistente")
    hashes = [metadata["sha256"], metadata["file_id"], *block_hashes]
    if any(
        not isinstance(value, str)
        or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value)
        for value in hashes
    ):
        raise PeerError("hash SHA-256 invalido nos metadados")
    if metadata["file_id"] != metadata["sha256"]:
        raise PeerError("file_id deve corresponder ao SHA-256 do arquivo")
    return metadata


def parse_address(value: str) -> tuple[str, int]:
    if value.startswith("["):
        closing = value.find("]")
        if closing < 0 or value[closing + 1 : closing + 2] != ":":
            raise argparse.ArgumentTypeError("use o formato [IPv6]:porta")
        host, port_text = value[1:closing], value[closing + 2 :]
    else:
        try:
            host, port_text = value.rsplit(":", 1)
        except ValueError as error:
            raise argparse.ArgumentTypeError("use o formato host:porta") from error
    try:
        port = int(port_text)
    except ValueError as error:
        raise argparse.ArgumentTypeError("a porta deve ser um numero") from error
    if not host or not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("endereco ou porta invalida")
    return host, port


class _PeerRequestHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        peer: Peer = self.server.peer  # type: ignore[attr-defined]
        try:
            request = _recv_json(self.request)
            response, payload = peer.handle_request(request)
            _send_json(self.request, response)
            if payload:
                self.request.sendall(payload)
        except (ConnectionError, OSError, PeerError, ValueError) as error:
            LOGGER.warning("requisicao de %s:%s falhou: %s", *self.client_address, error)
            try:
                _send_json(
                    self.request,
                    {"status": "error", "error": str(error), "payload_size": 0},
                )
            except OSError:
                pass


class _ThreadedTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


class Peer:
    def __init__(
        self,
        host: str,
        port: int,
        metadata: dict[str, Any],
        storage: Path,
        neighbors: list[tuple[str, int]] | None = None,
        peer_id: str | None = None,
        seed_file: Path | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.metadata = metadata
        self.neighbors = neighbors or []
        self.peer_id = peer_id or f"{host}:{port}"
        self.storage = storage / metadata["file_id"]
        self.storage.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._server: _ThreadedTCPServer | None = None
        self._server_thread: threading.Thread | None = None
        self._available = [False] * metadata["block_count"]

        if seed_file is not None:
            self._load_seed(seed_file)
        else:
            self._load_existing_blocks()

    def _block_path(self, index: int) -> Path:
        return self.storage / f"block-{index:08d}.bin"

    def _expected_block_size(self, index: int) -> int:
        start = index * self.metadata["block_size"]
        return min(self.metadata["block_size"], self.metadata["file_size"] - start)

    def _load_seed(self, source: Path) -> None:
        file_hash = hashlib.sha256()
        with source.open("rb") as file:
            for index in range(self.metadata["block_count"]):
                block = file.read(self.metadata["block_size"])
                if len(block) != self._expected_block_size(index):
                    raise PeerError("arquivo de origem mudou desde a criacao dos metadados")
                if hashlib.sha256(block).hexdigest() != self.metadata["block_hashes"][index]:
                    raise PeerError("arquivo de origem nao corresponde aos metadados")
                file_hash.update(block)
                _atomic_write(self._block_path(index), block)
                self._available[index] = True
            if file.read(1):
                raise PeerError("arquivo de origem mudou desde a criacao dos metadados")
        if file_hash.hexdigest() != self.metadata["sha256"]:
            raise PeerError("SHA-256 do arquivo de origem nao corresponde aos metadados")

    def _load_existing_blocks(self) -> None:
        for index in range(self.metadata["block_count"]):
            block_path = self._block_path(index)
            if not block_path.is_file():
                continue
            try:
                block = block_path.read_bytes()
            except OSError:
                continue
            if (
                len(block) == self._expected_block_size(index)
                and hashlib.sha256(block).hexdigest() == self.metadata["block_hashes"][index]
            ):
                self._available[index] = True
            else:
                block_path.unlink(missing_ok=True)

    def start(self) -> None:
        if self._server is not None:
            raise PeerError("servidor deste Peer ja foi iniciado")
        try:
            server = _ThreadedTCPServer((self.host, self.port), _PeerRequestHandler)
        except OSError as error:
            raise PeerError(f"nao foi possivel iniciar o servidor em {self.host}:{self.port}: {error}") from error
        server.peer = self  # type: ignore[attr-defined]
        self._server = server
        self._server_thread = threading.Thread(
            target=server.serve_forever,
            name=f"peer-server-{self.peer_id}",
            daemon=True,
        )
        self._server_thread.start()
        LOGGER.info("Peer %s ouvindo em %s:%s", self.peer_id, self.host, self.port)

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._server_thread is not None:
            self._server_thread.join(timeout=2)
            self._server_thread = None

    def have(self) -> list[bool]:
        with self._lock:
            return self._available.copy()

    def handle_request(self, request: dict[str, Any]) -> tuple[dict[str, Any], bytes]:
        operation = request.get("op")
        if operation == "metadata":
            return {
                "status": "ok",
                "payload_size": 0,
                "metadata": self.metadata,
            }, b""
        if request.get("file_id") != self.metadata["file_id"]:
            raise PeerError("file_id nao corresponde ao arquivo deste Peer")
        if operation == "have":
            return {
                "status": "ok",
                "payload_size": 0,
                "file_id": self.metadata["file_id"],
                "blocks": self.have(),
            }, b""
        if operation == "get":
            index = request.get("index")
            if not isinstance(index, int) or isinstance(index, bool):
                raise PeerError("indice de bloco invalido")
            if not 0 <= index < self.metadata["block_count"]:
                raise PeerError("indice de bloco fora do intervalo")
            with self._lock:
                available = self._available[index]
            if not available:
                raise PeerError("bloco ainda nao disponivel neste Peer")
            payload = self._block_path(index).read_bytes()
            return {
                "status": "ok",
                "payload_size": len(payload),
                "index": index,
            }, payload
        raise PeerError("operacao desconhecida")

    def _fetch_block(
        self,
        index: int,
        sources: list[tuple[str, int]],
    ) -> bool:
        if not sources:
            return False
        offset = index % len(sources)
        ordered_sources = sources[offset:] + sources[:offset]
        for address in ordered_sources:
            try:
                response, block = _rpc(
                    address,
                    {"op": "get", "file_id": self.metadata["file_id"], "index": index},
                )
                if response.get("index") != index:
                    raise PeerError("resposta contem indice de bloco inesperado")
                if len(block) != self._expected_block_size(index):
                    raise PeerError("bloco recebido possui tamanho incorreto")
                if hashlib.sha256(block).hexdigest() != self.metadata["block_hashes"][index]:
                    raise PeerError("hash do bloco recebido nao corresponde")
                _atomic_write(self._block_path(index), block)
                with self._lock:
                    self._available[index] = True
                LOGGER.info("bloco %s/%s recebido de %s:%s", index + 1, self.metadata["block_count"], *address)
                return True
            except (ConnectionError, OSError, PeerError, ValueError) as error:
                LOGGER.warning("falha ao obter bloco %s de %s:%s: %s", index, *address, error)
        return False

    def download(self, output: Path, timeout: float, workers: int) -> None:
        if not self.neighbors:
            raise PeerError("configure ao menos um vizinho para baixar o arquivo")
        deadline = time.monotonic() + timeout if timeout > 0 else None
        with ThreadPoolExecutor(max_workers=workers) as executor:
            while True:
                available = self.have()
                missing = [index for index, present in enumerate(available) if not present]
                if not missing:
                    break
                if deadline is not None and time.monotonic() >= deadline:
                    raise PeerError(f"tempo limite atingido; ainda faltam {len(missing)} blocos")

                sources_by_block: dict[int, list[tuple[str, int]]] = {}
                for address in self.neighbors:
                    try:
                        response, _ = _rpc(
                            address,
                            {"op": "have", "file_id": self.metadata["file_id"]},
                        )
                        if response.get("file_id") != self.metadata["file_id"]:
                            raise PeerError("vizinho informou outro arquivo")
                        blocks = response.get("blocks")
                        if not isinstance(blocks, list) or len(blocks) != len(available):
                            raise PeerError("bitmap de blocos invalido")
                        for index, has_block in enumerate(blocks):
                            if has_block is True and not available[index]:
                                sources_by_block.setdefault(index, []).append(address)
                    except (ConnectionError, OSError, PeerError, ValueError) as error:
                        LOGGER.warning("vizinho %s:%s indisponivel: %s", *address, error)

                pending = [
                    index for index in missing if sources_by_block.get(index)
                ]
                if not pending:
                    LOGGER.info("aguardando blocos; possui %s de %s", sum(available), len(available))
                    time.sleep(0.5)
                    continue

                batch_size = workers * 4
                for start in range(0, len(pending), batch_size):
                    batch = pending[start : start + batch_size]
                    futures = {
                        executor.submit(self._fetch_block, index, sources_by_block[index]): index
                        for index in batch
                    }
                    for future in as_completed(futures):
                        future.result()

        self._assemble(output)

    def _assemble(self, output: Path) -> None:
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary_path: Path | None = None
        file_hash = hashlib.sha256()
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", dir=output.parent, prefix=f".{output.name}.", delete=False
            ) as temporary:
                temporary_path = Path(temporary.name)
                for index in range(self.metadata["block_count"]):
                    block = self._block_path(index).read_bytes()
                    if (
                        len(block) != self._expected_block_size(index)
                        or hashlib.sha256(block).hexdigest() != self.metadata["block_hashes"][index]
                    ):
                        raise PeerError(f"bloco {index} invalido durante a remontagem")
                    temporary.write(block)
                    file_hash.update(block)
            if file_hash.hexdigest() != self.metadata["sha256"]:
                raise PeerError("SHA-256 do arquivo remontado nao corresponde ao original")
            if temporary_path.stat().st_size != self.metadata["file_size"]:
                raise PeerError("tamanho do arquivo remontado nao corresponde ao original")
            os.replace(temporary_path, output)
            temporary_path = None
            LOGGER.info(
                "arquivo remontado em %s (%s bytes, SHA-256 %s)",
                output,
                self.metadata["file_size"],
                file_hash.hexdigest(),
            )
        finally:
            if temporary_path is not None and temporary_path.exists():
                temporary_path.unlink()


def _configure_logging(peer_id: str) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [{peer_id}] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )


def _add_network_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--host", default="127.0.0.1", help="IP/endereco local (padrao: 127.0.0.1)")
    parser.add_argument("--port", type=int, required=True, help="porta TCP local")
    parser.add_argument(
        "--neighbors",
        type=parse_address,
        nargs="*",
        default=[],
        metavar="HOST:PORT",
        help="enderecos dos Peers vizinhos",
    )
    parser.add_argument("--storage", type=Path, default=Path("peer_data"), help="diretorio para blocos")
    parser.add_argument("--id", dest="peer_id", help="identificador usado nos logs")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Transferencia de arquivos P2P em blocos.")
    subparsers = parser.add_subparsers(dest="mode", required=True)

    seed_parser = subparsers.add_parser("seed", help="inicia um Peer com o arquivo completo")
    seed_parser.add_argument("--file", type=Path, required=True, help="arquivo original")
    seed_parser.add_argument("--metadata", type=Path, required=True, help="caminho para salvar metadados JSON")
    seed_parser.add_argument("--block-size", type=int, default=DEFAULT_BLOCK_SIZE)
    _add_network_arguments(seed_parser)

    peer_parser = subparsers.add_parser("peer", help="inicia um Leecher/Seeder que baixa do swarm")
    peer_parser.add_argument("--metadata", type=Path, required=True, help="metadados JSON copiados do Seeder")
    peer_parser.add_argument("--output", type=Path, help="caminho do arquivo remontado")
    peer_parser.add_argument("--timeout", type=float, default=300.0, help="limite em segundos; 0 espera indefinidamente")
    peer_parser.add_argument("--workers", type=int, default=8, help="maximo de requisicoes simultaneas")
    _add_network_arguments(peer_parser)
    return parser


def run(args: argparse.Namespace) -> None:
    if not 1 <= args.port <= 65535:
        raise PeerError("a porta deve estar entre 1 e 65535")
    if args.mode == "seed":
        metadata = create_metadata(args.file, args.block_size)
        save_metadata(args.metadata, metadata)
        LOGGER.info(
            "metadados salvos em %s: %s blocos, %s bytes",
            args.metadata,
            metadata["block_count"],
            metadata["file_size"],
        )
        seed_file: Path | None = args.file
        output = None
    else:
        metadata = load_metadata(args.metadata)
        if args.workers <= 0:
            raise PeerError("workers deve ser maior que zero")
        if args.timeout < 0:
            raise PeerError("timeout nao pode ser negativo")
        seed_file = None
        output = args.output or Path(Path(metadata["filename"]).name)

    peer = Peer(
        host=args.host,
        port=args.port,
        metadata=metadata,
        storage=args.storage,
        neighbors=args.neighbors,
        peer_id=args.peer_id,
        seed_file=seed_file,
    )
    peer.start()
    try:
        if args.mode == "peer":
            peer.download(output, args.timeout, args.workers)
        LOGGER.info("Peer ativo; pressione Ctrl+C para encerrar")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        LOGGER.info("encerrando Peer")
    finally:
        peer.stop()


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    _configure_logging(args.peer_id or f"{args.host}:{args.port}")
    try:
        run(args)
    except (PeerError, OSError) as error:
        LOGGER.error("%s", error)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
