@echo off

start "Peer 1" /D "%~dp0peer1" python peer.py seed --file NOME_DO_ARQUIVO_AQUI --metadata ..\File.json --host 127.0.0.1 --port 5001

start "Peer 2" /D "%~dp0peer2" python peer.py peer --metadata ..\File.json --host 127.0.0.1 --port 5002 --neighbors 127.0.0.1:5001 --output NOME_DO_ARQUIVO_AQUI

start "Peer 3" /D "%~dp0peer3" python peer.py peer --metadata ..\File.json --host 127.0.0.1 --port 5003 --neighbors 127.0.0.1:5002 --output NOME_DO_ARQUIVO_AQUI

start "Peer 4" /D "%~dp0peer4" python peer.py peer --metadata ..\File.json --host 127.0.0.1 --port 5004 --neighbors 127.0.0.1:5002 --output NOME_DO_ARQUIVO_AQUI

start "Peer 5" /D "%~dp0peer5" python peer.py peer --metadata ..\File.json --host 127.0.0.1 --port 5005 --neighbors 127.0.0.1:5003 127.0.0.1:5004 --output NOME_DO_ARQUIVO_AQUI

exit