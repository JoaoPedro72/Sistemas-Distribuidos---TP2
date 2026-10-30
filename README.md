# Transferencia P2P de arquivos

Implementacao em Python 3, usando somente a biblioteca padrao. Cada processo Peer
mantem um servidor TCP ativo enquanto solicita blocos dos vizinhos configurados.
O Seeder grava um arquivo de metadados JSON; copie esse arquivo para os demais
Peers antes de inicia-los.

## Preparar e iniciar o Seeder

```powershell
python peer.py seed --file FileA.bin --metadata FileA.json --host 127.0.0.1 --port 5001
```

O Seeder usa blocos de 1024 bytes por padrao. Para usar 4096 bytes, acrescente
`--block-size 4096`. O comando cria os metadados, armazena os blocos e mantem o
servidor em execucao. Encerre com `Ctrl+C`.

## Iniciar um Leecher

Copie `FileA.json` para a pasta do Peer e execute:

```powershell
python peer.py peer --metadata FileA.json --host 127.0.0.1 --port 5002 --neighbors 127.0.0.1:5001 --output downloads/FileA.bin
```

O Leecher inicia seu servidor antes de baixar. Assim, os blocos recebidos ficam
disponiveis imediatamente para outros Peers. Depois de concluir e validar o
arquivo, ele continua ativo para servir os blocos; encerre-o com `Ctrl+C`.
Para quatro processos, escolha uma
porta diferente para cada um e informe em `--neighbors` os enderecos dos Peers
que cada processo pode acessar. Todos devem usar os metadados gerados pelo
Seeder. No PowerShell, cada processo pode ser executado em uma janela separada.

Opcionalmente, `--timeout 0` aguarda sem limite de tempo e `--workers 12` altera
o numero maximo de requisicoes simultaneas. O programa registra conexoes,
transferencias e verificacoes no terminal.

## Protocolo e integridade

Cada mensagem comeca com o tamanho do cabecalho JSON em 4 bytes (inteiro de rede).
O cabecalho de uma resposta tambem informa o tamanho do payload binario. As
operacoes sao `have` (bitmap dos blocos disponiveis) e `get` (solicitacao de um
bloco); `metadata` retorna os metadados do arquivo pelo protocolo. Os metadados
contem tamanho, quantidade de blocos, SHA-256 do arquivo e
SHA-256 de cada bloco. O Peer valida cada bloco recebido e confere novamente o
tamanho e o SHA-256 do arquivo ao remontar.

Os blocos ficam em `peer_data/<file_id>/`, permitindo retomar blocos validados
após reiniciar o processo. O arquivo final so substitui o destino depois da
validacao de tamanho e checksum.

## Testes

```powershell
python -m unittest -v
```

## Estudos de caso

```powershell
python estudos_de_caso.py --repeat 3 --work-dir C:\Temp\p2p
```

O script executa as 24 combinacoes do enunciado (arquivos de 10 KB a 20 MB,
blocos de 1024 e 4096 bytes, 2 e 4 Peers), cada Peer em um processo separado.
Para cada execucao ele mede o tempo de download, confere tamanho e SHA-256 e
conta nos logs de qual Peer veio cada bloco. Os resultados ficam em
`resultados/` (`resultados.csv`, `resultados.json` e `resumo.md`) e os logs de
cada Peer em `--work-dir`. Use um `--work-dir` com caminho curto: no Windows,
`peer_data/<sha256>/block-*.bin` pode ultrapassar o limite de 260 caracteres.

Topologia com 4 Peers: P1 e o Seeder; P2 tem P1 como vizinho; P3 tem P1 e P2;
P4 tem P2 e P3. Como P4 nao conhece o Seeder, todos os blocos que recebe foram
repassados por Leechers.
