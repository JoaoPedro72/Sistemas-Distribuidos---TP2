# Checklist passo a passo — Trabalho Prático 2: Transferência de Arquivos P2P

## 1. Preparação e decisões de projeto

- [X] Escolher a linguagem de programação.
- [X] Definir a estrutura do projeto e como executá-lo.
- [X] Definir o tamanho padrão dos blocos: **1024 bytes (1 KB)**.
- [X] Definir como cada Peer será configurado: endereço IP, porta e lista estática de vizinhos.
- [X] Decidir se será usado um Tracker. Ele é **opcional**; a lista predefinida de vizinhos é suficiente.
- [X] Definir o formato dos metadados do arquivo: nome, tamanho total, tamanho do bloco, quantidade de blocos e checksum.
- [X] Definir o protocolo de mensagens entre Peers: como solicitar e responder com um bloco, identificar erros e informar metadados.

## 2. Implementação

- [X] Implementar o processo Peer para atuar simultaneamente como **servidor e cliente**.
- [X] Criar o servidor Socket que escuta na porta configurada e atende solicitações de blocos.
- [X] Implementar a conexão do Leecher com todos os vizinhos configurados.
- [X] Implementar a fragmentação do arquivo do Seeder em blocos de tamanho fixo.
- [X] Implementar o envio de metadados necessários para identificar e reconstruir o arquivo.
- [X] Implementar a solicitação de blocos que ainda não foram recebidos.
- [X] Implementar o recebimento, a validação e o armazenamento dos blocos.
- [X] Manter um registro local dos blocos disponíveis e pendentes em cada Peer.
- [X] Permitir que um Peer compartilhe cada bloco assim que o recebe, sem esperar o download completo.
- [X] Implementar o tratamento de conexões simultâneas e de falhas de comunicação.
- [X] Ao receber todos os blocos, remontar e salvar o arquivo final.
- [X] Registrar nos logs as conexões, solicitações, envios e recebimentos de blocos.

## 3. Testes básicos e verificação

- [X] Criar um arquivo de teste com conteúdo único (feito pelo teste automatizado).
- [X] Preparar o Seeder inicial com o arquivo completo e seus metadados (feito pelo teste automatizado).
- [X] Iniciar pelo menos **2 Peers**, com endereços, portas e vizinhos configurados (teste local automatizado).
- [X] Confirmar que o Leecher encontra o vizinho e solicita os blocos necessários.
- [X] Confirmar que o arquivo final é remontado e salvo corretamente.
- [X] Comparar o tamanho do arquivo baixado com o original.
- [X] Calcular o checksum SHA-256 do original e do arquivo baixado e confirmar que são iguais.
- [X] Verificar nos logs a origem dos blocos recebidos.

## 4. Estudos de caso

Use a configuração padrão como referência e, em seguida, varie os parâmetros para avaliar seus efeitos.

- [ ] **Arquivo pequeno:** testar **10 KB** e repetir com **20 KB**.
- [ ] **Arquivo médio:** testar **1 MB** e repetir com **5 MB**.
- [ ] **Arquivo grande:** testar **10 MB** e repetir com **20 MB**.
- [ ] Repetir os testes com blocos de **4096 bytes (4 KB)** e comparar com o padrão de 1024 bytes.
- [ ] Repetir os testes com **4 Peers** e comparar com a configuração mínima de 2.
- [ ] Para cada execução, registrar o tamanho do arquivo, o tamanho dos blocos, o número de Peers, o resultado da integridade e observações relevantes.
- [ ] Nos testes com 4 Peers, verificar se os Leecher que já receberam blocos também os servem para os demais.
- [ ] Nos arquivos maiores, conferir pelos logs que os blocos foram recebidos das fontes esperadas.
- [X] Investigar e corrigir falhas de transferência, divergências de checksum ou erros de remontagem encontrados (validação coberta por testes).

## 5. Relatório e entrega

- [X] Descrever as decisões de projeto e implementação em [README.md](README.md).
- [X] Explicar o protocolo, os metadados, o gerenciamento dos blocos e a remontagem do arquivo em [README.md](README.md).
- [ ] Apresentar os resultados dos estudos de caso e a avaliação das variações.
- [ ] Incluir evidências de integridade, como comparação de tamanho e checksum.
- [ ] Incluir observações dos logs sobre o compartilhamento entre Peers.
- [ ] Incluir a URL do código-fonte.
- [ ] Conferir que o relatório tem **no máximo 5 páginas**.
- [X] Fazer uma execução final do programa em múltiplos processos seguindo as instruções de configuração e uso descritas no projeto (Seeder e Leecher em processos separados; transferência e SHA-256 conferidos).