# Raft Consensus Algorithm

Учебная имплементация алгоритма консенсуса Raft на Go. Пишу для глубокого понимания принципов работы распределенных систем, обеспечения отказоустойчивости и сетевого взаимодействия на низком уровне.

## Архитектура
Проект логически разделен на ядро консенсуса и подключаемые слои (транспорт, хранилище):
* `internal/raft/` — стейт-машина (Leader, Follower, Candidate), обработка таймаутов выборов (Election) и хартбитов (Heartbeats).
* `internal/transport/` — слой межсервисного общения (обработка RPC `AppendEntries` и `RequestVote`).
* `internal/storage/` — персистентное хранилище состояния узла (терм, голос) и логов команд (Write-Ahead Log).

## Стек
* **Язык:** Go (1.22+)
* **Сеть:** gRPC / нативные TCP-сокеты (WIP)
* **Синхронизация:** каналы, select, мьютексы, atomic-операции.

```mermaid
graph TD
    CMD["📂 cmd/example_kv/<br><small><i>точка входа, wire-up</i></small>"]
    RAFT["⚙️ internal/raft/<br><small><i>ядро: state machine</i></small>"]
    TRANS["🌐 internal/transport/<br><small><i>интерфейс RPC (сеть)</i></small>"]
    STOR["💾 internal/storage/<br><small><i>интерфейс персистентности</i></small>"]

    CMD --> RAFT
    RAFT --> TRANS
    RAFT --> STOR

    classDef default fill:#f9f9f9,stroke:#333,stroke-width:1px,color:#333;
    classDef core fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#000;

    class RAFT core;
```
## Текущий статус (WIP)
- [x] Разбор оригинальной пейперы (In Search of an Understandable Consensus Algorithm).
- [x] Проектирование архитектуры слоев (Транспорт, Лог, Стейт-машина).
- [ ] Имплементация механизма Leader Election (рандомизация таймаутов, подсчет голосов).
- [ ] Имплементация Log Replication (рассылка логов фолловерам, фиксация коммитов).
- [ ] Интеграция с подсистемой хранения (WAL).