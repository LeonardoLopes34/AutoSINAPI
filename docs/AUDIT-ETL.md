# 🔧 Auditoria de Orientação — Camada ETL (AutoSINAPI)

> Documento de orientação da camada **ETL** (open source, submodule `AutoSINAPI v0.5.0-beta.0`).
> Parte do ecossistema AutoSINAPI auditado em `repos/mundoaec/docs/audits/autosinapi/2026-07-17_AUDIT-GERAL.md`
> (repos independentes; caminho relativo à raiz do SistemaServerLight).
> Segue o padrão DDD/Hexagonal já adotado em `docs/architecture.md`. Contém as seções
> **Audit · Epic · Spec-Rule · Sprint · Story** para guiar o desenvolvimento.

---

## 1. Audit (ETL)

### 1.1 Coesão
- `etl_pipeline.py` (~485 linhas) é *god-orchestrator*: mistura setup de logs, descoberta de
  filesystem (`_discover_local_files`, `_find_and_normalize_zip`), lógica de placeholder de itens
  faltantes (~70 linhas) e ordenação de load. A lógica de placeholder/integridade referencial
  pertence ao `processor`, não ao orquestrador.
- Imports mortos: `re`/`shutil` redeclarados dentro de método (`etl_pipeline.py:173-174`); `import
  traceback as tb_mod` nunca usado (`:196`); `downloader._build_url()` chamado só por efeito colateral
  descartado (`:290-291`).
- Artefatos de build commitados: `AutoSINAPI/build/lib/...` e `autosinapi.egg-info/`.
- Orphan scripts na raiz do repo pai: `reprocess_history.py`, `update_requirements.py`.

### 1.2 Coerência
- 🔴 **Status vocabulary incompatível com a ponte Celery** (verificado): o pipeline retorna
  `STATUS_FAILURE="FALHA"` (`config.py:42-44`), mas `api/tasks.py:55` checa
  `result.get("status") == "failed"`. Toda falha de ETL é reportada como sucesso; `max_retries=3`
  nunca dispara e o branch de retry 429 (`:58-59`) é inalcançável. Falhas silenciosas.
- 🔴 **`origem_preco`/`percentual_mo` ausentes no alembic** (verificado): escritos pelo
  `processor.py:346-347,518`, lidos por `api/crud.py:62,83,107,128` e expostos em `schemas.py:53`,
  mas **nenhuma migration do alembic** os cria (grep vazio). O upsert do ETL quebra com
  `column "origem_preco" does not exist`; endpoints de insumo/composição retornam 500.
- Duas fontes de verdade de schema: `database.py:create_tables()` (DDL do ETL) vs `alembic/versions/*`.
  Já divergem: `manutencoes_historico.descricao_item` existe no alembic mas não no ETL; tipos
  (`VARCHAR` vs `String(10)`, `NUMERIC` vs `Numeric(12,6)`) differ. `create_tables()` é destrutivo
  (`DROP ... CASCADE` + `CREATE`) e só roda se as tabelas não existirem — armadilha latente.

### 1.3 Resiliência
- 🔴 **Sem transação cross-tabela no load**: cada tabela no loop (`etl_pipeline.py:474-480`) chama
  `db.save_data`, que abre conexão própria e commita isoladamente (`database.py:223,236-240`). Erro em
  N deixa estado parcial persistido; re-run é seguro só por ser UPSERT, mas o bug de status (§1.2)
  impede o re-run automático.
- Download failure tratado como "sem dados": `DownloadError` é logado e engolido, retornando
  `STATUS_SUCCESS_NO_DATA` (`etl_pipeline.py:304-309`) — falha de rede transitória parece sucesso.
- Header rows fixos frágeis a drift da fonte SINAPI: `COMPOSICAO_ITENS_HEADER_ROW=9`,
  `PRECOS_HEADER_ROW=9` (`config.py:79-80`); só custos/manutenção usam `_find_header_row` dinâmico.
- Sem retry/resume no downloader: `downloader.py:100-121` faz `requests.get(timeout=30)` único; zips
  grandes carregados inteiros em `BytesIO` (sem streaming).

### 1.4 Robustez
- Parsing numérico inconsistente: `_unpivot_data` usa `pd.to_numeric(value, errors="coerce")`
  (`processor.py:155`) sem tratar formato BR `1.234,56`; coeficiente trata BR (`processor.py:241`),
  mas preços não — valor BR vira `NaN` e some em `dropna`.
- Encoding não fixado: `pd.read_csv(csv_path, sep=";")` (`processor.py:366`) sem `encoding=`.
- Date parse silencioso: `pd.to_datetime(..., errors="coerce")` produz `NaT` sem contagem/aviso.
- Workbook carregado inteiro em memória (`process_catalogo_e_precos`, `process_composicao_itens`) —
  risco de RAM em arquivo de referência muito grande.
- `except Exception` por sheet (ok) e blanket no run (ok); sem bare `except:`.

### 1.5 Compatibilidade
- Deps não pinadas: `requirements.txt` e `pyproject.toml` com nomes nus (sem floor de versão).
- 🔴 `psycopg2` ausente no metadata instalável (`pyproject.toml`) mas exigido por `database.py:31-34`
  (URL `postgresql://`); só funciona porque os Dockerfiles instalam `psycopg2-binary` à parte.
  `pip install autosinapi` quebra na criação do engine.
- Inconsistência de Python: `requires-python=">=3.8"` (setup/pyproject) mas API usa `str | None`
  (3.10+); imagens `python:3.9` (ETL) vs `python:3.10` (API).
- ✅ Postgres 15 OK; Celery wiring coerente (`celery_config.py` com `acks_late`,
  `task_reject_on_worker_lost`, `worker_max_tasks_per_child=10`, `task_time_limit=5400`).

### 1.6 Segurança
- ✅ Sem `eval/exec/os.system/subprocess/pickle`; sem SQLi (queries parametrizadas; identificadores
  sanitizados em `_normalize_cols`); sem segredos hardcoded (DB URL mascarado em log).
- Baixo: SSRF mínimo (`_build_url` só com constantes + `VALID_TYPES`); CSVs temporários sem permissão
  restrita.

---

## 2. Epic (ETL)

**`EPIC-ETL-REMEDIATION`** — Tornar o ETL coerente, resiliente e reproduzível: unificar contrato de
status com a API, eliminar drift de schema (fonte única de verdade no alembic), tornar o load
atômico/observável e fixar reproducibilidade de build.

---

## 3. Spec-Rule (ETL) — regras vinculantes

> Regras derivadas dos achados. Devem ser satisfeitas por todo PR nesta camada e validadas em CI.

- **SR-ETL-1 (Coerência):** O ETL e a ponte Celery **devem** usar um único enum de status canônico
  (`"success" | "failure" | "no_data"`). `tasks.py` **não** pode checar string em PT-BR.
- **SR-ETL-2 (Coerência/Schema):** A fonte única de verdade do schema é o **alembic** (API). Todo
  campo escrito/lido pelo ETL (`origem_preco`, `percentual_mo`, etc.) **deve** ter migration
  correspondente. O `create_tables()` do ETL é proibido em runtime (apenas dev/offline) e deve
  espelhar exatamente o alembic.
- **SR-ETL-3 (Resiliência):** O load de todas as tabelas de uma competência **deve** ocorrer dentro de
  **uma transação** (ou com audit row marcada `failed` em caso de exceção) — nunca commit parcial
  silencioso.
- **SR-ETL-4 (Resiliência):** Falha de download **deve** ser sinalizada como `failure` (não
  `no_data`); retries de rede com backoff antes de desistir.
- **SR-ETL-5 (Robustez):** Parsing numérico **deve** normalizar formato BR (`1.234,56` → `1234.56`)
  em todas as colunas numéricas; encoding **deve** ser declarado (`cp1252` fallback). Linhas
  descartadas por `dropna`/`coerce` **devem** ser contabilizadas e logadas.
- **SR-ETL-6 (Compatibilidade):** `requirements.txt` e `pyproject.toml` **devem** pinar faixas de
  versão; `psycopg2-binary` (ou `psycopg[binary]`) **deve** estar em `install_requires`.
- **SR-ETL-7 (Coesão/DRY):** Orquestrador (`etl_pipeline.py`) não deve conter regras de negócio de
  placeholder/integridade — mover para `processor`. Remover imports mortos e artefatos de build do VCS.
- **SR-ETL-8 (Segurança):** Manter 100% de queries parametrizadas; nunca introduzir `eval/exec/pickle`.

---

## 4. Sprint (ETL)

### P0 (bloqueadores)
- **Sprint ETL-P0-A — Contrato de status:** unificar enum ETL↔Celery (SR-ETL-1). Garante retries e
  detecção de falha. Baixo risco.
- **Sprint ETL-P0-B — Schema drift:** criar migration `origem_preco`/`percentual_mo`; neutralizar/
  espelhar `create_tables()` (SR-ETL-2). Remove 500 em insumo/composição.
- **Sprint ETL-P0-C — Load atômico:** envolver Phase-3 em transação única + audit row (SR-ETL-3).

### P1
- **Sprint ETL-P1-A — Resiliência de download:** retry/backoff + `failure` em erro de rede (SR-ETL-4);
  streaming de zip grande.
- **Sprint ETL-P1-B — Robustez de parsing:** normalização BR + encoding declarado + métricas de drop
  (SR-ETL-5).
- **Sprint ETL-P1-C — Reproducibilidade:** pin de deps + `psycopg2` no metadata (SR-ETL-6).

### P2
- **Sprint ETL-P2-A — Coesão:** refactoring do orquestrador; remover código morto/artefatos (SR-ETL-7).
- **Sprint ETL-P2-B — Header dinâmico:** estender `_find_header_row` para todas as planilhas (mitigar
  drift da fonte).

---

## 5. Story (ETL)

### STORY-ETL-001 — Contrato de status canônico
- **Como** mantenedor do ETL, **quero** um enum de status compartilhado com a API, **para** que falhas
  de ETL disparem retry e sejam visíveis.
- **Critérios de Aceite:**
  - [ ] `config.py` define `STATUS_SUCCESS | STATUS_FAILURE | STATUS_NO_DATA` (inglês, minúsculo).
  - [ ] `etl_pipeline.py` retorna essas chaves (não `"FALHA"`/`"SUCESSO"`).
  - [ ] `api/tasks.py:55` checa `== "failure"` e o branch 429 funciona.
  - [ ] Teste unitário cobre os 3 estados e o retry.

### STORY-ETL-002 — Schema como fonte única (alembic)
- **Como** DBA, **quero** que o alembic seja a única verdade de schema, **para** que ETL e API concordem.
- **Critérios de Aceite:**
  - [ ] Migration cria `origem_preco` e `percentual_mo` nas tabelas corretas.
  - [ ] `database.py:create_tables()` removido do path de runtime (ou valida igualdade com alembic).
  - [ ] CI executa `alembic upgrade head` contra Postgres real e roda o ETL end-to-end.

### STORY-ETL-003 — Load atômico e observável
- **Como** SRE, **quero** load de competência em transação única, **para** evitar estado parcial.
- **Critérios de Aceite:**
  - [ ] Falha em qualquer tabela faz rollback das anteriores (ou audit row = `failed`).
  - [ ] Contagem de linhas carregadas por tabela é logada.

### STORY-ETL-004 — Parsing numérico BR resiliente
- **Como** consumidor de dados, **quero** preços no formato BR normalizados, **para** não perder valores.
- **Critérios de Aceite:**
  - [ ] `1.234,56` → `1234.56` em todas as colunas numéricas.
  - [ ] Encoding declarado; linhas descartadas contabilizadas em log.

### STORY-ETL-005 — Build reproduzível
- **Como** DevOps, **quero** deps pinadas e driver no metadata, **para** que `pip install` funcione.
- **Critérios de Aceite:**
  - [ ] `pyproject.toml` declara faixas e `psycopg2-binary`.
  - [ ] Build em ambiente limpo instala e importa sem erro de driver.
