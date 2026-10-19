'''
Código desenvolvido por: André Luiz Souza Santos
Contato: andrezluz19@gmail.com

Versão revisada (out/2026):
- Removido `import spark` (quebrava o import do módulo)
- Tabela de governança com schema fixo e consistente com o buffer da BaseDataProcessorSpark
  (+ colunas column_description e last_update_time), com migração automática de tabelas antigas
- Carga do buffer via MERGE (upsert por catálogo/schema/tabela/coluna): o dicionário guarda a
  versão mais recente de cada coluna, preserva descrições e não duplica linhas
- Modelos de IA configuráveis ou descobertos automaticamente nos serving endpoints do workspace
- Escape correto de aspas nos comentários gerados por IA e identificadores entre crases
- fetch_table_descriptions não sobrescreve mais a origem (notebook) com o notebook do builder
'''
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os
import uuid

# Colunas gravadas pelo BaseDataProcessorSpark no buffer (mesma ordem/nomes)
BUFFER_COLUMNS = [
    "catalog", "schema", "table", "column", "collumn_dtype", "creation_date", "creation_time",
    "table_description", "user", "file_fetched", "source_notebook_url"
]
# Colunas extras mantidas apenas na tabela de governança
EXTRA_COLUMNS = ["column_description", "last_update_time"]
GOVERNANCE_COLUMNS = BUFFER_COLUMNS + EXTRA_COLUMNS
KEY_COLUMNS = ["catalog", "schema", "table", "column"]

# Lista de reserva caso a descoberta automática de endpoints falhe.
# Os nomes dos modelos da Databricks mudam com o tempo: confira em "Serving" no seu workspace
# e passe a lista correta em `ai_models` se nenhum destes existir.
DEFAULT_AI_MODELS = [
    "databricks-meta-llama-3-3-70b-instruct",
    "databricks-llama-4-maverick",
    "databricks-gpt-oss-120b",
    "databricks-gemma-3-12b",
    "databricks-meta-llama-3-1-8b-instruct",
]

# Descrições fixas para as colunas técnicas criadas pelo export_to_db (economiza chamadas de IA)
STANDARD_COLUMN_DESCRIPTIONS = {
    "write_date": "Data (AAAA-MM-DD) em que o registro foi gravado pelo pipeline de ingestão.",
    "write_time": "Hora (HH:MM:SS) em que o registro foi gravado pelo pipeline de ingestão.",
}


def _sql_str(valor):
    """Escapa um valor para uso dentro de uma string SQL com aspas simples."""
    return str(valor).replace("\\", "\\\\").replace("'", "\\'")


def _q(*partes):
    """Monta um identificador SQL com crases: `catalogo`.`schema`.`tabela`."""
    return ".".join(f"`{str(p).replace('`', '``')}`" for p in partes)


class DataDictionaryBuilder:
    def __init__(self, target_catalog, target_schema, target_table, buffer_path=None, ai_models=None):
        """
        Parâmetros:
        - target_catalog / target_schema: onde fica a tabela de governança. As descrições por IA
          são geradas para as tabelas deste mesmo catálogo/schema.
        - target_table: nome da tabela de governança (dicionário de dados).
        - buffer_path: caminho do buffer gravado pelo BaseDataProcessorSpark
          (o mesmo valor de `buffer_log_path` / '<output_directory>/export_log_buffer_.txt').
        - ai_models: lista opcional de serving endpoints para ai_query, em ordem de preferência.
          Se omitida, os endpoints de chat do workspace são descobertos automaticamente.
        """
        self.spark = SparkSession.builder.getOrCreate()
        self.buffer_path = buffer_path
        self.target_catalog = target_catalog
        self.target_schema = target_schema
        self.target_table = target_table
        self.ai_models = list(ai_models) if ai_models else None
        self._modelo_ia = None

    # ------------------------------------------------------------------
    # Infraestrutura
    # ------------------------------------------------------------------

    @property
    def governance_table(self):
        return f"{self.target_catalog}.{self.target_schema}.{self.target_table}"

    def _governance_table_sql(self):
        return _q(self.target_catalog, self.target_schema, self.target_table)

    def create_governance_infrastructure(self):
        """
        Cria o schema e a tabela de governança, caso não existam, com schema fixo.
        Se a tabela já existir (criada por uma versão anterior), adiciona as colunas que faltam.
        """
        try:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_q(self.target_catalog, self.target_schema)}")
            print(f"[OK] Schema '{self.target_catalog}.{self.target_schema}' garantido.")

            colunas_ddl = ",\n                    ".join(f"{_q(c)} STRING" for c in GOVERNANCE_COLUMNS)
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self._governance_table_sql()} (
                    {colunas_ddl}
                )
                USING delta
                COMMENT 'Dicionário de dados: uma linha por coluna das tabelas exportadas pelos pipelines.'
            """)

            existentes = {c.lower() for c in self.spark.table(self.governance_table).columns}
            faltantes = [c for c in GOVERNANCE_COLUMNS if c.lower() not in existentes]
            if faltantes:
                adicionar = ", ".join(f"{_q(c)} STRING" for c in faltantes)
                self.spark.sql(f"ALTER TABLE {self._governance_table_sql()} ADD COLUMNS ({adicionar})")
                print(f"[OK] Colunas adicionadas à tabela existente: {faltantes}")

            print(f"[OK] Tabela '{self.governance_table}' criada/verificada com sucesso.")
            return True
        except Exception as e:
            print(f"[ERRO] ao criar infraestrutura no catálogo '{self.target_catalog}': {e}")
            return False

    # ------------------------------------------------------------------
    # Carga do buffer
    # ------------------------------------------------------------------

    def load_buffer_to_governance_table(self):
        """
        Lê o buffer de log e faz MERGE na tabela de governança (uma linha por coluna):
        - coluna nova → inserida;
        - coluna já conhecida → atualiza tipo, usuário, origem e last_update_time, preservando
          creation_date/creation_time e as descrições.
        Após o sucesso, limpa o buffer mantendo apenas o cabeçalho.
        """
        if not self.buffer_path or not os.path.exists(self.buffer_path) or os.path.getsize(self.buffer_path) < 10:
            print(f"[INFO] Nenhum arquivo de buffer para processar em {self.buffer_path}.")
            return 0

        if not self.create_governance_infrastructure():
            return 0

        try:
            with open(self.buffer_path, "r", encoding="utf-8") as f:
                header_line = f.readline().strip()

            bruto = self.spark.read.option("header", "true").option("sep", ";").csv(self.buffer_path)
            colunas_buffer = set(bruto.columns)
            fonte = bruto.select(*[
                (F.col(f"`{c}`") if c in colunas_buffer else F.lit(None)).cast("string").alias(c)
                for c in BUFFER_COLUMNS
            ]).where(F.col("table").isNotNull() & F.col("column").isNotNull())

            # Se a mesma coluna aparece várias vezes no buffer, fica a ocorrência mais recente
            janela = Window.partitionBy(*KEY_COLUMNS).orderBy(F.col("creation_time").desc_nulls_last())
            fonte = fonte.withColumn("_rn", F.row_number().over(janela)).where("_rn = 1").drop("_rn")

            total = fonte.count()
            if total == 0:
                print("[INFO] Buffer não contém linhas de dados para carregar.")
                return 0

            view = f"_buffer_governanca_{uuid.uuid4().hex[:8]}"
            fonte.createOrReplaceTempView(view)

            condicao = " AND ".join(f"t.{_q(c)} = s.{_q(c)}" for c in KEY_COLUMNS)
            atualizar = ", ".join(f"t.{_q(c)} = s.{_q(c)}"
                                  for c in ["collumn_dtype", "user", "file_fetched", "source_notebook_url"])
            colunas_insert = ", ".join(_q(c) for c in GOVERNANCE_COLUMNS)
            valores_insert = ", ".join(
                [f"s.{_q(c)}" for c in BUFFER_COLUMNS] + ["NULL", "s.`creation_time`"]
            )
            self.spark.sql(f"""
                MERGE INTO {self._governance_table_sql()} AS t
                USING {view} AS s
                ON {condicao}
                WHEN MATCHED THEN UPDATE SET {atualizar}, t.`last_update_time` = s.`creation_time`
                WHEN NOT MATCHED THEN INSERT ({colunas_insert}) VALUES ({valores_insert})
            """)
            self.spark.catalog.dropTempView(view)
            print(f"[OK] {total} coluna(s) do buffer consolidadas em {self.governance_table}")

            try:
                with open(self.buffer_path, "w", encoding="utf-8") as f:
                    f.write(header_line + "\n")
                print(f"[OK] Arquivo de buffer '{self.buffer_path}' foi limpo, mantendo apenas o cabeçalho.")
            except Exception as e_clean:
                print(f"[ALERTA] Os dados foram carregados, mas falha ao limpar o arquivo de buffer: {e_clean}")
            return total

        except Exception as e:
            print(f"[ERRO] Falha ao carregar dados do buffer. O arquivo NÃO será limpo. Erro: {e}")
            return 0

    def drop_duplicate_rows(self):
        """
        Remove linhas duplicadas da tabela de governança, mantendo o registro mais recente
        de cada coluna (útil para limpar tabelas carregadas pela versão anterior, em append).
        """
        df = self.spark.table(self.governance_table)
        ordem = F.coalesce(F.col("last_update_time"), F.col("creation_time")) \
            if "last_update_time" in df.columns else F.col("creation_time")
        janela = Window.partitionBy(*KEY_COLUMNS).orderBy(ordem.desc_nulls_last())
        dedup = df.withColumn("_rn", F.row_number().over(janela)).where("_rn = 1").drop("_rn")

        antes, depois = df.count(), dedup.count()
        if antes == depois:
            print("[OK] Nenhuma duplicata encontrada.")
            return 0
        dedup.write.format("delta").mode("overwrite").saveAsTable(self.governance_table)
        print(f"[OK] {antes - depois} linha(s) duplicada(s) removida(s).")
        return antes - depois

    # ------------------------------------------------------------------
    # Descrições por IA
    # ------------------------------------------------------------------

    def _listar_modelos_candidatos(self):
        if self.ai_models:
            return self.ai_models

        candidatos = []
        try:
            from databricks.sdk import WorkspaceClient
            for ep in WorkspaceClient().serving_endpoints.list():
                tarefa = str(getattr(ep, "task", "") or "")
                if "chat" not in tarefa and "completions" not in tarefa:
                    continue
                pronto = str(getattr(getattr(ep, "state", None), "ready", "") or "")
                if pronto and "NOT_READY" in pronto:
                    continue
                candidatos.append(ep.name)
            # Endpoints de chat primeiro; depois os de completions
            candidatos.sort(key=lambda n: 0 if n.startswith("databricks-") else 1)
            if candidatos:
                print(f"[INFO] Endpoints de LLM encontrados no workspace: {candidatos}")
        except Exception as e:
            print(f"[INFO] Não foi possível listar serving endpoints ({e}); usando lista padrão.")

        return candidatos + [m for m in DEFAULT_AI_MODELS if m not in candidatos]

    def _find_available_ai_model(self):
        """
        Testa os modelos candidatos e retorna o primeiro que responde ao ai_query.
        """
        if self._modelo_ia:
            return self._modelo_ia

        print("[INFO] Buscando um modelo de IA disponível no ambiente...")
        for model_name in self._listar_modelos_candidatos():
            try:
                print(f"  -> Testando modelo: '{model_name}'...")
                self.spark.sql(f"SELECT ai_query('{_sql_str(model_name)}', 'Responda apenas: OK')").collect()
                print(f"  -> [OK] Modelo encontrado e funcionando: '{model_name}'")
                self._modelo_ia = model_name
                return model_name
            except Exception as e:
                print(f"  -> Modelo '{model_name}' indisponível: {str(e).splitlines()[0][:160]}")

        raise Exception("Nenhum modelo de IA disponível. Informe `ai_models` com um endpoint listado em 'Serving'.")

    def _gerar_texto(self, modelo, prompt, limite):
        texto = self.spark.sql(
            f"SELECT ai_query('{_sql_str(modelo)}', '{_sql_str(prompt)}') AS resposta"
        ).first()["resposta"]
        texto = (texto or "").strip().strip('"').strip("'").replace("\n", " ").strip()
        return texto[:limite]

    def generate_and_apply_ai_descriptions(self, sobrescrever=False, incluir_tabela_governanca=False, limite_itens=None):
        """
        Gera descrições com IA e aplica como COMMENT nas tabelas e colunas do schema alvo
        que ainda não têm comentário (ou em todas, se sobrescrever=True).

        Parâmetros:
        - sobrescrever: se True, regera também comentários já existentes.
        - incluir_tabela_governanca: se True, também descreve a própria tabela de governança.
        - limite_itens: máximo de tabelas + colunas processadas (útil para testar/economizar).

        Retorna: (n_tabelas, n_colunas) descritas.
        """
        try:
            modelo = self._find_available_ai_model()
        except Exception as e:
            print(f"[ERRO CRÍTICO] {e}")
            return 0, 0

        info = f"{_q(self.target_catalog)}.information_schema"
        filtro_comentario = "" if sobrescrever else "AND (t.comment IS NULL OR t.comment IN ('', 'N/A'))"
        filtro_gov = "" if incluir_tabela_governanca else \
            f"AND lower(t.table_name) <> lower('{_sql_str(self.target_table)}')"
        processados, n_tab, n_col = 0, 0, 0

        def estourou():
            return limite_itens is not None and processados >= limite_itens

        print(f"\n[INFO] Iniciando geração de descrições com o modelo '{modelo}'.")

        # Colunas por tabela (contexto para descrever as tabelas)
        try:
            colunas_por_tabela = {}
            for r in self.spark.sql(f"""
                SELECT table_name, column_name FROM {info}.columns
                WHERE table_schema = '{_sql_str(self.target_schema)}' ORDER BY table_name, ordinal_position
            """).collect():
                colunas_por_tabela.setdefault(r.table_name, []).append(r.column_name)
        except Exception as e:
            print(f"[ERRO] Falha ao ler information_schema.columns: {e}")
            return 0, 0

        # TABELAS
        try:
            tabelas = self.spark.sql(f"""
                SELECT t.table_name FROM {info}.tables t
                WHERE t.table_schema = '{_sql_str(self.target_schema)}'
                  AND t.table_type IN ('MANAGED', 'EXTERNAL')
                  {filtro_comentario} {filtro_gov}
            """).collect()
            for row in tabelas:
                if estourou():
                    break
                nome = row.table_name
                colunas = ", ".join(colunas_por_tabela.get(nome, [])[:60])
                print(f"  -> Gerando descrição para a tabela: {nome}")
                try:
                    prompt = (f"Gere uma descrição de negócio concisa, em português, para a tabela `{nome}` "
                              f"com as colunas: {colunas}. Máximo 250 caracteres. "
                              f"Não inicie com 'Esta tabela'. Responda apenas com a descrição.")
                    comentario = self._gerar_texto(modelo, prompt, 250)
                    if comentario:
                        self.spark.sql(f"COMMENT ON TABLE {_q(self.target_catalog, self.target_schema, nome)} "
                                       f"IS '{_sql_str(comentario)}'")
                        n_tab += 1
                        print(f"  -> [OK] Descrição aplicada à tabela {nome}")
                except Exception as e_table:
                    print(f"  -> [ERRO] Falha ao processar a tabela {nome}: {e_table}")
                processados += 1
        except Exception as e:
            print(f"[ERRO] Falha na etapa de geração de descrições para tabelas: {e}")

        # COLUNAS
        filtro_comentario_col = "" if sobrescrever else "AND (c.comment IS NULL OR c.comment IN ('', 'N/A'))"
        try:
            colunas = self.spark.sql(f"""
                SELECT c.table_name, c.column_name, c.data_type
                FROM {info}.columns c
                JOIN {info}.tables t
                  ON t.table_schema = c.table_schema AND t.table_name = c.table_name
                WHERE c.table_schema = '{_sql_str(self.target_schema)}'
                  AND t.table_type IN ('MANAGED', 'EXTERNAL')
                  {filtro_comentario_col} {filtro_gov}
                ORDER BY c.table_name, c.ordinal_position
            """).collect()
            for row in colunas:
                if estourou():
                    break
                tabela, coluna, tipo = row.table_name, row.column_name, row.data_type
                alvo = _q(self.target_catalog, self.target_schema, tabela)
                try:
                    if coluna in STANDARD_COLUMN_DESCRIPTIONS:
                        comentario = STANDARD_COLUMN_DESCRIPTIONS[coluna]
                    else:
                        print(f"  -> Gerando descrição para a coluna: {tabela}.{coluna}")
                        vizinhas = ", ".join(colunas_por_tabela.get(tabela, [])[:40])
                        prompt = (f"Gere uma descrição de negócio concisa, em português, para a coluna `{coluna}` "
                                  f"(tipo: {tipo}) da tabela `{tabela}`, que também tem as colunas: {vizinhas}. "
                                  f"Máximo 150 caracteres. Responda apenas com a descrição.")
                        comentario = self._gerar_texto(modelo, prompt, 150)
                        processados += 1
                    if comentario:
                        self.spark.sql(f"ALTER TABLE {alvo} ALTER COLUMN {_q(coluna)} COMMENT '{_sql_str(comentario)}'")
                        n_col += 1
                except Exception as e_col:
                    print(f"  -> [ERRO] Falha ao processar a coluna {tabela}.{coluna}: {e_col}")
        except Exception as e:
            print(f"[ERRO] Falha na etapa de geração de descrições para colunas: {e}")

        print(f"[OK] Descrições aplicadas: {n_tab} tabela(s), {n_col} coluna(s).")
        return n_tab, n_col

    # ------------------------------------------------------------------
    # Sincronização das descrições para o dicionário
    # ------------------------------------------------------------------

    def fetch_table_descriptions(self):
        """
        Copia o comentário de cada tabela (Unity Catalog) para a coluna 'table_description'
        de todas as linhas daquela tabela na tabela de governança.
        """
        info = f"{_q(self.target_catalog)}.information_schema"
        try:
            self.spark.sql(f"""
                MERGE INTO {self._governance_table_sql()} AS t
                USING (
                    SELECT table_catalog, table_schema, table_name, comment AS new_description
                    FROM {info}.tables
                    WHERE comment IS NOT NULL AND comment NOT IN ('', 'N/A')
                ) AS s
                ON t.`catalog` = s.table_catalog AND t.`schema` = s.table_schema
                   AND lower(t.`table`) = lower(s.table_name)
                WHEN MATCHED AND (t.table_description IS NULL OR t.table_description <> s.new_description)
                  THEN UPDATE SET t.table_description = s.new_description
            """)
            print(f"[OK] Descrições de tabelas replicadas em '{self.governance_table}'.")
        except Exception as e:
            print(f"[ERRO] Falha ao replicar descrições de tabelas: {str(e).splitlines()[0]}")

    def fetch_column_descriptions(self):
        """
        Copia o comentário de cada coluna (Unity Catalog) para 'column_description' na tabela de governança.
        """
        info = f"{_q(self.target_catalog)}.information_schema"
        try:
            self.spark.sql(f"""
                MERGE INTO {self._governance_table_sql()} AS t
                USING (
                    SELECT table_catalog, table_schema, table_name, column_name, comment AS new_description
                    FROM {info}.columns
                    WHERE comment IS NOT NULL AND comment NOT IN ('', 'N/A')
                ) AS s
                ON t.`catalog` = s.table_catalog AND t.`schema` = s.table_schema
                   AND lower(t.`table`) = lower(s.table_name) AND lower(t.`column`) = lower(s.column_name)
                WHEN MATCHED AND (t.column_description IS NULL OR t.column_description <> s.new_description)
                  THEN UPDATE SET t.column_description = s.new_description
            """)
            print(f"[OK] Descrições de colunas replicadas em '{self.governance_table}'.")
        except Exception as e:
            print(f"[ERRO] Falha ao replicar descrições de colunas: {str(e).splitlines()[0]}")

    # ------------------------------------------------------------------
    # Fluxo completo
    # ------------------------------------------------------------------

    def run_full_pipeline(self, gerar_descricoes_ia=True, limite_itens=None):
        """
        Executa o fluxo completo de governança, na ordem:
        infraestrutura → carga do buffer → descrições por IA → sincronização das descrições.
        """
        self.create_governance_infrastructure()
        self.load_buffer_to_governance_table()
        if gerar_descricoes_ia:
            self.generate_and_apply_ai_descriptions(limite_itens=limite_itens)
        self.fetch_table_descriptions()
        self.fetch_column_descriptions()