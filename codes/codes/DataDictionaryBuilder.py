'''
Código desenvolvido por: André Luiz Souza Santos
Contato: andrezluz19@gmail.com
'''
import spark
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql import functions as F 
import os
import getpass
from datetime import datetime

class DataDictionaryBuilder:
    def __init__(self, target_catalog, target_schema, target_table, buffer_path=None):
        self.spark = SparkSession.builder.getOrCreate()
        self.buffer_path = buffer_path
        self.target_catalog = target_catalog
        self.target_schema = target_schema
        self.target_table = target_table

    def create_governance_infrastructure(self):
        """
        Cria os catálogos, schemas e a tabela de governança, caso não existam,
        com as colunas inferidas a partir do cabeçalho do arquivo de buffer.
        """
        if not self.buffer_path or not os.path.exists(self.buffer_path):
            print(f"[INFO] Nenhum arquivo de buffer encontrado para inferir colunas.")
            # Define um schema padrão se não houver buffer
            columns_ddl_string = """
                catalog STRING,
                schema STRING,
                table STRING,
                column STRING,
                collumn_dtype STRING,
                creation_date STRING,
                creation_time STRING,
                table_description STRING,
                user STRING,
                file_fetched STRING,
                source_notebook_url STRING
            """
        else:
            try:
                with open(self.buffer_path, "r", encoding="utf-8") as f:
                    header_line = f.readline().strip()
                columns_from_buffer = header_line.split(';')
                columns_ddl = [f"{col_name} STRING" for col_name in columns_from_buffer]
                columns_ddl_string = ",\n".join(columns_ddl)
                print(f"[INFO] Colunas inferidas do buffer: {columns_from_buffer}")
            except Exception as e:
                print(f"[ERRO] ao ler o cabeçalho do buffer: {e}")
                return

        try:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.target_catalog}.{self.target_schema}")
            print(f"[OK] Schema '{self.target_catalog}.{self.target_schema}' garantido.")
            
            tabela_full_name = f"{self.target_catalog}.{self.target_schema}.{self.target_table}"
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {tabela_full_name} (
                    {columns_ddl_string}
                )
                USING delta
            """)
            print(f"[OK] Tabela '{tabela_full_name}' criada/verificada com sucesso.")
        except Exception as e:
            print(f"[ERRO] ao criar infraestrutura no catálogo '{self.target_catalog}': {e}")


    def load_buffer_to_governance_table(self):
        """
        Lê o buffer de log, insere os dados na tabela de governança e,
        após o sucesso, limpa o buffer mantendo apenas o cabeçalho.
        """
        if not self.buffer_path or not os.path.exists(self.buffer_path) or os.path.getsize(self.buffer_path) < 10:
            print(f"[INFO] Nenhum arquivo de buffer para processar em {self.buffer_path}.")
            return

        header_line = ""
        try:
            with open(self.buffer_path, "r", encoding="utf-8") as f:
                header_line = f.readline().strip()

            log_df_raw = self.spark.read.option("header", "true").option("sep", ";").csv(self.buffer_path)
            
            if log_df_raw.count() == 0:
                print("[INFO] Buffer não contém linhas de dados para carregar.")
                return

            governance_table_full_name = f"{self.target_catalog}.{self.target_schema}.{self.target_table}"
            log_df_raw.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(governance_table_full_name)
            print(f"[OK] Dados carregados do buffer para {governance_table_full_name}")

            self.drop_duplicate_rows()
            print("[OK] Verificação de duplicatas concluída.")

            try:
                with open(self.buffer_path, "w", encoding="utf-8") as f:
                    f.write(header_line + "\n")
                print(f"[OK] Arquivo de buffer '{self.buffer_path}' foi limpo, mantendo apenas o cabeçalho.")
            except Exception as e_clean:
                print(f"[ALERTA] Os dados foram carregados, mas falha ao limpar o arquivo de buffer: {e_clean}")

        except Exception as e:
            print(f"[ERRO] Falha ao carregar dados do buffer. O arquivo NÃO será limpo. Erro: {e}")

    def drop_duplicate_rows(self):
        """
        Remove linhas duplicadas da tabela de governança.
        """
        governance_table = f"{self.target_catalog}.{self.target_schema}.{self.target_table}"
        df = self.spark.table(governance_table)
        # Define a chave de duplicidade com base nas colunas que identificam um registro único de metadados
        key_columns = ["catalog", "schema", "table", "column"]
        deduped_df = df.dropDuplicates(subset=key_columns)
        deduped_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(governance_table)

    def _find_available_ai_model(self):
        """
        Testa uma lista priorizada de modelos de IA e retorna o primeiro que estiver disponível.
        """
        print("[INFO] Buscando um modelo de IA disponível no ambiente...")
        preferred_models = ['databricks-dbrx-instruct', 'databricks-meta-llama-3-70b-instruct', 'databricks-llama-2-70b-chat']
        
        for model_name in preferred_models:
            try:
                print(f"  -> Testando modelo: '{model_name}'...")
                self.spark.sql(f"SELECT ai_query('{model_name}', 'Test')").collect()
                print(f"  -> [OK] Modelo encontrado e funcionando: '{model_name}'")
                return model_name
            except Exception as e:
                if "RESOURCE_DOES_NOT_EXIST" in str(e) or "does not exist" in str(e):
                    print(f"  -> Modelo '{model_name}' não disponível. Tentando o próximo.")
                    continue
                else:
                    print(f"[ALERTA] Erro inesperado ao testar o modelo '{model_name}': {e}")
                    continue

        raise Exception("Nenhum modelo de IA na lista priorizada está disponível.")

    def generate_and_apply_ai_descriptions(self):
        """
        Descobre um modelo de IA e o usa para gerar e aplicar descrições para
        tabelas e colunas que ainda não possuem um comentário.
        """
        try:
            available_model = self._find_available_ai_model()
        except Exception as e:
            print(f"[ERRO CRÍTICO] {e}")
            return

        print(f"\n[INFO] Iniciando geração de descrições com o modelo '{available_model}'.")

        # Gerar para TABELAS
        try:
            tables_df = self.spark.sql(f"""
                SELECT table_name FROM {self.target_catalog}.information_schema.tables
                WHERE table_schema = '{self.target_schema}' AND (comment IS NULL OR comment = 'N/A')
            """)
            for row in tables_df.collect():
                table_name = row.table_name
                print(f"  -> Gerando descrição para a tabela: {table_name}")
                try:
                    ai_query = f"""SELECT ai_query('{available_model}', 'Gere uma descrição de negócio concisa para a tabela chamada `{table_name}`. Máximo 250 caracteres. Não inicie com "Esta tabela...".') AS comment"""
                    comment = self.spark.sql(ai_query).first().comment.strip().replace("'", "''").replace('"', '')
                    self.spark.sql(f"COMMENT ON TABLE {self.target_catalog}.{self.target_schema}.{table_name} IS '{comment}'")
                    print(f"  -> [OK] Descrição aplicada à tabela {table_name}")
                except Exception as e_table:
                    print(f"  -> [ERRO] Falha ao processar a tabela {table_name}: {e_table}")
        except Exception as e:
            print(f"[ERRO] Falha na etapa de geração de descrições para tabelas: {e}")

        # Gerar para COLUNAS
        try:
            cols_df = self.spark.sql(f"""
                SELECT table_name, column_name, data_type FROM {self.target_catalog}.information_schema.columns
                WHERE table_schema = '{self.target_schema}' AND (comment IS NULL OR comment = 'N/A')
            """)
            for row in cols_df.collect():
                table_name, column_name, data_type = row.table_name, row.column_name, row.data_type
                print(f"  -> Gerando descrição para a coluna: {table_name}.{column_name}")
                try:
                    ai_query = f"""SELECT ai_query('{available_model}', CONCAT('Gere uma descrição de negócio concisa para a coluna `{column_name}` (tipo: {data_type}) da tabela `{table_name}`. Máximo 150 caracteres.')) AS comment"""
                    comment = self.spark.sql(ai_query).first().comment.strip().replace("'", "''").replace('"', '')
                    self.spark.sql(f"ALTER TABLE {self.target_catalog}.{self.target_schema}.{table_name} ALTER COLUMN {column_name} COMMENT '{comment}'")
                    print(f"  -> [OK] Descrição aplicada à coluna {table_name}.{column_name}")
                except Exception as e_col:
                    print(f"  -> [ERRO] Falha ao processar a coluna {table_name}.{column_name}: {e_col}")
        except Exception as e:
            print(f"[ERRO] Falha na etapa de geração de descrições para colunas: {e}")

    def fetch_table_descriptions(self):
        """
        ATUALIZA a coluna 'table_description' para TODAS as linhas de uma tabela
        na tabela de governança, replicando a descrição da tabela.
        """
        notebook_url = "N/A (dbutils not available)"
        try:
            # Esta é a forma padrão de obter o dbutils em ambientes modernos do Databricks
            dbutils = spark.sparkContext._jvm.com.databricks.service.DBUtils.getDBUtils()
            
            # Pega o contexto e constrói a URL
            context = dbutils.notebook().getContext()
            hostname = context.tags().get("browserHostName").get()
            notebook_path = context.tags().get("notebookPath").get()
            notebook_url = f"https://{hostname}{notebook_path}"
            print(f"[INFO] URL do notebook de execução capturada: {notebook_url}")
        except Exception as e:
            print(f"[ALERTA] Não foi possível obter a URL do notebook. Usando 'N/A'. Erro: {e}")
    
        try:

            query = f"""
                SELECT table_catalog AS catalog, table_schema AS schema, table_name AS table,
                       COALESCE(comment, 'N/A') AS new_description
                FROM {self.target_catalog}.information_schema.tables
                WHERE table_schema = '{self.target_schema}'
            """
            table_descriptions_df = self.spark.sql(query)

            if table_descriptions_df.count() == 0:
                print(f"[INFO] Nenhuma tabela encontrada em {self.target_catalog}.{self.target_schema} para buscar descrições.")
                return

            governance_table_full_name = f"{self.target_catalog}.{self.target_schema}.{self.target_table}"
            governance_delta_table = DeltaTable.forName(self.spark, governance_table_full_name)

            governance_delta_table.alias("target").merge(
                table_descriptions_df.alias("source"),
                "target.catalog = source.catalog AND target.schema = source.schema AND LOWER(target.table) = LOWER(source.table)"
            ).whenMatchedUpdate(
                condition=(
                    "target.table_description IS NULL OR "
                    "target.table_description = 'N/A' OR "
                    "target.table_description <> source.new_description"
                ),
                set={
                    "table_description": "source.new_description",
                    "source_notebook_url": F.lit(notebook_url)
                }
            ).execute()
            print(f"[OK] Descrições de tabelas replicadas com sucesso em '{governance_table_full_name}'.")
        except Exception as e:
            print(f"[ERRO] Falha ao atualizar e replicar descrições de tabelas: {e}")