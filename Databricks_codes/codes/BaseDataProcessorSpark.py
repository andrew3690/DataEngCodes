'''
    Codigo desenvolvido por André Luiz Souza Santos
    contato: andrezluz19@gmail.com
'''

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, when, lit
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, monotonically_increasing_id, regexp_replace
from multiprocessing import Pool
import os
import pandas as pd
from functools import reduce
import zipfile
import tarfile
import gzip
import shutil
import unicodedata
import re
from datetime import datetime
import getpass
import psutil
import platform

# General Path
BUFFER_PATH = f"/insert_your_path_here/export_log_buffer_.txt"

class BaseDataProcessorSpark:
    def __init__(self,input_directory=None, output_directory=None, catalog = None): # buffer_log_path = BUFFER_PATH
        """
        Inicializa uma instância da classe BaseDataProcessorSpark.

        Parâmetros:
        - input_directory: O diretório de entrada contendo os dados brutos (opcional)
            - Exemplo: '/Volumes/your_path_here/' --> Caminho do volume
        - output_directory: O diretório de saída para os dados tratados.] (opcional)
            - Exemplo: '/Volumes/your_path_here/' --> Caminho do volume
        - catalog: Catálogo de dados que deve ser utilizado para exportação de dados
            - Exemplo: your_catalog
        """
        if input_directory is not None:
            self.input_directory = input_directory
        else:
            raise ValueError("O diretório de entrada deve ser fornecido.")

        if output_directory is not None:
            self.output_directory = output_directory
        else:
            raise ValueError("O diretório de saída deve ser fornecido.")

        # Inicializa a sessão Spark
        self.spark = SparkSession.builder.appName("BaseDataProcessor_Spark").getOrCreate()

        # Objeto de DataFrame para uso da classe genérica
        self.df = None
        self.catalog = catalog
        self.data_dict = {}
        # Database exporting methods in databricks
        self.import_database = None
        self.import_table = None
        self.export_database = None
        self.export_table = None
        # Dicionario de multiplos arquivos lidos
        self.file_list = []

    def read_data(self, filename, separator=None, skip_rows=0, header=True, sheet_name=None, ignore_last_rows=0):
        """
        Lê os dados de um arquivo Parquet, CSV ou Excel no Spark Connect-compatible.

        Parâmetros:
        - filename: Nome do arquivo.
        - separator: Separador CSV (opcional).
        - skip_rows: Linhas a ignorar no topo (opcional).
        - header: Se o CSV/Excel possui cabeçalho.
        - sheet_name: Nome da aba do Excel (opcional).
        - ignore_last_rows: Número de linhas a ignorar no final (opcional).

        Retorna:
        Um DataFrame do Spark contendo os dados lidos.

        EX:
        read_data("my_file.csv", separator=",", skip_rows=1, header=True)
        """
        # Garante caminho absoluto
        file_path = filename if filename.startswith("/") or filename.startswith("dbfs:") \
            else os.path.abspath(os.path.join(self.input_directory, filename))

        print(f"Tentando ler o arquivo: {file_path}")

        # Método de pré-processamento de dados, visa sanitizar colunas antes da execucção, caso ocorra erro, continua a execução
        try:
            self.preprocess_data_file(
                file_path=file_path,
                separator=separator,
                sheet_name=sheet_name
            )
        except Exception as e:
            print(f"Erro ao pré-processar o arquivo: {e}, continuando a leitura de daddos")
            # continue

        if filename.endswith(".parquet"):
            df = self.spark.read.parquet(file_path)

        elif filename.endswith(".csv"):
            read_options = {
                "header": header,
                "inferSchema": True
            }
            if separator:
                read_options["sep"] = separator

            df = self.spark.read.csv(file_path, **read_options)

        elif filename.endswith(".xlsx") or filename.endswith(".xls"):
            pdf = pd.read_excel(
                file_path,
                engine='openpyxl',
                skiprows=skip_rows,
                header=0 if header else None,
                sheet_name=sheet_name
            )
            if ignore_last_rows > 0:
                pdf = pdf.iloc[:-ignore_last_rows]

            pdf.columns = [str(col).strip().replace(" ", "_") for col in pdf.columns]
            self.df = self.spark.createDataFrame(pdf)
            return self.df

        else:
            raise ValueError("Formato de arquivo não suportado. Use Parquet, CSV ou Excel.")

        # Aplicar skip_rows e ignore_last_rows para CSV/Parquet
        if skip_rows > 0 or ignore_last_rows > 0:
            df = df.withColumn("__row_id", row_number().over(Window.orderBy(monotonically_increasing_id())))

            total_rows = df.selectExpr("count(*) as total").collect()[0]["total"]
            start = skip_rows + 1  # row_number começa em 1
            end = total_rows - ignore_last_rows if ignore_last_rows > 0 else total_rows

            df = df.filter((df["__row_id"] >= start) & (df["__row_id"] <= end)).drop("__row_id")

        self.df = df

        # Appending file reading here
        self.file_list.append(file_path)

        return self.df

    def fetch_and_append_data(self, search_string, separator=None):
        """
        Busca arquivos com base em uma string presente no nome do arquivo, 
        concatena os arquivos em um único DataFrame e salva o DataFrame resultante.

        Parâmetros:
        - search_string: String a ser buscada nos nomes dos arquivos.
        - separator: Separador a ser usado para arquivos CSV (opcional).

        Retorna:
        Um DataFrame do Spark contendo os dados concatenados.

        EX:
        fetch_and_append_data("my_string", ",")
        """
        # Lista todos os arquivos no diretório de entrada
        files = self.list_files()
        
        # Filtra arquivos que contêm a string de busca no nome
        matching_files = [f for f in files if search_string in f]
        print(matching_files)
        if not matching_files:
            raise ValueError("Nenhum arquivo correspondente encontrado.")
        
        # Lê e concatena os arquivos correspondentes
        dataframes = [self.read_data(f, separator) for f in matching_files]
        combined_df = reduce(lambda df1, df2: df1.union(df2), dataframes)
        
        # Salva o DataFrame resultante
        # combined_df.write.format("delta").mode("overwrite").save(f"{self.output_directory}/combined_data")
        
        return combined_df
    
    def list_files(self):
        """
        Lista todos os arquivos no diretório de entrada.

        Retorna:
        Uma lista de nomes de arquivos.
        """
        return os.listdir(f"{self.input_directory}")
    
    def output_files(self):
        """
        Lista todos os arquivos no diretório de saída.

        Retorna:
        Uma lista de nomes de arquivos.
        """
        return os.listdir(self.output_directory)

    def create_temp_view(self, view_name):
        """
        Cria uma view temporária usando o DataFrame atual, pode ser utilizada em consultas SQL.
        
        Parâmetros:
        - view_name: Nome da view desejada


        Ex:
        query = '''
        SELECT * FROM {view_name}
        '''
        processor.get_sql_query(query)
        """
        if self.df is not None:
            self.df.createOrReplaceTempView(view_name)
        else:
            raise ValueError("DataFrame não está definido.")

    def drop_columns(self, df, drop_list):
        """
        Remove colunas de um DataFrame do Spark.

        Parâmetros:
        - df: Dataframe a ser passado.
        - drop_list: Lista de colunas a serem dropadas.

        Retorna:
        Um DataFrame do Spark com colunas seleciondas dropadas.

        EX:
        df = drop_columns(df, ["coluna1", "coluna2")
        """
        return df.drop(*drop_list)

    def reorganize_columns(self, df, column_order):
        """
        Reorganiza as colunas de um DataFrame do Spark.

        Parâmetros:
        - df: Dataframe a ser passado.
        - column_order: Lista de colunas na ordem desejada.

        Retorna:
        Um DataFrame do Spark com as colunas ordenadas.

        EX:
        df = reorganize_columns(df, ["coluna1", "coluna2", "coluna3"])
        """
        return df.select(column_order)

    def rename_columns(self, df, column_mapping):
        """
        Renomeia colunas de um DataFrame do Spark.

        Parâmetros:
        - df: Dataframe a ser passado.
        - column_mapping: Dicionário com mapeamento de nomes de colunas antigos para novos nomes.

        Retorna:
        Um DataFrame do Spark com as colunas renomeadas.

        EX:
        df = rename_columns(df, {"coluna_antiga": "coluna_nova"})
        """
        for old_col, new_col in column_mapping.items():
            df = df.withColumnRenamed(old_col, new_col)
        return df

    def preencher_valores_nulos(self, df, coluna, valor_substituto=""):
        """
        Preenche valores nulos em uma coluna específica.
        
        Parametros:
        - df: DataFrame do Spark.
        - coluna: Coluna a ser preenchida.
        - valor_substituto: Valor a ser usado para substituir os valores nulos.

        Retorna:
        DataFrame do Spark com valores nulos preenchidos.

        EX:
        df = preencher_valores_nulos(df, "coluna", "valor_substituto")
        """
        return df.fillna({coluna: valor_substituto})

    def convert_type(self, df, column_to_convert, new_type):
        """
        Converte o tipo de uma coluna específica em um DataFrame do Spark.

        Parâmetros:
        - df: Dataframe a ser passado.
        - column_to_convert: Coluna a ser convertida.
        - new_type: Novo tipo de dados para a coluna.

        Retorna:
        DataFrame do Spark com a coluna convertida.

        EX:
        df = convert_type(df, "coluna", "int")
        """
        return df.withColumn(column_to_convert, col(column_to_convert).cast(new_type))

    def formatar_nomes_colunas(self, df, substituicoes={}):
        """
        Formata os nomes das colunas substituindo caracteres especificados.
        
        Paramêtros:
        - df: DataFrame do Spark.
        - substituicoes: Dicionário com caracteres a serem substituídos

        Retorna:
        DataFrame do Spark com os nomes das colunas formatados.

        EX:
        df = formatar_nomes_colunas(df, {" ": "_", "-": "_", "(": "", ")": ""})
        """
        for old_char, new_char in substituicoes.items():
            df = df.toDF(*[c.replace(old_char, new_char) for c in df.columns])
        return df

    def ordenar_dataframe(self, df, colunas, ascendentes):
        """
        Ordena um DataFrame do Spark com base em colunas específicas.

        Parâmetros:
        - df: Dataframe a ser passado.
        - colunas: Lista de colunas a serem usadas para ordenamento

        Retorna:
        DataFrame do Spark ordenado.

        EX:
        df = ordenar_dataframe(df, ["coluna1", "coluna2"], [True, False])
        """
        sort_cols = [col(c).asc() if asc else col(c).desc() for c, asc in zip(colunas, ascendentes)]
        return df.orderBy(*sort_cols)

    def drop_duplicates(self, df, subset=None):
        """
        Remove linhas duplicadas de um DataFrame do Spark.

        Parametros:
        - df: DataFrame do Spark.
        - subset: Lista de colunas a serem usadas para detecção de duplicatas.

        Retorna:
        DataFrame do Spark sem duplicatas.

        EX:
        df = drop_duplicates(df, ["coluna1", "coluna2"])
        """
        return df.dropDuplicates(subset)

    def drop_null_rows(self, df):
        """
        Remove linhas inteiramente nulas de um DataFrame do Spark.

        Parâmetros:
        - df: DataFrame do Spark.

        Retorna:
        - DataFrame sem linhas nulas

        EX:
        df = drop_null_rows(df)
        """
        return df.dropna(how="all")

    def sanitize_column_names(self, df):
        """
        Limpa os nomes das colunas removendo acentos, espaços, caracteres especiais e padroniza para minúsculo.

        Parâmetros:
        - df: DataFrame do Spark.

        Retorna:
        - DataFrame com colunas renomeadas.

        EX:
        df = sanitize_column_names(df)
        """
        def clean_string(s):
            # Remove acentos
            s = ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
            # Substitui espaços e caracteres especiais por _
            s = re.sub(r'[^0-9a-zA-Z]+', '_', s)
            # Remove underscores duplicados
            s = re.sub(r'_+', '_', s)
            # Remove underscores no início/fim e transforma em minúsculo
            return s.strip('_').lower()

        new_columns = [clean_string(c) for c in df.columns]
        return df.toDF(*new_columns)


    def resetar_indice(self, df):
        """
        No Spark, resetar o índice não é necessário, mas pode-se adicionar uma coluna de índice.
        
        Parâmetros:
        - df: DataFrame do Spark.

        Retorna:
        - DataFrame do Spark com uma coluna de índice.

        EX:
        df = resetar_indice(df)
        """
        return df.withColumn("index", monotonically_increasing_id())

    def selecionar_colunas(self, df, colunas):
        """
        Seleciona colunas específicas de um DataFrame do Spark.

        Parâmetros:
        - df: DataFrame do Spark.
        - colunas: Lista de colunas a serem selecionadas.

        Retorna:
        DataFrame do Spark com as colunas selecionadas.

        EX:
        df = selecionar_colunas(df, ["coluna1", "coluna2"])
        """
        return df.select(*colunas)

    def combinar_dataframes(self, df1, df2, coluna_comum, metodo="left"):
        """
        Combina dois DataFrames do Spark com base em uma coluna comum.

        Parâmetros:
        - df1: Primeiro DataFrame do Spark.
        - df2: Segundo DataFrame do Spark.

        Retorna:
        DataFrame do Spark resultante da combinação.

        EX:
        df = combinar_dataframes(df1, df2, "coluna_comum", metodo="left")
        """
        return df1.join(df2, on=coluna_comum, how=metodo)

    def tratar_colunas(self, df, colunas, substituicoes):
        """
        Substitui valores nas colunas especificadas de um DataFrame do Spark.

        Parâmetros:
        - df: DataFrame do Spark.
        - colunas: Lista de colunas a serem tratadas.

        Retorna:
        DataFrame do Spark com as substituições aplicadas.

        EX:
        df = tratar_colunas(df, ["coluna1", "coluna2"], {"antigo": "novo"})
        """
        for coluna in colunas:
            for antigo, novo in substituicoes.items():
                df = df.withColumn(coluna, when(col(coluna) == antigo, novo).otherwise(col(coluna)))
        return df

    def criar_coluna(self, df, nome_coluna, valor_padrao=""):
        """
        Cria uma nova coluna com um valor padrão.

        Parâmetros:
        - df: DataFrame do Spark.
        - nome_coluna: Nome da nova coluna.
        - valor_padrao: Valor padrão para a nova coluna.

        Retorna:
        DataFrame do Spark com a nova coluna criada.

        EX:
        df = criar_coluna(df, "nova_coluna", "")
        """
        return df.withColumn(nome_coluna, lit(valor_padrao))

    def apply_function_to_dataframes(self, function, dataframe_list, *args, **kwargs):
        """
        Aplica uma função a todos os dataframes em uma lista.

        Parâmetros:
        - function: Função a ser aplicada.
        - dataframe_list: Lista de DataFrames do Spark.
        - args: Argumentos posicionais adicionais para a função.
        - kwargs: Argumentos nomeados adicionais para a função.

        Retorna:
        Lista de DataFrames resultantes da aplicação da função.

        EX:
        resultados = apply_function_to_dataframes(func, dataframes, arg1, arg2)
        """
        return [function(df, *args, **kwargs) for df in dataframe_list]

    def set_df(self, df):
        """
        Armazena o DataFrame na classe.

        Parâmetros:
        - df: DataFrame do Spark.

        Retorna:
        DataFrame do Spark armazenado na classe de forma temporária

        EX:
        set_df(df)
        """
        self.df = df

    def get_df(self):
        """
        Retorna o DataFrame armazenado.

        Retorna:
        DataFrame do Spark armazenado na classe.

        EX:
        df = get_df()
        """
        return self.df
    
    def get_sql_query(self, query):
        """
        Executa uma consulta SQL, realiza a operação SQL, tenta exibir os resultados; se for bem-sucedido, retorna um DataFrame Spark, caso contrário retorna uma mensagem de erro
        
        Parametros:
            - query: SQL query to be executed
        
        Retorna:
            - df: Spark dataframe with the results of the query
        
        EX:
        df = get_sql_query("SELECT * FROM table")
        """
        try:
            df = self.spark.sql(query)
            display(df)
            return df
        except Exception as e:
            return f"Error: {str(e)}"

    def export_to_db(self, database, table, mode="overwrite"):
        """
        Exporta dados do DataFrame para uma tabela Delta e registra metadados em uma tabela de auditoria.

        Parâmetros:
        - database: Nome do banco de dados de destino.
        - table: Nome da tabela de destino.
        - mode: Modo de gravação (padrão: "overwrite").

        Retorna:
        - None

        EX:
        processor = BaseDataProcessorSpark(input_directory="/caminho/entrada", output_directory="/caminho/saida", catalog="meu_catalogo")
        processor.set_df(meu_dataframe)
        processor.export_to_db(database="meu_banco", table="minha_tabela", mode="append")
        """
        
        audit_log_table="governanca_export_log"

        def get_current_notebook_name():
            try:
                ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
                return ctx.notebookPath().get().split("/")[-1]
            except Exception:
                return "unknown_notebook"

        notebook_name = get_current_notebook_name()
        df = self.get_df()

        # Adicionando a colunas de horarios de escritas de dados
        df = self.criar_coluna(df, "write_date", datetime.now().strftime("%Y-%m-%d"))
        df = self.criar_coluna(df, "write_time", datetime.now().strftime("%H:%M:%S"))

        if df is None:
            raise ValueError("Nenhum DataFrame definido para exportação. Use 'set_df()' primeiro.")

        full_table_name = f"{self.catalog}.{database}.{table}"
        self.export_database = database
        self.export_table = table

        print(f'Exportando para :{full_table_name}')
        # Exporta os dados
        df.write \
            .format("delta") \
            .option("overwriteSchema", "true") \
            .option("userMetadata", notebook_name) \
            .mode(mode) \
            .saveAsTable(full_table_name)

        # Dados de auditoria completos com fallback para `self.data_dict`
        audit_log_table = "governanca_export_log"
        # Importante mudar o path do buffer para cada ambiente databricks
        buffer_log_path = BUFFER_PATH

        # Obtendo informações do usuário atual
        current_user = self.spark.sql("SELECT current_user()").collect()[0][0]

        # Retirando valores duplicados da lista de arquivos usados       
        self.file_list = list(dict.fromkeys(self.file_list))
        
        # Montando o log de arquivos e dados de colunas e tabelas usadas
        log_rows = []
        for file in self.file_list:
            for column, dtype in self.get_df().dtypes:
                log_rows.append({
                    "catalog": self.catalog,
                    "schema": database,
                    "table": table,
                    "column": column,
                    "collumn_dtype": dtype,
                    "creation_date": datetime.now().strftime("%Y-%m-%d"),
                    "creation_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "table_description": "N/A",
                    "user": current_user,
                    "file_fetched": file
                })
        
        # Salva em buffer local (como .txt temporário) em formato tabular
        print(log_rows)
        try:
            headers = [
                "catalog", "schema", "table", "column", "collumn_dtype", "creation_date", "creation_time", "table_description", "user", "file_fetched"
            ]
            
            with open(buffer_log_path, "w", encoding="utf-8") as f:
                f.write(";".join(headers) + "\n") # Escreve o cabeçalho
                
                for row in log_rows:
                    print(row)
                    values = [str(row.get(h, "")) for h in headers]
                    f.write(";".join(values) + "\n")  # Escreve a linha de dados
                    
        except Exception as e:
            print(f"[ERRO] Falha ao escrever o log de buffer: {e}")

    def preprocess_data_file(self, file_path, separator=None, sheet_name=None):
        """
        Pré-processa o arquivo para sanitizar os nomes das colunas diretamente no arquivo. Se falhar na leitura, tenta ler com Pandas, sanitiza as colunas e sobrescreve o arquivo. Funciona para arquivos CSV, Excel e Parquet.

        Parametros:
            - file_path: Caminho para o arquivo a ser pré-processado.
            - separator: Separador usado
            - sheet_name: Nome da planilha

        # EX:
        # processor = BaseDataProcessorSpark(input_directory="/caminho/entrada", output_directory="/caminho/saida")
        # processor.preprocess_data_file(input_directory="/caminho/entrada", separator=';', sheet_name='Sheet1')
        """
        try:
            if file_path.endswith(".csv"):
                df = pd.read_csv(file_path, sep=separator or ",", nrows=5)  # testa leitura

            elif file_path.endswith(".xlsx") or file_path.endswith(".xls"):
                df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=5)

            elif file_path.endswith(".parquet"):
                df = pd.read_parquet(file_path)

            else:
                return  # Arquivo não suportado para esse processo

            # Teste passou, não precisa processar
            return

        except Exception as e:
            print(f"[PREPROCESS] Erro ao ler o arquivo {file_path}, tentando sanitizar: {e}")

            try:
                # Leitura completa com Pandas
                if file_path.endswith(".csv"):
                    df = pd.read_csv(file_path, sep=separator or ",")

                elif file_path.endswith(".xlsx") or file_path.endswith(".xls"):
                    df = pd.read_excel(file_path, sheet_name=sheet_name)

                elif file_path.endswith(".parquet"):
                    df = pd.read_parquet(file_path)

                else:
                    return

                # Sanitização dos nomes de colunas
                temp_spark_df = self.spark.createDataFrame(pd.DataFrame(columns=df.columns))
                sanitized_df = self.sanitize_column_names(temp_spark_df)
                df.columns = sanitized_df.columns

                # Sobrescreve o arquivo original
                if file_path.endswith(".csv"):
                    df.to_csv(file_path, index=False)
                elif file_path.endswith(".xlsx") or file_path.endswith(".xls"):
                    df.to_excel(file_path, index=False)
                elif file_path.endswith(".parquet"):
                    df.to_parquet(file_path, index=False)

                print(f"[PREPROCESS] Arquivo {file_path} sanitizado com sucesso.")

            except Exception as err:
                print(f"[PREPROCESS] Falha ao sanitizar o arquivo {file_path}: {err}")

    
    def drop_specific_row(self, df, column, target_value):
        """
        Remove linhas de um DataFrame do Spark com base em um valor específico de uma coluna.

        Parâmetros:
        - df: DataFrame do Spark.
        - column: Nome da coluna.
        - target_value: Valor alvo para remoção das linhas.

        Retorna:
        DataFrame do Spark sem as linhas que contêm o valor alvo na coluna especificada.

        EX:
        drop_specific_row(df, "coluna", "valor")
        """
        return df.filter(col(column) != target_value)
    
    def substituir_valores(self, df, valor_antigo, valor_novo):
        """
        Substitui valores em todas as colunas de um DataFrame do Spark.

        Parâmetros:
        - df: DataFrame do Spark.
        - valor_antigo: Valor a ser substituído.
        - valor_novo: Novo valor.

        Retorna:
        DataFrame do Spark com os valores substituídos.

        EX:
        substituir_valores(df, "valor_antigo", "valor_novo")
        """
        for coluna in df.columns:
            df = df.withColumn(coluna, when(col(coluna) == valor_antigo, valor_novo).otherwise(col(coluna)))
        return df

    def replace_values(self, df, column, old_value, new_value):
        """
        Substitui valores em uma coluna específica de um DataFrame usando regexp_replace.

        Parâmetros:
            df (DataFrame): DataFrame do Spark a ser processado.
            column (str): Nome da coluna para aplicar as substituições.
            old_value (str): Valor a ser substituído.
            new_value (str): Valor para substituir.

        Retorna:
            DataFrame: DataFrame atualizado com os valores substituídos.
        
        EX:
        replace_values(df, "coluna", "valor_antigo", "valor_novo")
        """
        df = df.withColumn(column, regexp_replace(col(column), old_value, new_value))
        return df

    def change_data_types(self, df, type_mapping):
        """
        Altera os tipos de dados das colunas de um DataFrame do Spark com base em um dicionário de mapeamento.

        Parâmetros:
        - df: DataFrame do Spark.
        - type_mapping: Dicionário onde as chaves são os nomes das colunas e os valores são os novos tipos de dados.

        Retorna:
        DataFrame do Spark com os tipos de dados alterados.

        EX:
        change_data_types(df, {"coluna1": "int", "coluna2": "string"})
        """
        for column, new_type in type_mapping.items():
            df = df.withColumn(column, col(column).cast(new_type))
        return df

    @staticmethod
    def fill_nan_based_on_dtype(df):
        """
        Preenche valores nulos com base no tipo de dado.

        Parametros:
        - Dataframe: dataframe

        Retorna:
        - DataFrame com valores nulos preenchidos com base no tipo de dado.

        EX:
        fill_nan_based_on_dtype(df)
        """
        default_values = {
            "timestamp": "1901-01-01",  # Data padrão
            "double": 0.0,              # Número float padrão
            "int": 0,                   # Número inteiro padrão
            "string": "Não identificado", # Texto padrão
            "boolean": False            # Boolean padrão
        }

        for col_name, dtype in df.dtypes:
            default_value = default_values.get(dtype, "Não identificado")
            df = df.fillna({col_name: default_value})

        return df
    
    # === Preprocessing Utilities ===
    
    def decompress_files(self, compressed_file, output_dir=None):
        """
        Descomprime arquivos e exporta para o diretório atual ou especificado.

        Parâmetros:
        - compressed_file: Nome do arquivo comprimido.
        - output_dir: Diretório de saída para os arquivos descomprimidos (opcional).

        Retorna:
        Lista de arquivos descomprimidos.

        EX:
        decompress_files("arquivo.zip", "/caminho/para/diretorio")
        """
        if output_dir is None:
            output_dir = self.input_directory

        file_path = f"{self.input_directory}/{compressed_file}"
        extracted_files = []

        if compressed_file.endswith(".zip"):
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(output_dir)
                extracted_files = zip_ref.namelist()
        elif compressed_file.endswith(".tar.gz") or compressed_file.endswith(".tgz"):
            with tarfile.open(file_path, 'r:gz') as tar_ref:
                tar_ref.extractall(output_dir)
                extracted_files = tar_ref.getnames()
        elif compressed_file.endswith(".tar"):
            with tarfile.open(file_path, 'r:') as tar_ref:
                tar_ref.extractall(output_dir)
                extracted_files = tar_ref.getnames()
        elif compressed_file.endswith(".gz"):
            output_file = os.path.join(output_dir, os.path.basename(compressed_file).replace(".gz", ""))
            with gzip.open(file_path, 'rb') as f_in:
                with open(output_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            extracted_files.append(output_file)
        else:
            raise ValueError("Formato de arquivo comprimido não suportado. Use arquivos ZIP, TAR ou GZ.")

        return extracted_files
    
    def pivot_or_unpivot(self, df, mode, index_columns=None, pivot_column=None, value_column=None,
                        value_vars=None, var_name="variable", value_name="value", agg_func="sum"):
        """
        (Método Não funcional, ainda em desenvolvimento)
        Realiza pivoteamento ou unpivot (melt) em um DataFrame Spark.

        Parâmetros:
        - df (DataFrame): DataFrame do Spark a ser transformado.
        - mode (str): Tipo de operação. Use "pivot" para pivoteamento e "unpivot" para melt (derretimento).

        Para mode="pivot", são obrigatórios:
        - index_columns (str ou list): Coluna(s) base para agrupamento (ex: 'cod_municipio').
        - pivot_column (str): Coluna cujos valores se tornarão nomes de colunas (ex: 'ano').
        - value_column (str): Coluna com os valores a agregar (ex: 'qtd').
        - agg_func (str): Função agregadora ('sum', 'avg', 'count'). Default: 'sum'.

        Para mode="unpivot", são obrigatórios:
        - index_columns (list): Colunas fixas (ex: ['cod_municipio', 'nome_municipio']).
        - value_vars (list): Lista de colunas que serão convertidas em linhas (ex: ['2008', ..., '2022']).
        - var_name (str): Nome da nova coluna que armazenará os nomes das colunas derretidas (ex: 'ano').
        - value_name (str): Nome da nova coluna que armazenará os valores (ex: 'qtd').

        Retorna:
        - DataFrame transformado conforme a operação especificada.
        """

        if mode not in ("pivot", "unpivot"):
            raise ValueError("Parâmetro 'mode' deve ser 'pivot' ou 'unpivot'.")

        if isinstance(index_columns, str):
            index_columns = [index_columns]

        if mode == "pivot":
            if not pivot_column or not value_column:
                raise ValueError("Parâmetros 'pivot_column' e 'value_column' são obrigatórios para pivot.")

            if agg_func == "sum":
                agg_expr = F.sum(value_column)
            elif agg_func == "avg":
                agg_expr = F.avg(value_column)
            elif agg_func == "count":
                agg_expr = F.count(value_column)
            else:
                raise ValueError(f"Função de agregação '{agg_func}' não suportada.")

            return (
                df.groupBy(index_columns)
                .pivot(pivot_column)
                .agg(agg_expr)
            )

        elif mode == "unpivot":
            if not value_vars or not isinstance(value_vars, list):
                raise ValueError("Parâmetro 'value_vars' (lista de colunas a derreter) é obrigatório para unpivot.")

            stack_expr = f"stack({len(value_vars)}, " + ", ".join(
                [f"'{str(col)}', `{col}`" for col in value_vars]
            ) + f") as ({var_name}, {value_name})"

            return df.selectExpr(*index_columns, stack_expr)

    def convert_to_parquet(self, filename, output_path=None):
        """
        Converte um arquivo CSV ou XLSX em Parquet para otimizar leitura posterior.
        
        Parâmetros:
        - filename (str): Nome do arquivo no diretório de entrada.
        - output_path (str): Caminho de saída opcional. Se não definido, sobrescreve o original.

        Retorna:
        - None. Cria um arquivo .parquet no caminho especificado.

        EX:
        convert_to_parquet("arquivo.csv", "/caminho/para/diretorio")
        """
        file_path = os.path.join(self.input_directory, filename)
        output_path = output_path or file_path.replace(".csv", ".parquet").replace(".xlsx", ".parquet")

        if filename.endswith(".csv"):
            df = pd.read_csv(file_path)
        elif filename.endswith(".xlsx"):
            df = pd.read_excel(file_path)
        else:
            raise ValueError("Formato não suportado. Use CSV ou XLSX.")

        df.to_parquet(output_path, index=False)
        print(f"Arquivo convertido para Parquet: {output_path}")


    def write_partitioned_data(self, df, path, partition_cols):
        """
        Salva um DataFrame particionado por colunas para leitura seletiva.

        Parâmetros:
        - df (DataFrame): Spark DataFrame.
        - path (str): Caminho de saída.
        - partition_cols (list): Lista de colunas para particionar.
        
        Retorna:
        - None. Salva o DataFrame particionado no caminho especificado.

        EX:
        write_partitioned_data(df, "/caminho/para/diretorio", ["coluna1", "coluna2"])
        """
        df.write.partitionBy(partition_cols).mode("overwrite").parquet(path)


    def profile_file_sample(self, filename, sampling_ratio=0.01):
        """
        Realiza leitura parcial de um arquivo CSV para profiling de schema.

        Parâmetros:
        - filename (str): Nome do arquivo.
        - sampling_ratio (float): Proporção da amostra lida.

        Retorna:
        - Spark DataFrame com amostra dos dados.

        EX:
        profile_file_sample("arquivo.csv", 0.01)
        """
        path = os.path.join(self.input_directory, filename)
        df = self.spark.read.option("samplingRatio", sampling_ratio).option("header", True).csv(path)
        df.printSchema()
        df.show(5)
        return df


    def read_csv_with_schema(self, filename, schema):
        """
        Lê um CSV usando schema explícito, evitando inferência automática.

        Parâmetros:
        - filename (str): Nome do arquivo CSV.
        - schema (StructType): Schema Spark definido manualmente.

        Retorna:
        - Spark DataFrame.

        EX:
        schema = StructType([
            StructField("coluna1", StringType(), True),
            StructField("coluna2", IntegerType(), True),
            StructField("coluna3", FloatType(), True)
        ])
        read_csv_with_schema("arquivo.csv", schema)
        """
        path = os.path.join(self.input_directory, filename)
        return self.spark.read.schema(schema).option("header", True).csv(path)


    def parallel_read_csv(self, file_list, num_processes=4):
        """
        Lê vários arquivos CSV em paralelo usando Pandas + multiprocessing.

        Parâmetros:
        - file_list (list): Lista de nomes de arquivos CSV.
        - num_processes (int): Número de processos paralelos.

        Retorna:
        - Lista de DataFrames Pandas.

        EX:
        parallel_read_csv(["arquivo1.csv", "arquivo2.csv"], 4)
        """
        def read_csv_local(file):
            path = os.path.join(self.input_directory, file)
            return pd.read_csv(path)

        with Pool(num_processes) as pool:
            results = pool.map(read_csv_local, file_list)

        return results


    def cache_dataframe(self, df):
        """
        Aplica cache em um DataFrame para acelerar reutilizações.

        Parâmetros:
        - df (DataFrame): Spark DataFrame.

        Retorna:
        - DataFrame Spark com cache aplicado.
        
        EX:
        cache_dataframe(df)
        """
        df.cache()
        df.count()  # força cache
        return df


    def checkpoint_dataframe(self, df):
        """
        Aplica checkpoint em um DataFrame para persistência intermediária.

        Parâmetros:
        - df (DataFrame): Spark DataFrame.

        Retorna:
        - DataFrame após checkpoint.

        EX:
        checkpoint_dataframe(df)
        """
        self.spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
        return df.checkpoint()
    
    def estimate_file_access_times(self):
        """
        Estima o tempo de leitura de cada arquivo no diretório de entrada com base no tipo, tamanho e capacidade da máquina.

        Retorna:
        Uma lista de dicionários com:
        - nome do arquivo
        - tipo
        - tamanho em MB
        - estimativa de tempo (segundos)
        - fatores de ajuste (RAM, CPU)

        EX:
        estimate_file_access_times()
        """
        # === 1. Coleta informações da máquina ===
        cpu_freq = psutil.cpu_freq().max / 1000  # GHz
        ram_available_gb = psutil.virtual_memory().available / (1024 ** 3)

        # === 2. Função de estimativa com ajuste dinâmico ===
        def get_adjusted_estimate(file_type, size_mb, cpu_ghz, ram_gb):
            if file_type == "csv":
                base = size_mb / 100 * 30  # 30s por 100MB como base
            elif file_type == "parquet":
                base = size_mb / 500 * 20  # 20s por 500MB como base
            elif file_type == "xlsx":
                base = size_mb / 50 * 90  # 90s por 50MB como base
            else:
                return -1

            if ram_gb < 8:
                base *= 1.5
            if cpu_ghz < 2.5:
                base *= 1.3

            return round(base)

        # === 3. Itera pelos arquivos ===
        resultados = []
        for file in os.listdir(self.input_directory):
            path = os.path.join(self.input_directory, file)
            if not os.path.isfile(path):
                continue

            size_mb = os.path.getsize(path) / (1024 * 1024)

            if file.endswith(".csv"):
                tipo = "csv"
            elif file.endswith(".parquet"):
                tipo = "parquet"
            elif file.endswith(".xlsx") or file.endswith(".xls"):
                tipo = "xlsx"
            else:
                tipo = "desconhecido"

            estimativa = get_adjusted_estimate(tipo, size_mb, cpu_freq, ram_available_gb)

            resultados.append({
                "arquivo": file,
                "tipo": tipo,
                "tamanho_mb": round(size_mb, 2),
                "estimado_segundos": estimativa,
                "cpu_ghz": round(cpu_freq, 2),
                "ram_disponivel_gb": round(ram_available_gb, 2)
            })
        
        for info in resultados:
            print(f"{info['arquivo']} ({info['tipo']}, {info['tamanho_mb']} MB) → "
                f"{info['estimado_segundos']}s estimado | CPU: {info['cpu_ghz']}GHz | RAM: {info['ram_disponivel_gb']}GB")