# Publicação e uso da biblioteca nos notebooks

Objetivo: em qualquer notebook serverless, basta

```python
from BaseDataProcessorSpark import BaseDataProcessorSpark
from DataDictionaryBuilder import DataDictionaryBuilder
```

sem `%pip install`, sem `sys.path` e sem bump manual de versão.

## Como funciona

```
push no GitHub ─► GitHub Actions ──► wheel dataeng_codes-1.0.0.post<timestamp>
                                     │
                                     ├─► /Volumes/<cat>/<schema>/libs/<wheel>
                                     ├─► /Volumes/<cat>/<schema>/libs/dataeng_base_env.yml  (aponta p/ o wheel novo)
                                     └─► refresh do base environment "dataeng-codes"
                                                     │
                     base environment padrão do workspace ──► todo notebook serverless novo já tem o pacote
```

- A versão é gerada no build (`.post<timestamp>`), então o cache do serverless sempre reinstala.
- O YAML fica sempre no mesmo caminho; só o conteúdo muda.
- Sem GitHub Actions, o notebook `build_wheel.py` faz exatamente o mesmo a partir da Git folder (Run all).

## Configuração (uma vez só)

1. **Volume** para os wheels:
   ```sql
   CREATE VOLUME IF NOT EXISTS <catalog>.<schema>.libs;
   ```
2. **Token**: crie um PAT (User Settings > Developer > Access tokens) com um usuário admin do workspace.
3. **GitHub** (Settings > Secrets and variables > Actions):
   - Secrets: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`
   - Variable: `DATABRICKS_VOLUME_LIBS` = `/Volumes/<catalog>/<schema>/libs`
4. Copie o workflow para `.github/workflows/publish-dataeng-codes.yml` (na raiz do repositório) e rode-o
   uma vez em **Actions > Run workflow**. Isso cria o primeiro wheel e o `dataeng_base_env.yml`.
5. **Base environment**: Settings > Workspace admin > Compute > Base environments for serverless compute > Manage > Create
   - Nome: `dataeng-codes`
   - Arquivo: `/Volumes/<catalog>/<schema>/libs/dataeng_base_env.yml`
   - Marque como padrão para novos notebooks
   - Em *Advanced*, use o ID `dataeng-codes`
6. No GitHub, crie a variable `DATABRICKS_BASE_ENV_ID` = `dataeng-codes`. A partir daí cada push já faz o refresh.

## Uso

- **Notebooks novos**: já nascem com o base environment padrão; é só importar.
- **Notebooks já existentes**: painel *Environment* > *Base environment* > `dataeng-codes` (uma vez por notebook).
- **Depois de um push**: notebooks com sessão aberta precisam reiniciar a sessão (ou desconectar/reconectar)
  para pegar a versão nova.
- **Jobs**: em tarefas de notebook, o base environment vale quando está configurado no próprio notebook
  (suporte a base environments em jobs está em Beta).

Para conferir a versão carregada:

```python
from importlib.metadata import version
print(version("dataeng-codes"))
```

## Desenvolvimento da própria biblioteca

Notebooks que ficam **dentro** de `Databricks_codes/codes/` importam o `.py` local (a pasta do notebook vem antes
do pacote instalado no `sys.path`). Com `%autoreload 2`, edições no código valem na hora, sem build.
Quando estiver bom: commit + push, e o workflow publica para os demais notebooks.
