# IRPF - Controle de Declarações

Aplicativo interno em Streamlit para controlar o fluxo de declarações de IRPF por setor, usando Supabase como banco principal.

## Rodar localmente

```bash
pip install -r requirements.txt
streamlit run app.py
```

O app lê as credenciais nesta ordem:

- variáveis de ambiente `SUPABASE_URL` e `SUPABASE_ANON_KEY`;
- secrets do Streamlit;
- arquivo local `data/supabase-credentials.txt`.

Para deploy no Streamlit, cadastre os secrets com base em `.streamlit/secrets.example.toml`.

## Fluxo do sistema

- `Comercial`: consulta clientes, mantém cadastro e atualiza checklist de documentos recebidos.
- `Preenchimento`: lista geral filtrável, alteração em lote de responsável/status e checklist de andamento da declaração.
- `Revisão`: visão consolidada do processo, filtros gerenciais, exportação e histórico diário.
- `Cadastros`: importação e comparação de planilhas com o banco, visível apenas para Paulo e Heverton.

## Segurança operacional

- As planilhas, PDFs, exports e credenciais locais estão no `.gitignore`.
- O app usa login do Supabase Auth e libera telas conforme `team_members`.
- Dados sensíveis ficam na tabela `client_private` e só aparecem dentro do app autenticado.
- Para produção mais rígida, revise o RLS para aplicar permissões também no banco, além das travas da interface.

## Deploy MVP

Antes de publicar, conferir:

- `requirements.txt` presente no repositório.
- `logogestao.png` presente no repositório.
- Secrets configurados no ambiente do deploy.
- Usuários criados no Supabase Auth.
- Tabela `team_members` preenchida com emails e setores corretos.
- RLS ativo no Supabase.
- Planilhas reais fora do git.

## Validação rápida

```bash
python -m py_compile app.py setup_supabase.py
```
