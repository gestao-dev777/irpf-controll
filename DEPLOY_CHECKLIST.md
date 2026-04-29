# Checklist de Deploy do MVP

## Antes de publicar

- Confirmar que `requirements.txt`, `app.py`, `README.md`, `logogestao.png` e `.streamlit/secrets.example.toml` vão para o repositório.
- Confirmar que `data/`, planilhas reais, PDFs e `.streamlit/secrets.toml` não vão para o git.
- Criar os secrets do deploy com `SUPABASE_URL` e `SUPABASE_ANON_KEY`.
- Conferir usuários no Supabase Auth e emails iguais aos da tabela `team_members`.
- Conferir se RLS está ativo no Supabase.
- Fazer login com pelo menos um usuário de cada perfil: Comercial, Preenchimento, Revisão e Cadastros.

## Testes manuais mínimos

- Wanessa: consultar cliente, alterar checklist de documentos e exportar relatório comercial.
- Paulo ou Heverton: acessar Cadastros, subir planilha teste, conferir diferenças e cancelar sem aplicar.
- Preenchimento: alterar responsável/status em lote e salvar andamento de um cliente teste.
- Revisão: filtrar tabela, exportar CSV e salvar posição do dia manualmente.

## Pontos de atenção pós-MVP

- Fortalecer RLS para aplicar permissões diretamente no banco, não só na interface.
- Criar trilha de auditoria para saber quem alterou cliente/documento/status.
- Definir política de backup/export diário do Supabase.
- Monitorar lentidão se a base passar de alguns milhares de clientes/documentos.
