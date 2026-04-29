-- Ativa RLS em todas as tabelas do projeto.
alter table public.team_members enable row level security;
alter table public.clients enable row level security;
alter table public.client_private enable row level security;
alter table public.documents enable row level security;
alter table public.contact_log enable row level security;
alter table public.declaration_checkpoints enable row level security;
alter table public.daily_snapshots enable row level security;

-- Remove policies antigas para permitir reexecucao segura do script.
drop policy if exists "team_members_authenticated_select" on public.team_members;
drop policy if exists "team_members_authenticated_insert" on public.team_members;
drop policy if exists "team_members_authenticated_update" on public.team_members;
drop policy if exists "team_members_authenticated_delete" on public.team_members;

drop policy if exists "clients_authenticated_select" on public.clients;
drop policy if exists "clients_authenticated_insert" on public.clients;
drop policy if exists "clients_authenticated_update" on public.clients;
drop policy if exists "clients_authenticated_delete" on public.clients;

drop policy if exists "documents_authenticated_select" on public.documents;
drop policy if exists "documents_authenticated_insert" on public.documents;
drop policy if exists "documents_authenticated_update" on public.documents;
drop policy if exists "documents_authenticated_delete" on public.documents;

drop policy if exists "contact_log_authenticated_select" on public.contact_log;
drop policy if exists "contact_log_authenticated_insert" on public.contact_log;
drop policy if exists "contact_log_authenticated_update" on public.contact_log;
drop policy if exists "contact_log_authenticated_delete" on public.contact_log;

drop policy if exists "client_private_authenticated_select" on public.client_private;
drop policy if exists "client_private_authenticated_insert" on public.client_private;
drop policy if exists "client_private_authenticated_update" on public.client_private;
drop policy if exists "client_private_authenticated_delete" on public.client_private;

drop policy if exists "declaration_checkpoints_authenticated_select" on public.declaration_checkpoints;
drop policy if exists "declaration_checkpoints_authenticated_insert" on public.declaration_checkpoints;
drop policy if exists "declaration_checkpoints_authenticated_update" on public.declaration_checkpoints;
drop policy if exists "declaration_checkpoints_authenticated_delete" on public.declaration_checkpoints;

drop policy if exists "daily_snapshots_authenticated_select" on public.daily_snapshots;
drop policy if exists "daily_snapshots_authenticated_insert" on public.daily_snapshots;
drop policy if exists "daily_snapshots_authenticated_update" on public.daily_snapshots;
drop policy if exists "daily_snapshots_authenticated_delete" on public.daily_snapshots;

-- Tabelas operacionais: acesso amplo para usuarios autenticados.
create policy "team_members_authenticated_select"
on public.team_members
for select
to authenticated
using (true);

create policy "team_members_authenticated_insert"
on public.team_members
for insert
to authenticated
with check (true);

create policy "team_members_authenticated_update"
on public.team_members
for update
to authenticated
using (true)
with check (true);

create policy "team_members_authenticated_delete"
on public.team_members
for delete
to authenticated
using (true);

create policy "clients_authenticated_select"
on public.clients
for select
to authenticated
using (true);

create policy "clients_authenticated_insert"
on public.clients
for insert
to authenticated
with check (true);

create policy "clients_authenticated_update"
on public.clients
for update
to authenticated
using (true)
with check (true);

create policy "clients_authenticated_delete"
on public.clients
for delete
to authenticated
using (true);

create policy "documents_authenticated_select"
on public.documents
for select
to authenticated
using (true);

create policy "documents_authenticated_insert"
on public.documents
for insert
to authenticated
with check (true);

create policy "documents_authenticated_update"
on public.documents
for update
to authenticated
using (true)
with check (true);

create policy "documents_authenticated_delete"
on public.documents
for delete
to authenticated
using (true);

create policy "contact_log_authenticated_select"
on public.contact_log
for select
to authenticated
using (true);

create policy "contact_log_authenticated_insert"
on public.contact_log
for insert
to authenticated
with check (true);

create policy "contact_log_authenticated_update"
on public.contact_log
for update
to authenticated
using (true)
with check (true);

create policy "contact_log_authenticated_delete"
on public.contact_log
for delete
to authenticated
using (true);

create policy "client_private_authenticated_select"
on public.client_private
for select
to authenticated
using (true);

create policy "client_private_authenticated_insert"
on public.client_private
for insert
to authenticated
with check (true);

create policy "client_private_authenticated_update"
on public.client_private
for update
to authenticated
using (true)
with check (true);

create policy "client_private_authenticated_delete"
on public.client_private
for delete
to authenticated
using (true);

create policy "declaration_checkpoints_authenticated_select"
on public.declaration_checkpoints
for select
to authenticated
using (true);

create policy "declaration_checkpoints_authenticated_insert"
on public.declaration_checkpoints
for insert
to authenticated
with check (true);

create policy "declaration_checkpoints_authenticated_update"
on public.declaration_checkpoints
for update
to authenticated
using (true)
with check (true);

create policy "declaration_checkpoints_authenticated_delete"
on public.declaration_checkpoints
for delete
to authenticated
using (true);

create policy "daily_snapshots_authenticated_select"
on public.daily_snapshots
for select
to authenticated
using (true);

create policy "daily_snapshots_authenticated_insert"
on public.daily_snapshots
for insert
to authenticated
with check (true);

create policy "daily_snapshots_authenticated_update"
on public.daily_snapshots
for update
to authenticated
using (true)
with check (true);

create policy "daily_snapshots_authenticated_delete"
on public.daily_snapshots
for delete
to authenticated
using (true);
