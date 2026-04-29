from __future__ import annotations

import argparse
from pathlib import Path

import psycopg

from bootstrap_database import documentation_status, load_clients, load_documents


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CLIENTS_PATH = BASE_DIR / "INFORMAÇÕES DE CLIENTES(Relatório) (2).csv"
DEFAULT_DOCUMENTS_PATH = BASE_DIR / "controle_documento(Controle Documentos) (2).csv"
DEFAULT_SCHEMA_PATH = BASE_DIR / "supabase_schema.sql"
DEFAULT_CREDS_PATH = BASE_DIR / "data" / "supabase-credentials.txt"

TEAM_MEMBERS = [
    {
        "name": "Wanessa",
        "display_name": "Wanessa",
        "email": "wanessa.aparecida@gestaocontabil.com",
        "role": "comercial",
        "allowed_sectors": "Comercial,Preenchimento,Revisão",
        "can_manage_records": True,
        "permission_level": "full",
    },
    {
        "name": "Paulo",
        "display_name": "Paulo",
        "email": "paulo.nunes@gestaocontabil.com",
        "role": "preenchimento",
        "allowed_sectors": "Comercial,Preenchimento,Revisão,Cadastros",
        "can_manage_records": True,
        "permission_level": "full",
    },
    {
        "name": "Valdivone",
        "display_name": "Valdivone",
        "email": "valdivone.dias@gestaocontabil.com",
        "role": "preenchimento",
        "allowed_sectors": "Preenchimento,Revisão",
        "can_manage_records": True,
        "permission_level": "full",
    },
    {
        "name": "Michelle",
        "display_name": "Michelle",
        "email": "michelle.mustafa@gestaocontabil.com",
        "role": "preenchimento",
        "allowed_sectors": "Preenchimento,Revisão",
        "can_manage_records": True,
        "permission_level": "full",
    },
    {
        "name": "Erlane",
        "display_name": "Erlane",
        "email": "",
        "role": "preenchimento",
        "allowed_sectors": "Preenchimento",
        "can_manage_records": False,
        "permission_level": "full",
    },
    {
        "name": "Heverton",
        "display_name": "Heverton",
        "email": "heverton@gestaocontabil.com",
        "role": "revisao",
        "allowed_sectors": "Comercial,Preenchimento,Revisão,Cadastros",
        "can_manage_records": True,
        "permission_level": "full",
    },
    {
        "name": "Duda",
        "display_name": "Duda",
        "email": "maria.lins@gestaocontabil.com",
        "role": "preenchimento",
        "allowed_sectors": "Preenchimento",
        "can_manage_records": False,
        "permission_level": "status_only",
    },
    {
        "name": "Malu",
        "display_name": "Malu",
        "email": "maria.luiza@gestaocontabil.com",
        "role": "preenchimento",
        "allowed_sectors": "Preenchimento",
        "can_manage_records": False,
        "permission_level": "status_only",
    },
    {
        "name": "Renato",
        "display_name": "Renato",
        "email": "renato@gestaocontabil.com",
        "role": "revisao_final",
        "allowed_sectors": "Comercial,Preenchimento,Revisão",
        "can_manage_records": True,
        "permission_level": "full",
    },
]


def read_credentials(path: Path) -> dict[str, str]:
    credentials: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" not in line or line.strip().startswith("-"):
            continue
        key, value = line.split("=", 1)
        credentials[key.strip()] = value.strip()
    return credentials


def execute_schema(connection: psycopg.Connection, schema_path: Path) -> None:
    sql = schema_path.read_text(encoding="utf-8")
    with connection.cursor() as cursor:
        cursor.execute(sql)
    connection.commit()


def seed_team(connection: psycopg.Connection) -> None:
    with connection.cursor() as cursor:
        for member in TEAM_MEMBERS:
            cursor.execute(
                """
                insert into public.team_members (
                    name,
                    display_name,
                    email,
                    role,
                    allowed_sectors,
                    can_manage_records,
                    permission_level
                )
                values (%s, %s, %s, %s, %s, %s, %s)
                on conflict (name) do update set
                    display_name = excluded.display_name,
                    email = excluded.email,
                    role = excluded.role,
                    allowed_sectors = excluded.allowed_sectors,
                    can_manage_records = excluded.can_manage_records,
                    permission_level = excluded.permission_level,
                    updated_at = now()
                """,
                (
                    member["name"],
                    member["display_name"],
                    member["email"],
                    member["role"],
                    member["allowed_sectors"],
                    member["can_manage_records"],
                    member["permission_level"],
                ),
            )
    connection.commit()


def upsert_client(connection: psycopg.Connection, row: dict, documentation: str) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            insert into public.clients (
                normalized_name,
                full_name,
                group_name,
                meeting_status,
                complexity_level,
                tax_status,
                assigned_preparer,
                post_filing_status,
                documentation_status
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict (normalized_name) do update set
                full_name = excluded.full_name,
                group_name = excluded.group_name,
                meeting_status = excluded.meeting_status,
                complexity_level = excluded.complexity_level,
                tax_status = excluded.tax_status,
                assigned_preparer = excluded.assigned_preparer,
                post_filing_status = excluded.post_filing_status,
                documentation_status = excluded.documentation_status,
                active = true,
                updated_at = now()
            returning id
            """,
            (
                row["chave_pessoa"],
                row["NOME"],
                row["Grupo"],
                row["Reunião"],
                row["Nivel de Complexidade"],
                row["Status Preenchimento"],
                row["Responsável pelo Preenchimento"],
                row["Status Pós-Envio"],
                documentation,
            ),
        )
        client_id = cursor.fetchone()[0]
        cursor.execute(
            """
            insert into public.client_private (
                client_id,
                cpf,
                phone,
                gov_password,
                has_digital_certificate,
                power_of_attorney,
                notes,
                updated_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, now())
            on conflict (client_id) do update set
                cpf = excluded.cpf,
                phone = excluded.phone,
                gov_password = excluded.gov_password,
                has_digital_certificate = excluded.has_digital_certificate,
                power_of_attorney = excluded.power_of_attorney,
                notes = excluded.notes,
                updated_at = now()
            """,
            (
                client_id,
                row["CPF"],
                row["Telefone"],
                row["Senha Gov"],
                bool(row.get("Tem Certificado Digital", False)),
                row["Cadastro de Procuração"],
                "",
            ),
        )
        return client_id


def replace_documents(connection: psycopg.Connection, client_id: int, docs: list[dict]) -> None:
    with connection.cursor() as cursor:
        cursor.execute("delete from public.documents where client_id = %s", (client_id,))
        for doc in docs:
            cursor.execute(
                """
                insert into public.documents (
                    client_id,
                    document_type,
                    institution,
                    status,
                    last_update,
                    control_key
                )
                values (%s, %s, %s, %s, %s, %s)
                """,
                (
                    client_id,
                    doc["Tipo Documento"],
                    doc["Instituição"],
                    doc["Status"],
                    doc["Última Atualização"] or None,
                    doc["chave_controle"],
                ),
            )


def import_data(connection: psycopg.Connection, clients_path: Path, documents_path: Path) -> tuple[int, int]:
    clients_df = load_clients(clients_path)
    documents_df = load_documents(documents_path)

    summary = (
        documents_df.groupby("chave_pessoa")
        .agg(
            total_documentos=("Status", "size"),
            recebidos=("Status", lambda values: int((values == "RECEBIDO").sum())),
        )
        .reset_index()
    )
    documentation_map = {
        row["chave_pessoa"]: documentation_status(int(row["total_documentos"]), int(row["recebidos"]))
        for _, row in summary.iterrows()
    }

    client_ids: dict[str, int] = {}
    for _, row in clients_df.iterrows():
        client_id = upsert_client(connection, row, documentation_map.get(row["chave_pessoa"], "Sem documentação"))
        client_ids[row["chave_pessoa"]] = client_id

    for _, row in documents_df[["Nome Pessoa", "chave_pessoa"]].drop_duplicates().iterrows():
        if row["chave_pessoa"] in client_ids:
            continue
        placeholder = {
            "chave_pessoa": row["chave_pessoa"],
            "NOME": row["Nome Pessoa"],
            "Grupo": "",
            "Reunião": "",
            "Nivel de Complexidade": "",
            "Status Preenchimento": "SEM STATUS",
            "Responsável pelo Preenchimento": "Não atribuído",
            "Status Pós-Envio": "",
            "CPF": "",
            "Telefone": "",
            "Senha Gov": "",
            "Cadastro de Procuração": "",
        }
        client_ids[row["chave_pessoa"]] = upsert_client(
            connection,
            placeholder,
            documentation_map.get(row["chave_pessoa"], "Sem documentação"),
        )

    for normalized_name, client_id in client_ids.items():
        docs = documents_df[documents_df["chave_pessoa"] == normalized_name]
        replace_documents(connection, client_id, [doc for _, doc in docs.iterrows()])

    connection.commit()
    return len(client_ids), len(documents_df)


def main() -> None:
    parser = argparse.ArgumentParser(description="Cria schema e importa dados no Supabase Postgres.")
    parser.add_argument("--creds", type=Path, default=DEFAULT_CREDS_PATH)
    parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA_PATH)
    parser.add_argument("--clients", type=Path, default=DEFAULT_CLIENTS_PATH)
    parser.add_argument("--documents", type=Path, default=DEFAULT_DOCUMENTS_PATH)
    args = parser.parse_args()

    credentials = read_credentials(args.creds)
    connection_string = credentials["DATABASE_URL_ENCODED"]

    with psycopg.connect(connection_string, connect_timeout=10) as connection:
        execute_schema(connection, args.schema)
        seed_team(connection)
        client_count, document_count = import_data(connection, args.clients, args.documents)
        print("Schema criado e dados importados no Supabase.")
        print(f"Clientes: {client_count}")
        print(f"Documentos: {document_count}")


if __name__ == "__main__":
    main()
