from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from database import DB_PATH, get_connection, initialize_database, replace_client_documents, seed_team_members, upsert_client


CLIENT_COLUMNS = [
    "CPF",
    "NOME",
    "Grupo",
    "Reunião",
    "Nivel de Complexidade",
    "Status Preenchimento",
    "Responsável pelo Preenchimento",
    "Status Pós-Envio",
    "Telefone",
    "Senha Gov",
    "Cadastro de Procuração",
]

DOCUMENT_COLUMNS = [
    "Nome Pessoa",
    "Tipo Documento",
    "Instituição",
    "Status",
    "Última Atualização",
    "chave_controle",
]

DEFAULT_TEAM_MEMBERS = [
    ("Wanessa", "comercial"),
    ("Paulo", "preenchimento"),
    ("Valdivone", "preenchimento"),
    ("Michelle", "preenchimento"),
    ("Erlane", "preenchimento"),
    ("Heverton", "revisao"),
    ("Duda", "preenchimento"),
    ("Malu", "preenchimento"),
    ("Renato", "revisao_final"),
]


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return " ".join(str(value).replace("\n", " ").split()).strip()


def normalize_key(value: object) -> str:
    text = normalize_text(value).upper()
    cleaned = (
        text.replace("Á", "A")
        .replace("À", "A")
        .replace("Ã", "A")
        .replace("Â", "A")
        .replace("É", "E")
        .replace("Ê", "E")
        .replace("Í", "I")
        .replace("Ó", "O")
        .replace("Ô", "O")
        .replace("Õ", "O")
        .replace("Ú", "U")
        .replace("Ç", "C")
    )
    return "".join(char if char.isalnum() else " " for char in cleaned).strip()


def canonical_status(value: object) -> str:
    text = normalize_text(value).upper()
    normalized = normalize_key(text)
    if "TRANSMITID" in normalized:
        return "TRANSMITIDO"
    if "REVISAO" in normalized and "RENATO" in normalized:
        return "EM REVISÃO - RENATO"
    if "PREENCHIMENTO" in normalized:
        return "EM PREENCHIMENTO"
    if "PENDENTE" in normalized:
        return "PENDENTE"
    return text


def normalize_digits(value: object) -> str:
    return "".join(char for char in normalize_text(value) if char.isdigit())


def normalize_cpf(value: object) -> str:
    digits = normalize_digits(value)
    if not digits:
        return ""
    digits = digits.zfill(11)
    if len(digits) != 11:
        return digits
    return f"{digits[:3]}.{digits[3:6]}.{digits[6:9]}-{digits[9:]}"


def normalize_phone(value: object) -> str:
    digits = normalize_digits(value)
    if not digits:
        return ""
    if len(digits) == 10:
        return f"({digits[:2]}) {digits[2:6]}-{digits[6:]}"
    if len(digits) == 11:
        return f"({digits[:2]}) {digits[2]} {digits[3:7]}-{digits[7:]}"
    return digits


def split_gov_access(value: object) -> tuple[str, bool]:
    text = normalize_text(value)
    if not text:
        return "", False
    normalized = normalize_key(text)
    if "CERTIFICADO DIGITAL" in normalized:
        return "", True
    return text, False


def documentation_status(total_documents: int, received_documents: int) -> str:
    if total_documents == 0 or received_documents == 0:
        return "Sem documentação"
    if total_documents == received_documents:
        return "Recebido total"
    return "Recebido parcial"


def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, sep=";", encoding="latin1", dtype=str)


def select_columns(dataframe: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    return dataframe.reindex(columns=columns, fill_value="")


def load_clients(path: Path) -> pd.DataFrame:
    dataframe = select_columns(read_csv(path), CLIENT_COLUMNS).copy()
    for column in CLIENT_COLUMNS:
        dataframe[column] = dataframe[column].map(normalize_text)
    dataframe["CPF"] = dataframe["CPF"].map(normalize_cpf)
    dataframe["NOME"] = dataframe["NOME"].replace("", "Sem nome identificado")
    dataframe["Grupo"] = dataframe["Grupo"].replace("", "Sem grupo")
    dataframe["Reunião"] = dataframe["Reunião"].replace("", "Sem reunião informada")
    dataframe["Nivel de Complexidade"] = dataframe["Nivel de Complexidade"].str.title().replace("", "Não informado")
    dataframe["Status Preenchimento"] = dataframe["Status Preenchimento"].map(canonical_status).replace("", "SEM STATUS")
    dataframe["Responsável pelo Preenchimento"] = dataframe["Responsável pelo Preenchimento"].str.upper().replace("", "Não atribuído")
    dataframe["Status Pós-Envio"] = dataframe["Status Pós-Envio"].str.upper().replace("", "Não informado")
    dataframe["Telefone"] = dataframe["Telefone"].map(normalize_phone)
    gov_split = dataframe["Senha Gov"].map(split_gov_access)
    dataframe["Senha Gov"] = gov_split.map(lambda item: item[0])
    dataframe["Tem Certificado Digital"] = gov_split.map(lambda item: item[1])
    dataframe["chave_pessoa"] = dataframe["NOME"].map(normalize_key)
    return dataframe


def load_documents(path: Path) -> pd.DataFrame:
    dataframe = select_columns(read_csv(path), DOCUMENT_COLUMNS).copy()
    for column in DOCUMENT_COLUMNS:
        dataframe[column] = dataframe[column].map(normalize_text)
    dataframe["Nome Pessoa"] = dataframe["Nome Pessoa"].replace("", "Sem nome identificado")
    dataframe["Tipo Documento"] = dataframe["Tipo Documento"].replace("", "")
    dataframe["Instituição"] = dataframe["Instituição"].replace("", "")
    dataframe["Status"] = dataframe["Status"].str.upper().replace("", "SEM STATUS")
    dataframe["Última Atualização"] = pd.to_datetime(
        dataframe["Última Atualização"], format="%d/%m/%Y", errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    dataframe["Última Atualização"] = dataframe["Última Atualização"].fillna("")
    dataframe["chave_pessoa"] = dataframe["Nome Pessoa"].map(normalize_key)
    dataframe = dataframe[
        ~(
            (dataframe["Tipo Documento"] == "")
            & (dataframe["Instituição"] == "")
            & (dataframe["Status"] == "SEM STATUS")
        )
    ].copy()
    return dataframe


def import_into_database(client_path: Path, document_path: Path, db_path: Path) -> None:
    clients_df = load_clients(client_path)
    documents_df = load_documents(document_path)

    initialize_database(db_path)
    with get_connection(db_path) as connection:
        seed_team_members(connection, DEFAULT_TEAM_MEMBERS)

        client_ids: dict[str, int] = {}
        doc_summary = (
            documents_df.groupby("chave_pessoa")
            .agg(
                total_documentos=("Status", "size"),
                recebidos=("Status", lambda values: int((values == "RECEBIDO").sum())),
            )
            .reset_index()
        )
        documentation_map = {
            row["chave_pessoa"]: documentation_status(int(row["total_documentos"]), int(row["recebidos"]))
            for _, row in doc_summary.iterrows()
        }

        for _, row in clients_df.iterrows():
            client_id = upsert_client(
                connection,
                normalized_name=row["chave_pessoa"],
                full_name=row["NOME"],
                group_name=row["Grupo"],
                meeting_status=row["Reunião"],
                complexity_level=row["Nivel de Complexidade"],
                tax_status=row["Status Preenchimento"],
                assigned_preparer=row["Responsável pelo Preenchimento"],
                post_filing_status=row["Status Pós-Envio"],
                documentation_status=documentation_map.get(row["chave_pessoa"], "Sem documentação"),
                cpf=row["CPF"],
                phone=row["Telefone"],
                gov_password=row["Senha Gov"],
                has_digital_certificate=bool(row.get("Tem Certificado Digital", False)),
                power_of_attorney=row["Cadastro de Procuração"],
            )
            client_ids[row["chave_pessoa"]] = client_id

        for _, row in documents_df[["Nome Pessoa", "chave_pessoa"]].drop_duplicates().iterrows():
            if row["chave_pessoa"] not in client_ids:
                client_ids[row["chave_pessoa"]] = upsert_client(
                    connection,
                    normalized_name=row["chave_pessoa"],
                    full_name=row["Nome Pessoa"],
                    documentation_status=documentation_map.get(row["chave_pessoa"], "Sem documentação"),
                )

        for normalized_name, client_id in client_ids.items():
            client_documents = documents_df[documents_df["chave_pessoa"] == normalized_name]
            replace_client_documents(
                connection,
                client_id,
                [
                    {
                        "document_type": document_row["Tipo Documento"],
                        "institution": document_row["Instituição"],
                        "status": document_row["Status"],
                        "last_update": document_row["Última Atualização"],
                        "control_key": document_row["chave_controle"],
                    }
                    for _, document_row in client_documents.iterrows()
                ],
            )

        print(f"Banco criado em: {db_path}")
        print(f"Clientes importados: {len(client_ids)}")
        print(f"Documentos importados: {len(documents_df)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Importa as planilhas do IRPF para um banco SQLite.")
    parser.add_argument("--clients", type=Path, required=True, help="Caminho da planilha de clientes.")
    parser.add_argument("--documents", type=Path, required=True, help="Caminho da planilha de documentos.")
    parser.add_argument("--db", type=Path, default=DB_PATH, help="Caminho do banco SQLite.")
    args = parser.parse_args()
    import_into_database(args.clients, args.documents, args.db)


if __name__ == "__main__":
    main()
