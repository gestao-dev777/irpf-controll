from __future__ import annotations

import io
import os
import re
import unicodedata
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import streamlit as st
from supabase import Client, create_client


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

PREPARATION_STEPS = [
    ("cadastro", "Informações cadastrais, permanece?"),
    ("bens_direitos", "Bens e direitos, permanece?"),
    ("dividas_emprestimos", "Possui dívidas, AFAC ou empréstimos?"),
    ("revisao_interna", "Declaração revisada e encaminhada para revisão?"),
]

TEAM_FALLBACK = [
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

STATUS_OPTIONS = [
    "PENDENTE",
    "EM PREENCHIMENTO",
    "PRONTO PARA REVISÃO",
    "EM REVISÃO - RENATO",
    "AJUSTE - HEVERTON",
    "AGUARDANDO REUNIÃO",
    "TRANSMITIDO",
    "SEM STATUS",
]

AVAILABLE_DECLARATION_STATUSES = {"PENDENTE", "SEM STATUS"}
SECTOR_OPTIONS = ["Comercial", "Preenchimento", "Revisão", "Cadastros"]

LOCAL_CLIENT_SAMPLE = Path(r"C:\Users\user\Downloads\INFORMAÇÕES DE CLIENTES(Relatório) (8).csv")
LOCAL_DOCUMENT_SAMPLE = Path(r"C:\Users\user\Downloads\controle_documento(Controle Documentos (2)).csv")
DATA_DIR = Path(__file__).resolve().parent / "data"
SNAPSHOT_PATH = DATA_DIR / "historico_snapshots.csv"
SUPABASE_CREDS_PATH = DATA_DIR / "supabase-credentials.txt"
LOGO_PATH = Path(__file__).resolve().parent / "logogestao.png"
BUNDLE_CACHE_TTL_SECONDS = 20

STANDARD_IMPORT_COLUMNS = [
    "NOME",
    "CPF",
    "Grupo",
    "Reunião",
    "Nivel de Complexidade",
    "Status Preenchimento",
    "Responsável pelo Preenchimento",
    "Status Pós-Envio",
    "Telefone",
    "Senha Gov",
    "Cadastro de Procuração",
    "Tipo Documento",
    "Instituição",
    "Status Documento",
    "Última Atualização",
    "chave_controle",
]


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return re.sub(r"\s+", " ", str(value).replace("\n", " ")).strip()


def normalize_key(value: object) -> str:
    text = normalize_text(value).upper()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    return re.sub(r"[^A-Z0-9]+", " ", text).strip()


def normalize_column(value: object) -> str:
    return normalize_key(value).lower()


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
    if "CERTIFICADO DIGITAL" in normalize_key(text):
        return "", True
    return text, False


def safe_percent(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return round((numerator / denominator) * 100, 1)


def documentation_hint(value: object) -> str:
    normalized = normalize_key(value)
    if "RECEBIDO TOTAL" in normalized:
        return "Recebido total"
    if "RECEBIDO PARCIAL" in normalized:
        return "Recebido parcial"
    if "SEM DOCUMENTACAO" in normalized:
        return "Sem documentação"
    return ""


def canonical_status(value: object) -> str:
    text = normalize_text(value).upper()
    normalized = normalize_key(text)
    if documentation_hint(text):
        return "PENDENTE"
    if "TRANSMITID" in normalized:
        return "TRANSMITIDO"
    if "REVISAO" in normalized and "RENATO" in normalized:
        return "EM REVISÃO - RENATO"
    if "PREENCHIMENTO" in normalized:
        return "EM PREENCHIMENTO"
    if "PENDENTE" in normalized:
        return "PENDENTE"
    if "AJUSTE" in normalized:
        return text
    return text or "SEM STATUS"


def documentation_status(total: int, received: int) -> str:
    if total == 0 or received == 0:
        return "Sem documentação"
    if received == total:
        return "Recebido total"
    return "Recebido parcial"


def list_join(values: pd.Series) -> str:
    cleaned = [normalize_text(value) for value in values if normalize_text(value)]
    return "\n".join(dict.fromkeys(cleaned))


def is_bank_document(document_type: str, institution: str) -> bool:
    text = normalize_key(f"{document_type} {institution}")
    bank_terms = [
        "BANCO",
        "ITAU",
        "BRADESCO",
        "SANTANDER",
        "CAIXA",
        "NUBANK",
        "NU PAGAMENTOS",
        "SICOOB",
        "SICREDI",
        "XP",
        "BTG",
        "INTER",
        "C6",
        "MERCADO PAGO",
        "RICO",
        "CLEAR",
        "GENIAL",
        "SOFISA",
        "ORIGINAL",
        "BMG",
        "SAFRA",
        "PAN",
        "COOPERATIVA",
        "CORRETORA",
        "INVEST",
    ]
    return any(term in text for term in bank_terms)


def document_category(document_type: object, institution: object) -> str:
    doc_type = normalize_key(document_type)
    institution_key = normalize_key(institution)
    if "DESPES" in doc_type or "PAGAMENTO" in doc_type or "DEPENDENTE" in doc_type:
        return "despesas_dedutiveis"
    if is_bank_document(doc_type, institution_key):
        return "informes_bancarios"
    if "ISENT" in doc_type:
        return "renda_isenta"
    if "EXCLUS" in doc_type:
        return "tributacao_exclusiva"
    if "REND" in doc_type or "INFORMATIVO" in doc_type:
        return "renda_tributavel"
    return "outros_documentos"


SECTION_LABELS = {
    "renda_tributavel": "Fontes de renda tributáveis",
    "renda_isenta": "Rendas isentas",
    "tributacao_exclusiva": "Tributação exclusiva",
    "despesas_dedutiveis": "Pagamentos efetuados / despesas dedutíveis",
    "informes_bancarios": "Informes bancários",
    "outros_documentos": "Outros documentos",
}


def read_key_value_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" not in line or line.strip().startswith("-"):
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def load_supabase_public_config() -> dict[str, str]:
    file_values = read_key_value_file(SUPABASE_CREDS_PATH)
    secrets_values: dict[str, str] = {}
    try:
        secrets_values = {
            "SUPABASE_URL": st.secrets.get("SUPABASE_URL", ""),
            "NEXT_PUBLIC_SUPABASE_URL": st.secrets.get("NEXT_PUBLIC_SUPABASE_URL", ""),
            "SUPABASE_ANON_KEY": st.secrets.get("SUPABASE_ANON_KEY", ""),
            "NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY": st.secrets.get("NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY", ""),
        }
        if "supabase" in st.secrets:
            supabase_section = st.secrets["supabase"]
            secrets_values.update(
                {
                    "SUPABASE_URL": supabase_section.get("SUPABASE_URL", secrets_values["SUPABASE_URL"]),
                    "NEXT_PUBLIC_SUPABASE_URL": supabase_section.get(
                        "NEXT_PUBLIC_SUPABASE_URL",
                        secrets_values["NEXT_PUBLIC_SUPABASE_URL"],
                    ),
                    "SUPABASE_ANON_KEY": supabase_section.get("SUPABASE_ANON_KEY", secrets_values["SUPABASE_ANON_KEY"]),
                    "NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY": supabase_section.get(
                        "NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY",
                        secrets_values["NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY"],
                    ),
                }
            )
    except Exception:
        secrets_values = {}
    url = (
        os.getenv("SUPABASE_URL")
        or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
        or secrets_values.get("SUPABASE_URL")
        or secrets_values.get("NEXT_PUBLIC_SUPABASE_URL")
        or file_values.get("SUPABASE_URL")
        or file_values.get("NEXT_PUBLIC_SUPABASE_URL")
        or ""
    )
    anon_key = (
        os.getenv("SUPABASE_ANON_KEY")
        or os.getenv("NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY")
        or secrets_values.get("SUPABASE_ANON_KEY")
        or secrets_values.get("NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY")
        or file_values.get("SUPABASE_ANON_KEY")
        or file_values.get("NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY")
        or ""
    )
    if not url or not anon_key:
        return {}
    return {"url": url, "anon_key": anon_key}


def clear_supabase_session() -> None:
    for key in ["supabase_access_token", "supabase_refresh_token", "supabase_user_email"]:
        st.session_state.pop(key, None)


def build_supabase_client() -> Client | None:
    config = load_supabase_public_config()
    if not config:
        return None
    client = create_client(config["url"], config["anon_key"])
    access_token = st.session_state.get("supabase_access_token")
    refresh_token = st.session_state.get("supabase_refresh_token")
    if access_token and refresh_token:
        try:
            auth_response = client.auth.set_session(access_token, refresh_token)
            session = getattr(auth_response, "session", None)
            if session is not None:
                st.session_state["supabase_access_token"] = session.access_token
                st.session_state["supabase_refresh_token"] = session.refresh_token
                st.session_state["supabase_user_email"] = getattr(session.user, "email", "")
        except Exception:
            clear_supabase_session()
            return None
    return client


def invalidate_data_cache() -> None:
    st.session_state.pop("supabase_bundle_cache", None)
    st.session_state.pop("supabase_bundle_loaded_at", None)


def load_supabase_bundle_cached(client: Client) -> dict[str, pd.DataFrame]:
    cached_bundle = st.session_state.get("supabase_bundle_cache")
    loaded_at = st.session_state.get("supabase_bundle_loaded_at")
    if cached_bundle is not None and isinstance(loaded_at, datetime):
        cache_age = (datetime.now() - loaded_at).total_seconds()
        if cache_age < BUNDLE_CACHE_TTL_SECONDS:
            return cached_bundle
    bundle = load_supabase_bundle(client)
    st.session_state["supabase_bundle_cache"] = bundle
    st.session_state["supabase_bundle_loaded_at"] = datetime.now()
    return bundle


def fetch_all_rows(client: Client, table_name: str, columns: str, page_size: int = 1000) -> list[dict]:
    rows: list[dict] = []
    start = 0
    while True:
        response = client.table(table_name).select(columns).range(start, start + page_size - 1).execute()
        data = response.data or []
        rows.extend(data)
        if len(data) < page_size:
            break
        start += page_size
    return rows


def read_csv_bytes(file_bytes: bytes) -> pd.DataFrame:
    errors: list[str] = []
    for encoding in ["utf-8-sig", "utf-8", "latin1", "cp1252"]:
        try:
            return pd.read_csv(
                io.BytesIO(file_bytes),
                sep=None,
                engine="python",
                encoding=encoding,
                dtype=str,
            )
        except Exception as exc:
            errors.append(f"{encoding}: {exc}")
    raise ValueError("Não foi possível ler o CSV. Tentativas: " + " | ".join(errors))


def read_table_file(file_bytes: bytes, file_name: str) -> pd.DataFrame:
    suffix = Path(file_name).suffix.lower()
    if suffix in [".xlsx", ".xls"]:
        return pd.read_excel(io.BytesIO(file_bytes), dtype=str)
    return read_csv_bytes(file_bytes)


def select_columns(df: pd.DataFrame, expected_columns: list[str]) -> pd.DataFrame:
    normalized_to_original = {normalize_column(column): column for column in df.columns}
    selected: dict[str, pd.Series] = {}
    for expected in expected_columns:
        original = normalized_to_original.get(normalize_column(expected))
        selected[expected] = df[original] if original else pd.Series([""] * len(df))
    return pd.DataFrame(selected)


def parse_clients(file_bytes: bytes, file_name: str) -> pd.DataFrame:
    raw_df = read_table_file(file_bytes, file_name)
    df = select_columns(raw_df, CLIENT_COLUMNS)

    for column in CLIENT_COLUMNS:
        df[column] = df[column].map(normalize_text)

    gov_split = df["Senha Gov"].map(split_gov_access)
    df["CPF"] = df["CPF"].map(normalize_cpf)
    df["NOME"] = df["NOME"].replace("", "Sem nome identificado")
    df["Grupo"] = df["Grupo"].replace("", "Sem grupo")
    df["Reunião"] = df["Reunião"].replace("", "Sem reunião informada")
    df["Nivel de Complexidade"] = (
        df["Nivel de Complexidade"].str.strip().str.title().replace("", "Não informado")
    )
    df["Documentação Informada"] = df["Status Preenchimento"].map(documentation_hint)
    df["Status Preenchimento"] = df["Status Preenchimento"].map(canonical_status)
    df["Responsável pelo Preenchimento"] = (
        df["Responsável pelo Preenchimento"].str.upper().replace("", "Não atribuído")
    )
    df["Status Pós-Envio"] = df["Status Pós-Envio"].str.upper().replace("", "Não informado")
    df["Telefone"] = df["Telefone"].map(normalize_phone)
    df["Senha Gov"] = gov_split.map(lambda item: item[0])
    df["Tem Certificado Digital"] = gov_split.map(lambda item: item[1])
    df["Cadastro de Procuração"] = df["Cadastro de Procuração"].replace("", "Não informado")
    df["chave_pessoa"] = df["NOME"].map(normalize_key)
    return df


def parse_documents(file_bytes: bytes, file_name: str) -> pd.DataFrame:
    raw_df = read_table_file(file_bytes, file_name)
    df = select_columns(raw_df, DOCUMENT_COLUMNS)

    for column in DOCUMENT_COLUMNS:
        df[column] = df[column].map(normalize_text)

    df["Nome Pessoa"] = df["Nome Pessoa"].replace("", "Sem nome identificado")
    df["Tipo Documento"] = df["Tipo Documento"].replace("", "Não informado")
    df["Instituição"] = df["Instituição"].replace("", "Não informada")
    df["Status"] = df["Status"].str.upper().replace("", "SEM STATUS")
    df["Última Atualização"] = pd.to_datetime(
        df["Última Atualização"], format="%d/%m/%Y", errors="coerce"
    )
    df["documento_descricao"] = df["Tipo Documento"] + " - " + df["Instituição"]
    df["chave_pessoa"] = df["Nome Pessoa"].map(normalize_key)
    return df


def default_team_df() -> pd.DataFrame:
    return pd.DataFrame(TEAM_FALLBACK)


def parse_allowed_sectors(value: object) -> list[str]:
    sectors = [normalize_text(item) for item in normalize_text(value).split(",") if normalize_text(item)]
    return [sector for sector in SECTOR_OPTIONS if sector in sectors]


def get_user_profile(team_df: pd.DataFrame, user_email: str, source: str) -> dict[str, object]:
    if source != "Supabase":
        return {
            "email": "",
            "display_name": "Equipe",
            "allowed_sectors": ["Comercial", "Preenchimento", "Revisão"],
            "can_manage_records": False,
            "permission_level": "full",
        }
    normalized_email = normalize_text(user_email).lower()
    if normalized_email and not team_df.empty and "email" in team_df.columns:
        match = team_df[team_df["email"].map(lambda value: normalize_text(value).lower()) == normalized_email]
        if not match.empty:
            row = match.iloc[0]
            return {
                "email": normalized_email,
                "display_name": normalize_text(row.get("display_name", "")) or normalize_text(row.get("name", "")) or normalized_email,
                "allowed_sectors": parse_allowed_sectors(row.get("allowed_sectors", "")),
                "can_manage_records": bool(row.get("can_manage_records", False)),
                "permission_level": normalize_text(row.get("permission_level", "")) or "full",
            }
    return {
        "email": normalized_email,
        "display_name": normalized_email or "Usuário",
        "allowed_sectors": [],
        "can_manage_records": False,
        "permission_level": "read_only",
    }


def load_supabase_bundle(client: Client) -> dict[str, pd.DataFrame]:
    client_rows = fetch_all_rows(
        client,
        "clients",
        "id, normalized_name, full_name, group_name, meeting_status, complexity_level, tax_status, assigned_preparer, post_filing_status, updated_at",
    )
    document_rows = fetch_all_rows(
        client,
        "documents",
        "id, client_id, document_type, institution, status, last_update, control_key",
    )
    private_rows = fetch_all_rows(
        client,
        "client_private",
        "client_id, cpf, phone, gov_password, has_digital_certificate, power_of_attorney",
    )
    team_rows = fetch_all_rows(
        client,
        "team_members",
        "name, display_name, email, role, allowed_sectors, can_manage_records, permission_level, active",
    )
    checkpoint_rows = fetch_all_rows(
        client,
        "declaration_checkpoints",
        "client_id, step_key, step_label, completed, note, updated_by, updated_at",
    )

    client_df = pd.DataFrame(client_rows).fillna("")
    if client_df.empty:
        clients_df = pd.DataFrame(columns=CLIENT_COLUMNS + ["chave_pessoa", "client_id", "client_updated_at"])
    else:
        clients_df = pd.DataFrame(
            {
                "client_id": client_df["id"],
                "CPF": "",
                "NOME": client_df["full_name"].map(normalize_text),
                "Grupo": client_df["group_name"].map(normalize_text).replace("", "Sem grupo"),
                "Reunião": client_df["meeting_status"].map(normalize_text).replace("", "Sem reunião informada"),
                "Nivel de Complexidade": client_df["complexity_level"].map(normalize_text).replace("", "Não informado"),
                "Documentação Informada": client_df["tax_status"].map(documentation_hint),
                "Status Preenchimento": client_df["tax_status"].map(canonical_status),
                "Responsável pelo Preenchimento": client_df["assigned_preparer"].map(normalize_text).replace("", "Não atribuído"),
                "Status Pós-Envio": client_df["post_filing_status"].map(normalize_text).replace("", "Não informado"),
                "Telefone": "",
                "Senha Gov": "",
                "Tem Certificado Digital": False,
                "Cadastro de Procuração": "",
                "chave_pessoa": client_df["normalized_name"].map(normalize_text),
                "client_updated_at": pd.to_datetime(client_df["updated_at"], errors="coerce"),
            }
        )

    client_lookup = {
        int(row["id"]): {
            "full_name": normalize_text(row["full_name"]),
            "normalized_name": normalize_text(row["normalized_name"]),
        }
        for row in client_rows
    }

    document_source = []
    for row in document_rows:
        client_info = client_lookup.get(
            int(row["client_id"]),
            {"full_name": "Sem nome identificado", "normalized_name": ""},
        )
        document_source.append(
            {
                "document_id": int(row["id"]),
                "client_id": int(row["client_id"]),
                "Nome Pessoa": client_info["full_name"],
                "Tipo Documento": normalize_text(row.get("document_type", "")) or "Não informado",
                "Instituição": normalize_text(row.get("institution", "")) or "Não informada",
                "Status": normalize_text(row.get("status", "")).upper() or "SEM STATUS",
                "Última Atualização": pd.to_datetime(row.get("last_update"), errors="coerce"),
                "chave_controle": normalize_text(row.get("control_key", "")),
                "documento_descricao": (
                    f"{normalize_text(row.get('document_type', '')) or 'Não informado'} - "
                    f"{normalize_text(row.get('institution', '')) or 'Não informada'}"
                ),
                "chave_pessoa": client_info["normalized_name"] or normalize_key(client_info["full_name"]),
            }
        )
    documents_df = pd.DataFrame(document_source)
    if documents_df.empty:
        documents_df = pd.DataFrame(
            columns=DOCUMENT_COLUMNS + ["documento_descricao", "chave_pessoa", "client_id", "document_id"]
        )

    private_df = pd.DataFrame(private_rows).fillna("")
    if private_df.empty:
        private_df = pd.DataFrame(
            columns=["client_id", "CPF", "Telefone", "Senha Gov", "Tem Certificado Digital", "Cadastro de Procuração"]
        )
    else:
        private_df = pd.DataFrame(
            {
                "client_id": private_df["client_id"].astype(int),
                "CPF": private_df["cpf"].map(normalize_text),
                "Telefone": private_df["phone"].map(normalize_text),
                "Senha Gov": private_df["gov_password"].map(normalize_text),
                "Tem Certificado Digital": private_df["has_digital_certificate"].fillna(False).astype(bool),
                "Cadastro de Procuração": private_df["power_of_attorney"].map(normalize_text),
            }
        )

    team_df = pd.DataFrame(team_rows).fillna("")
    if team_df.empty:
        team_df = default_team_df()

    checkpoints_df = pd.DataFrame(checkpoint_rows)
    if checkpoints_df.empty:
        checkpoints_df = pd.DataFrame(
            columns=["client_id", "step_key", "step_label", "completed", "note", "updated_by", "updated_at"]
        )
    else:
        checkpoints_df["client_id"] = checkpoints_df["client_id"].astype(int)
        checkpoints_df["completed"] = checkpoints_df["completed"].fillna(False).astype(bool)
        checkpoints_df["note"] = checkpoints_df["note"].fillna("").map(normalize_text)
        checkpoints_df["updated_by"] = checkpoints_df["updated_by"].fillna("").map(normalize_text)
        checkpoints_df["updated_at"] = pd.to_datetime(checkpoints_df["updated_at"], errors="coerce")

    return {
        "clients_df": clients_df,
        "documents_df": documents_df,
        "private_df": private_df,
        "team_df": team_df,
        "checkpoints_df": checkpoints_df,
    }


def build_people_summary(clients_df: pd.DataFrame, documents_df: pd.DataFrame) -> pd.DataFrame:
    docs_by_client = (
        documents_df.groupby("chave_pessoa", dropna=False)
        .agg(
            nome_documentos=("Nome Pessoa", "first"),
            total_documentos=("Status", "size"),
            documentos_recebidos=("Status", lambda values: int((values == "RECEBIDO").sum())),
            documentos_pendentes=("Status", lambda values: int((values != "RECEBIDO").sum())),
            documentos_enviados_lista=(
                "documento_descricao",
                lambda values: list_join(
                    documents_df.loc[values.index][
                        documents_df.loc[values.index, "Status"] == "RECEBIDO"
                    ]["documento_descricao"]
                ),
            ),
            documentos_faltantes_lista=(
                "documento_descricao",
                lambda values: list_join(
                    documents_df.loc[values.index][
                        documents_df.loc[values.index, "Status"] != "RECEBIDO"
                    ]["documento_descricao"]
                ),
            ),
            ultima_atualizacao_docs=("Última Atualização", "max"),
        )
        .reset_index()
    )

    declaration_columns = [
        column
        for column in [
            "client_id",
            "CPF",
            "NOME",
            "Grupo",
            "Reunião",
            "Nivel de Complexidade",
            "Documentação Informada",
            "Status Preenchimento",
            "Responsável pelo Preenchimento",
            "Status Pós-Envio",
            "Telefone",
            "Senha Gov",
            "Tem Certificado Digital",
            "Cadastro de Procuração",
            "chave_pessoa",
            "client_updated_at",
        ]
        if column in clients_df.columns
    ]
    people_df = clients_df[declaration_columns].copy().merge(docs_by_client, on="chave_pessoa", how="outer")
    if "client_id" not in people_df.columns:
        people_df["client_id"] = range(1, len(people_df) + 1)
    else:
        people_df["client_id"] = pd.to_numeric(people_df["client_id"], errors="coerce")
        next_id = int(people_df["client_id"].max()) if people_df["client_id"].notna().any() else 0
        missing_count = int(people_df["client_id"].isna().sum())
        if missing_count:
            people_df.loc[people_df["client_id"].isna(), "client_id"] = range(next_id + 1, next_id + missing_count + 1)
        people_df["client_id"] = people_df["client_id"].astype(int)

    people_df["NOME"] = people_df["NOME"].replace("", pd.NA).fillna(people_df["nome_documentos"])
    people_df["Grupo"] = people_df["Grupo"].fillna("Sem grupo")
    people_df["Reunião"] = people_df["Reunião"].fillna("Sem reunião informada")
    people_df["Nivel de Complexidade"] = people_df["Nivel de Complexidade"].fillna("Não informado")
    if "Documentação Informada" in people_df.columns:
        people_df["Documentação Informada"] = people_df["Documentação Informada"].fillna("")
    people_df["Status Preenchimento"] = people_df["Status Preenchimento"].fillna("SEM STATUS")
    people_df["Responsável pelo Preenchimento"] = people_df["Responsável pelo Preenchimento"].fillna("Não atribuído")

    for column in ["CPF", "Telefone", "Senha Gov", "Cadastro de Procuração"]:
        if column in people_df.columns:
            people_df[column] = people_df[column].fillna("")
    if "Tem Certificado Digital" in people_df.columns:
        people_df["Tem Certificado Digital"] = people_df["Tem Certificado Digital"].fillna(False).astype(bool)

    for column in ["total_documentos", "documentos_recebidos", "documentos_pendentes"]:
        people_df[column] = people_df[column].fillna(0).astype(int)

    people_df["Documentação"] = people_df.apply(
        lambda row: documentation_status(row["total_documentos"], row["documentos_recebidos"])
        if int(row["total_documentos"]) > 0
        else (row.get("Documentação Informada", "") or "Sem documentação"),
        axis=1,
    )
    people_df["% documentação recebida"] = people_df.apply(
        lambda row: safe_percent(row["documentos_recebidos"], row["total_documentos"]),
        axis=1,
    )
    people_df["Recebidos / Total"] = people_df.apply(
        lambda row: f"{int(row['documentos_recebidos'])} de {int(row['total_documentos'])}",
        axis=1,
    )
    people_df["documentos_enviados_lista"] = people_df["documentos_enviados_lista"].fillna("")
    people_df["documentos_faltantes_lista"] = people_df["documentos_faltantes_lista"].fillna("")
    people_df["documentos_faltantes_lista"] = people_df.apply(
        lambda row: "Checklist não cadastrado no banco"
        if int(row["total_documentos"]) == 0 and not row["documentos_faltantes_lista"]
        else row["documentos_faltantes_lista"],
        axis=1,
    )
    people_df["ultima_atualizacao_docs"] = pd.to_datetime(people_df["ultima_atualizacao_docs"], errors="coerce")
    if "client_updated_at" not in people_df.columns:
        people_df["client_updated_at"] = pd.NaT
    people_df["dias_desde_ultima_atualizacao"] = (
        pd.Timestamp(date.today()) - people_df["ultima_atualizacao_docs"]
    ).dt.days
    people_df["Precisa cobrar"] = (
        (people_df["Documentação"] != "Recebido total")
        & (
            people_df["ultima_atualizacao_docs"].isna()
            | (people_df["dias_desde_ultima_atualizacao"] > 7)
        )
    )
    return people_df.sort_values(["Status Preenchimento", "Grupo", "NOME"])


def attach_private_data(people_df: pd.DataFrame, private_df: pd.DataFrame) -> pd.DataFrame:
    if people_df.empty or private_df.empty or "client_id" not in people_df.columns:
        return people_df
    merged = people_df.merge(private_df, on="client_id", how="left", suffixes=("", "_private"))
    for column in ["CPF", "Telefone", "Senha Gov", "Cadastro de Procuração"]:
        if f"{column}_private" in merged.columns:
            merged[column] = merged[f"{column}_private"].replace("", pd.NA).fillna(merged[column])
            merged = merged.drop(columns=[f"{column}_private"])
    if "Tem Certificado Digital_private" in merged.columns:
        merged["Tem Certificado Digital"] = (
            merged["Tem Certificado Digital_private"].fillna(False).astype(bool)
            | merged["Tem Certificado Digital"].fillna(False).astype(bool)
        )
        merged = merged.drop(columns=["Tem Certificado Digital_private"])
    return merged


def build_checkpoint_summary(checkpoints_df: pd.DataFrame, documents_df: pd.DataFrame | None = None) -> pd.DataFrame:
    if checkpoints_df.empty:
        return pd.DataFrame(columns=["client_id", "completed_steps", "progress_percent", "last_step_update"])
    summary_df = (
        checkpoints_df.groupby("client_id")
        .agg(
            completed_steps=("completed", lambda values: int(pd.Series(values).astype(bool).sum())),
            last_step_update=("updated_at", "max"),
        )
        .reset_index()
    )
    if documents_df is not None and not documents_df.empty and "client_id" in documents_df.columns:
        doc_totals = (
            documents_df.groupby("client_id")
            .size()
            .reset_index(name="document_steps")
        )
        summary_df = summary_df.merge(doc_totals, on="client_id", how="left")
        summary_df["document_steps"] = summary_df["document_steps"].fillna(0).astype(int)
    else:
        summary_df["document_steps"] = 0
    summary_df["total_steps"] = summary_df["document_steps"] + len(PREPARATION_STEPS)
    summary_df["progress_percent"] = summary_df.apply(
        lambda row: safe_percent(int(row["completed_steps"]), int(row["total_steps"])),
        axis=1,
    )
    return summary_df


def attach_progress(people_df: pd.DataFrame, checkpoint_summary_df: pd.DataFrame) -> pd.DataFrame:
    if people_df.empty:
        return people_df
    merged = people_df.copy()
    if "client_id" in merged.columns and not checkpoint_summary_df.empty:
        merged = merged.merge(checkpoint_summary_df, on="client_id", how="left")
    if "completed_steps" not in merged.columns:
        merged["completed_steps"] = 0
    merged["completed_steps"] = merged["completed_steps"].fillna(0).astype(int)
    if "progress_percent" not in merged.columns:
        merged["progress_percent"] = 0.0
    merged["progress_percent"] = merged["progress_percent"].fillna(0.0)
    if "total_documentos" in merged.columns:
        fallback_total_steps = merged["total_documentos"].fillna(0).astype(int) + len(PREPARATION_STEPS)
    else:
        fallback_total_steps = pd.Series([len(PREPARATION_STEPS)] * len(merged), index=merged.index)
    if "total_steps" not in merged.columns:
        merged["total_steps"] = fallback_total_steps
    merged["total_steps"] = merged["total_steps"].fillna(fallback_total_steps).astype(int)
    if "last_step_update" not in merged.columns:
        merged["last_step_update"] = pd.NaT
    if "client_updated_at" not in merged.columns:
        merged["client_updated_at"] = pd.NaT
    merged["client_updated_at"] = pd.to_datetime(merged["client_updated_at"], errors="coerce")
    merged["last_step_update"] = pd.to_datetime(merged["last_step_update"], errors="coerce")
    merged["last_activity_at"] = merged.apply(
        lambda row: max(
            [value for value in [row["client_updated_at"], row["last_step_update"]] if pd.notna(value)],
            default=pd.NaT,
        ),
        axis=1,
    )
    merged["Progresso Geral"] = merged.apply(
        lambda row: f"{int(row['completed_steps'])}/{int(row['total_steps'])} ({row['progress_percent']:.1f}%)",
        axis=1,
    )
    return merged


def build_checkpoint_editor_state(checkpoints_df: pd.DataFrame, client_id: int) -> list[dict]:
    step_map = {}
    if not checkpoints_df.empty:
        filtered = checkpoints_df[checkpoints_df["client_id"] == client_id]
        step_map = {row["step_key"]: row for _, row in filtered.iterrows()}

    editor_state = []
    for step_key, step_label in PREPARATION_STEPS:
        row = step_map.get(step_key)
        editor_state.append(
            {
                "step_key": step_key,
                "step_label": step_label,
                "completed": bool(row["completed"]) if row is not None else False,
                "note": normalize_text(row["note"]) if row is not None else "",
            }
        )
    return editor_state


def build_document_sections(documents_df: pd.DataFrame, checkpoints_df: pd.DataFrame, client_id: int) -> list[dict]:
    if documents_df.empty or "client_id" not in documents_df.columns:
        return []

    step_map = {}
    if not checkpoints_df.empty:
        filtered = checkpoints_df[checkpoints_df["client_id"] == client_id]
        step_map = {row["step_key"]: row for _, row in filtered.iterrows()}

    client_docs_df = documents_df[documents_df["client_id"] == client_id].copy()
    if client_docs_df.empty:
        return []

    client_docs_df["categoria_documento"] = client_docs_df.apply(
        lambda row: document_category(row["Tipo Documento"], row["Instituição"]),
        axis=1,
    )

    sections: list[dict] = []
    for section_key, section_label in SECTION_LABELS.items():
        section_docs = client_docs_df[client_docs_df["categoria_documento"] == section_key].copy()
        if section_docs.empty:
            continue
        items = []
        for _, row in section_docs.sort_values(["Tipo Documento", "Instituição"]).iterrows():
            step_key = f"doc_{int(row['document_id'])}"
            stored = step_map.get(step_key)
            items.append(
                {
                    "step_key": step_key,
                    "step_label": normalize_text(row["documento_descricao"]),
                    "completed": bool(stored["completed"]) if stored is not None else False,
                    "note": normalize_text(stored["note"]) if stored is not None else "",
                    "document_status": normalize_text(row["Status"]) or "SEM STATUS",
                }
            )
        sections.append({"section_key": section_key, "section_label": section_label, "items": items})
    return sections


def load_source(uploaded_file, fallback_path: Path) -> tuple[bytes | None, str]:
    if uploaded_file is not None:
        return uploaded_file.getvalue(), uploaded_file.name
    if fallback_path.exists():
        return fallback_path.read_bytes(), fallback_path.name
    return None, "Arquivo não carregado"


def save_snapshot(snapshot_df: pd.DataFrame) -> None:
    DATA_DIR.mkdir(exist_ok=True)
    if SNAPSHOT_PATH.exists():
        history_df = pd.read_csv(SNAPSHOT_PATH, parse_dates=["data_referencia"])
        history_df = history_df[
            history_df["data_referencia"].dt.date
            != snapshot_df.loc[0, "data_referencia"].date()
        ]
        snapshot_df = pd.concat([history_df, snapshot_df], ignore_index=True)
    snapshot_df.sort_values("data_referencia").to_csv(SNAPSHOT_PATH, index=False)


def load_history() -> pd.DataFrame:
    if not SNAPSHOT_PATH.exists():
        return pd.DataFrame()
    return pd.read_csv(SNAPSHOT_PATH, parse_dates=["data_referencia"]).sort_values("data_referencia")


def save_snapshot_remote(client: Client, snapshot_df: pd.DataFrame) -> None:
    row = snapshot_df.iloc[0]
    payload = {
        "reference_date": row["data_referencia"].date().isoformat(),
        "declaracoes": int(row["declaracoes"]),
        "transmitidas": int(row["transmitidas"]),
        "em_revisao": int(row["em_revisao"]),
        "clientes_com_alguma_documentacao": int(row["clientes_com_alguma_documentacao"]),
        "clientes_docs_completos": int(row["clientes_docs_completos"]),
        "clientes_docs_parciais": int(row["clientes_docs_parciais"]),
        "clientes_sem_documentacao": int(row["clientes_sem_documentacao"]),
        "pct_transmitidas": float(row["pct_transmitidas"]),
        "pct_docs_completos": float(row["pct_docs_completos"]),
    }
    client.table("daily_snapshots").upsert(payload, on_conflict="reference_date").execute()
    invalidate_data_cache()


def load_history_remote(client: Client) -> pd.DataFrame:
    rows = fetch_all_rows(
        client,
        "daily_snapshots",
        "reference_date, declaracoes, transmitidas, em_revisao, clientes_com_alguma_documentacao, clientes_docs_completos, clientes_docs_parciais, clientes_sem_documentacao, pct_transmitidas, pct_docs_completos",
    )
    if not rows:
        return pd.DataFrame()
    history_df = pd.DataFrame(rows)
    history_df["data_referencia"] = pd.to_datetime(history_df["reference_date"], errors="coerce")
    history_df = history_df.drop(columns=["reference_date"])
    return history_df.sort_values("data_referencia")


def ensure_daily_snapshot(snapshot_df: pd.DataFrame, supabase_client: Client | None) -> bool:
    current_time = datetime.now()
    if current_time.hour < 17:
        return False
    snapshot_date = snapshot_df.loc[0, "data_referencia"].date()
    history_df = load_history_remote(supabase_client) if supabase_client is not None else load_history()
    already_saved = (
        not history_df.empty
        and (history_df["data_referencia"].dt.date == snapshot_date).any()
    )
    if already_saved:
        return False
    if supabase_client is not None:
        save_snapshot_remote(supabase_client, snapshot_df)
    else:
        save_snapshot(snapshot_df)
    return True


def build_snapshot(snapshot_date: date, clients_df: pd.DataFrame, people_df: pd.DataFrame) -> pd.DataFrame:
    total_declarations = len(clients_df)
    transmitted = int((clients_df["Status Preenchimento"] == "TRANSMITIDO").sum())
    reviewing = int(clients_df["Status Preenchimento"].str.contains("REVISÃO", na=False).sum())
    docs_any = int((people_df["Documentação"] != "Sem documentação").sum())
    docs_complete = int((people_df["Documentação"] == "Recebido total").sum())
    docs_partial = int((people_df["Documentação"] == "Recebido parcial").sum())
    docs_missing = int((people_df["Documentação"] == "Sem documentação").sum())
    return pd.DataFrame(
        [
            {
                "data_referencia": pd.to_datetime(snapshot_date),
                "declaracoes": total_declarations,
                "transmitidas": transmitted,
                "em_revisao": reviewing,
                "clientes_com_alguma_documentacao": docs_any,
                "clientes_docs_completos": docs_complete,
                "clientes_docs_parciais": docs_partial,
                "clientes_sem_documentacao": docs_missing,
                "pct_transmitidas": safe_percent(transmitted, total_declarations),
                "pct_docs_completos": safe_percent(docs_complete, len(people_df)),
            }
        ]
    )


def display_metric(label: str, value: int, percent: float | None = None) -> None:
    delta = None if percent is None else f"{percent:.1f}%"
    st.metric(label, f"{value}", delta=delta)


def render_login_page() -> None:
    config = load_supabase_public_config()
    if not config:
        st.error("Credenciais do Supabase não encontradas.")
        st.stop()

    left, center, right = st.columns([1, 1.1, 1])
    with center:
        if LOGO_PATH.exists():
            st.image(str(LOGO_PATH), use_container_width=True)
        st.markdown("### IRPF - Controle de Declarações")
        with st.form("supabase_login"):
            email = st.text_input("Email")
            password = st.text_input("Senha", type="password")
            submitted = st.form_submit_button("Entrar", use_container_width=True)
        if submitted:
            try:
                client = create_client(config["url"], config["anon_key"])
                auth_response = client.auth.sign_in_with_password({"email": email, "password": password})
                session = getattr(auth_response, "session", None)
                user = getattr(auth_response, "user", None)
                if session is None:
                    st.error("Não foi possível abrir a sessão.")
                else:
                    st.session_state["supabase_access_token"] = session.access_token
                    st.session_state["supabase_refresh_token"] = session.refresh_token
                    st.session_state["supabase_user_email"] = getattr(user, "email", email)
                    st.rerun()
            except Exception as exc:
                st.error(f"Falha no login: {exc}")


def render_app_header(user_profile: dict[str, object]) -> None:
    logo_col, title_col, action_col = st.columns([1, 5, 1.2], vertical_alignment="center")
    with logo_col:
        if LOGO_PATH.exists():
            st.image(str(LOGO_PATH), use_container_width=True)
    with title_col:
        st.title("IRPF - Controle de Declarações")
    with action_col:
        st.caption(user_profile.get("display_name", "Usuário"))
        if st.button("Sair", use_container_width=True):
            clear_supabase_session()
            st.rerun()


def render_sector_selector(user_profile: dict[str, object]) -> str:
    allowed_sectors = user_profile.get("allowed_sectors", []) or ["Comercial", "Preenchimento", "Revisão"]
    current_value = st.session_state.get("selected_sector", allowed_sectors[0])
    if current_value not in allowed_sectors:
        current_value = allowed_sectors[0]
    selected_sector = st.radio(
        "Setor",
        options=allowed_sectors,
        index=allowed_sectors.index(current_value),
        horizontal=True,
        label_visibility="collapsed",
    )
    st.session_state["selected_sector"] = selected_sector
    return selected_sector


def save_client_record(client: Client, client_payload: dict[str, object], private_payload: dict[str, object], client_id: int | None = None) -> int:
    timestamp = datetime.utcnow().replace(microsecond=0).isoformat()
    client_payload = {**client_payload, "updated_at": timestamp}
    if client_id is None:
        response = client.table("clients").insert(client_payload).execute()
        saved_row = (response.data or [None])[0]
        if not saved_row:
            raise ValueError("Não foi possível criar o cliente.")
        client_id = int(saved_row["id"])
    else:
        client.table("clients").update(client_payload).eq("id", client_id).execute()
    client.table("client_private").upsert(
        {
            "client_id": client_id,
            "cpf": normalize_text(private_payload.get("cpf", "")),
            "phone": normalize_text(private_payload.get("phone", "")),
            "gov_password": normalize_text(private_payload.get("gov_password", "")),
            "has_digital_certificate": bool(private_payload.get("has_digital_certificate", False)),
            "power_of_attorney": normalize_text(private_payload.get("power_of_attorney", "")),
            "updated_at": timestamp,
        },
        on_conflict="client_id",
    ).execute()
    invalidate_data_cache()
    return client_id


def refresh_client_documentation_status(client: Client, client_id: int) -> None:
    docs_response = client.table("documents").select("status").eq("client_id", client_id).execute()
    statuses = [normalize_text(row.get("status", "")).upper() for row in (docs_response.data or [])]
    total = len(statuses)
    received = sum(status == "RECEBIDO" for status in statuses)
    client.table("clients").update(
        {"documentation_status": documentation_status(total, received), "updated_at": datetime.utcnow().replace(microsecond=0).isoformat()}
    ).eq("id", client_id).execute()


def save_document_record(
    client: Client,
    client_id: int,
    document_type: str,
    institution: str,
    status: str,
    last_update: date | None,
    control_key: str,
) -> None:
    client.table("documents").insert(
        {
            "client_id": client_id,
            "document_type": normalize_text(document_type) or "Não informado",
            "institution": normalize_text(institution) or "Não informada",
            "status": normalize_text(status).upper() or "SEM STATUS",
            "last_update": last_update.isoformat() if last_update else None,
            "control_key": normalize_text(control_key),
        }
    ).execute()
    refresh_client_documentation_status(client, client_id)
    invalidate_data_cache()


def update_document_record(
    client: Client,
    document_id: int,
    client_id: int,
    document_type: str,
    institution: str,
    status: str,
    last_update: date | None,
    control_key: str,
) -> None:
    client.table("documents").update(
        {
            "document_type": normalize_text(document_type) or "Não informado",
            "institution": normalize_text(institution) or "Não informada",
            "status": normalize_text(status).upper() or "SEM STATUS",
            "last_update": last_update.isoformat() if last_update else None,
            "control_key": normalize_text(control_key),
            "updated_at": datetime.utcnow().replace(microsecond=0).isoformat(),
        }
    ).eq("id", document_id).execute()
    refresh_client_documentation_status(client, client_id)
    invalidate_data_cache()


def save_document_bulk_updates(client: Client, document_rows: list[dict], client_id: int) -> None:
    timestamp = datetime.utcnow().replace(microsecond=0).isoformat()
    for row in document_rows:
        client.table("documents").update(
            {
                "status": normalize_text(row.get("Status", "")).upper() or "SEM STATUS",
                "last_update": row.get("last_update"),
                "updated_at": timestamp,
            }
        ).eq("id", int(row["document_id"])).execute()
    refresh_client_documentation_status(client, client_id)
    invalidate_data_cache()


def delete_document_record(client: Client, document_id: int, client_id: int) -> None:
    client.table("documents").delete().eq("id", document_id).execute()
    refresh_client_documentation_status(client, client_id)
    invalidate_data_cache()


def delete_client_record(client: Client, client_id: int) -> None:
    client.table("clients").delete().eq("id", client_id).execute()
    invalidate_data_cache()


def save_batch_client_updates(client: Client, rows: list[dict], acting_as: str) -> None:
    timestamp = datetime.utcnow().replace(microsecond=0).isoformat()
    for row in rows:
        client.table("clients").update(
            {
                "assigned_preparer": normalize_text(row.get("Responsável pelo Preenchimento", "")),
                "tax_status": normalize_text(row.get("Status Preenchimento", "")),
                "updated_at": timestamp,
            }
        ).eq("id", int(row["client_id"])).execute()
    invalidate_data_cache()


def build_standard_template() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "NOME": "CLIENTE EXEMPLO",
                "CPF": "000.000.000-00",
                "Grupo": "Grupo exemplo",
                "Reunião": "Pendente",
                "Nivel de Complexidade": "Médio",
                "Status Preenchimento": "PENDENTE",
                "Responsável pelo Preenchimento": "Não atribuído",
                "Status Pós-Envio": "Não informado",
                "Telefone": "(00) 9 0000-0000",
                "Senha Gov": "",
                "Cadastro de Procuração": "Não informado",
                "Tipo Documento": "Informe de rendimentos",
                "Instituição": "Banco/empresa exemplo",
                "Status Documento": "PENDENTE",
                "Última Atualização": date.today().strftime("%d/%m/%Y"),
                "chave_controle": "",
            }
        ],
        columns=STANDARD_IMPORT_COLUMNS,
    )


def parse_standard_import(file_bytes: bytes, file_name: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    raw_df = read_table_file(file_bytes, file_name)
    selected_df = select_columns(raw_df, STANDARD_IMPORT_COLUMNS)
    client_source = selected_df.rename(columns={"Status Documento": "Status"})
    clients_df = parse_clients(client_source[CLIENT_COLUMNS].to_csv(index=False).encode("utf-8"), "clientes.csv")

    docs_source = pd.DataFrame(
        {
            "Nome Pessoa": selected_df["NOME"],
            "Tipo Documento": selected_df["Tipo Documento"],
            "Instituição": selected_df["Instituição"],
            "Status": selected_df["Status Documento"],
            "Última Atualização": selected_df["Última Atualização"],
            "chave_controle": selected_df["chave_controle"],
        }
    )
    docs_source = docs_source[docs_source["Tipo Documento"].map(normalize_text) != ""].copy()
    documents_df = parse_documents(docs_source.to_csv(index=False).encode("utf-8"), "documentos.csv")
    return clients_df.drop_duplicates("chave_pessoa"), documents_df


def document_status_priority(value: object) -> int:
    status = normalize_key(value)
    if status == "RECEBIDO":
        return 2
    if status == "PENDENTE":
        return 1
    return 0


def deduplicate_imported_documents(documents_df: pd.DataFrame) -> pd.DataFrame:
    if documents_df.empty:
        return documents_df
    deduped_df = documents_df.copy()
    deduped_df["_doc_key"] = deduped_df.apply(
        lambda row: (
            normalize_text(row.get("chave_pessoa", "")),
            normalize_key(row.get("Tipo Documento", "")),
            normalize_key(row.get("Instituição", "")),
        ),
        axis=1,
    )
    deduped_df["_doc_key_sort"] = deduped_df["_doc_key"].map(lambda key: "|".join(key))
    deduped_df["_status_priority"] = deduped_df["Status"].map(document_status_priority)
    deduped_df["_last_update_sort"] = pd.to_datetime(
        deduped_df["Última Atualização"],
        errors="coerce",
        dayfirst=True,
    ).fillna(pd.Timestamp("1900-01-01"))
    deduped_df = deduped_df.sort_values(
        ["_doc_key_sort", "_status_priority", "_last_update_sort"],
        kind="stable",
    )
    deduped_df = deduped_df.drop_duplicates("_doc_key", keep="last")
    return deduped_df.drop(
        columns=["_doc_key", "_doc_key_sort", "_status_priority", "_last_update_sort"]
    ).reset_index(drop=True)


def build_import_comparison(
    imported_clients_df: pd.DataFrame,
    imported_documents_df: pd.DataFrame,
    people_df: pd.DataFrame,
    documents_df: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    imported_documents_df = deduplicate_imported_documents(imported_documents_df)
    current_people = people_df.set_index("chave_pessoa", drop=False) if "chave_pessoa" in people_df.columns else pd.DataFrame()
    current_docs = documents_df.copy()
    current_docs_by_key = {}
    if not current_docs.empty:
        current_docs["doc_key"] = current_docs.apply(
            lambda row: (
                normalize_text(row["chave_pessoa"]),
                normalize_key(row["Tipo Documento"]),
                normalize_key(row["Instituição"]),
            ),
            axis=1,
        )
        current_docs_by_key = {row["doc_key"]: row for _, row in current_docs.iterrows()}

    new_clients = []
    changed_clients = []
    compare_columns = [
        ("Grupo", "Grupo"),
        ("Reunião", "Reunião"),
        ("Nivel de Complexidade", "Nivel de Complexidade"),
        ("Status Preenchimento", "Status Preenchimento"),
        ("Responsável pelo Preenchimento", "Responsável pelo Preenchimento"),
        ("Status Pós-Envio", "Status Pós-Envio"),
        ("CPF", "CPF"),
        ("Telefone", "Telefone"),
        ("Senha Gov", "Senha Gov"),
        ("Cadastro de Procuração", "Cadastro de Procuração"),
    ]
    for _, row in imported_clients_df.iterrows():
        key = row["chave_pessoa"]
        if current_people.empty or key not in current_people.index:
            new_clients.append({"NOME": row["NOME"], "Grupo": row["Grupo"], "Status": "Novo cliente"})
            continue
        current_row = current_people.loc[key]
        differences = []
        for source_column, current_column in compare_columns:
            new_value = normalize_text(row.get(source_column, ""))
            old_value = normalize_text(current_row.get(current_column, ""))
            if new_value and new_value != old_value:
                differences.append(f"{current_column}: {old_value or '-'} -> {new_value}")
        if differences:
            changed_clients.append({"NOME": row["NOME"], "Alterações": "\n".join(differences)})

    new_documents = []
    changed_documents = []
    for _, row in imported_documents_df.iterrows():
        doc_key = (normalize_text(row["chave_pessoa"]), normalize_key(row["Tipo Documento"]), normalize_key(row["Instituição"]))
        if not current_docs_by_key or doc_key not in current_docs_by_key:
            new_documents.append(
                {
                    "NOME": row["Nome Pessoa"],
                    "Documento": row["documento_descricao"],
                    "Status": row["Status"],
                }
            )
            continue
        current_doc = current_docs_by_key[doc_key]
        new_status = normalize_text(row["Status"]).upper()
        old_status = normalize_text(current_doc.get("Status", "")).upper()
        if new_status and new_status != old_status:
            changed_documents.append(
                {
                    "NOME": row["Nome Pessoa"],
                    "Documento": row["documento_descricao"],
                    "Alteração": f"{old_status or '-'} -> {new_status}",
                }
            )

    return {
        "new_clients": pd.DataFrame(new_clients),
        "changed_clients": pd.DataFrame(changed_clients),
        "new_documents": pd.DataFrame(new_documents),
        "changed_documents": pd.DataFrame(changed_documents),
    }


def import_value_or_existing(new_value: object, existing_value: object = "", placeholders: set[str] | None = None) -> str:
    text = normalize_text(new_value)
    placeholder_keys = placeholders or set()
    if not text or normalize_key(text) in placeholder_keys:
        return normalize_text(existing_value)
    return text


def parse_optional_date(value: object) -> date | None:
    parsed = pd.to_datetime(value, errors="coerce", dayfirst=True)
    return None if pd.isna(parsed) else parsed.date()


def apply_import_updates(
    client: Client,
    imported_clients_df: pd.DataFrame,
    imported_documents_df: pd.DataFrame,
    people_df: pd.DataFrame,
    documents_df: pd.DataFrame,
) -> tuple[int, int]:
    imported_documents_df = deduplicate_imported_documents(imported_documents_df)
    current_people = people_df.set_index("chave_pessoa", drop=False) if "chave_pessoa" in people_df.columns else pd.DataFrame()
    client_ids: dict[str, int] = {}
    updated_clients = 0

    for _, row in imported_clients_df.iterrows():
        key = row["chave_pessoa"]
        existing_id = None
        current_row = pd.Series(dtype=object)
        if not current_people.empty and key in current_people.index:
            current_row = current_people.loc[key]
            existing_id = int(current_row["client_id"])
        generic_placeholders = {
            "SEM GRUPO",
            "SEM REUNIAO INFORMADA",
            "NAO INFORMADO",
            "NAO ATRIBUIDO",
            "SEM STATUS",
        }
        imported_gov_password = normalize_text(row["Senha Gov"])
        imported_has_certificate = bool(row.get("Tem Certificado Digital", False))
        existing_has_certificate = bool(current_row.get("Tem Certificado Digital", False)) if existing_id else False
        has_digital_certificate = (
            imported_has_certificate
            if imported_has_certificate or imported_gov_password
            else existing_has_certificate
        )
        saved_id = save_client_record(
            client,
            {
                "normalized_name": key,
                "full_name": normalize_text(row["NOME"]),
                "group_name": import_value_or_existing(row["Grupo"], current_row.get("Grupo", ""), generic_placeholders),
                "meeting_status": import_value_or_existing(row["Reunião"], current_row.get("Reunião", ""), generic_placeholders),
                "complexity_level": import_value_or_existing(
                    row["Nivel de Complexidade"],
                    current_row.get("Nivel de Complexidade", ""),
                    generic_placeholders,
                ),
                "tax_status": import_value_or_existing(
                    row["Status Preenchimento"],
                    current_row.get("Status Preenchimento", ""),
                    {"SEM STATUS"},
                ),
                "assigned_preparer": import_value_or_existing(
                    row["Responsável pelo Preenchimento"],
                    current_row.get("Responsável pelo Preenchimento", ""),
                    {"NAO ATRIBUIDO"},
                ),
                "post_filing_status": import_value_or_existing(
                    row["Status Pós-Envio"],
                    current_row.get("Status Pós-Envio", ""),
                    generic_placeholders,
                ),
                "documentation_status": "",
                "active": True,
            },
            {
                "cpf": import_value_or_existing(normalize_cpf(row["CPF"]), current_row.get("CPF", "")),
                "phone": import_value_or_existing(normalize_phone(row["Telefone"]), current_row.get("Telefone", "")),
                "gov_password": import_value_or_existing(imported_gov_password, current_row.get("Senha Gov", "")),
                "has_digital_certificate": has_digital_certificate,
                "power_of_attorney": import_value_or_existing(
                    row["Cadastro de Procuração"],
                    current_row.get("Cadastro de Procuração", ""),
                    {"NAO INFORMADO"},
                ),
            },
            client_id=existing_id,
        )
        client_ids[key] = saved_id
        updated_clients += 1

    current_docs = documents_df.copy()
    current_docs_by_key = {}
    if not current_docs.empty:
        current_docs["doc_key"] = current_docs.apply(
            lambda row: (
                int(row["client_id"]),
                normalize_key(row["Tipo Documento"]),
                normalize_key(row["Instituição"]),
            ),
            axis=1,
        )
        current_docs_by_key = {row["doc_key"]: row for _, row in current_docs.iterrows()}

    updated_documents = 0
    processed_doc_keys: set[tuple[int, str, str]] = set()
    for _, row in imported_documents_df.iterrows():
        key = row["chave_pessoa"]
        if key not in client_ids:
            existing_id = None
            if not current_people.empty and key in current_people.index:
                existing_id = int(current_people.loc[key]["client_id"])
            else:
                existing_id = save_client_record(
                    client,
                    {
                        "normalized_name": key,
                        "full_name": normalize_text(row["Nome Pessoa"]),
                        "group_name": "",
                        "meeting_status": "",
                        "complexity_level": "",
                        "tax_status": "PENDENTE",
                        "assigned_preparer": "Não atribuído",
                        "post_filing_status": "",
                        "documentation_status": "",
                        "active": True,
                    },
                    {"cpf": "", "phone": "", "gov_password": "", "has_digital_certificate": False, "power_of_attorney": ""},
                    client_id=None,
                )
            client_ids[key] = existing_id
        client_id = client_ids[key]
        doc_key = (client_id, normalize_key(row["Tipo Documento"]), normalize_key(row["Instituição"]))
        if doc_key in processed_doc_keys:
            continue
        processed_doc_keys.add(doc_key)
        last_update_value = parse_optional_date(row["Última Atualização"])
        if current_docs_by_key and doc_key in current_docs_by_key:
            current_doc = current_docs_by_key[doc_key]
            update_document_record(
                client,
                document_id=int(current_doc["document_id"]),
                client_id=client_id,
                document_type=row["Tipo Documento"],
                institution=row["Instituição"],
                status=import_value_or_existing(row["Status"], current_doc.get("Status", ""), {"SEM STATUS"}),
                last_update=last_update_value
                if last_update_value is not None
                else parse_optional_date(current_doc.get("Última Atualização")),
                control_key=import_value_or_existing(row["chave_controle"], current_doc.get("chave_controle", "")),
            )
        else:
            save_document_record(
                client,
                client_id=client_id,
                document_type=row["Tipo Documento"],
                institution=row["Instituição"],
                status=row["Status"],
                last_update=last_update_value,
                control_key=row["chave_controle"],
            )
        updated_documents += 1

    return updated_clients, updated_documents


def render_commercial_page(
    people_df: pd.DataFrame,
    supabase_client: Client | None,
    documents_df: pd.DataFrame,
    team_df: pd.DataFrame,
    user_profile: dict[str, object],
) -> None:
    st.header("Comercial")
    report_tab, operation_tab = st.tabs(["Relatório de documentação", "Atendimento e documentos"])
    with report_tab:
        metric_1, metric_2, metric_3, metric_4 = st.columns(4)
        with metric_1:
            st.metric("Clientes", len(people_df))
        with metric_2:
            st.metric("Docs completos", int((people_df["Documentação"] == "Recebido total").sum()))
        with metric_3:
            st.metric("Docs parciais", int((people_df["Documentação"] == "Recebido parcial").sum()))
        with metric_4:
            st.metric("Sem documentação", int((people_df["Documentação"] == "Sem documentação").sum()))

        report_df = people_df[
            [
                "NOME",
                "Grupo",
                "Documentação",
                "Recebidos / Total",
                "% documentação recebida",
                "documentos_enviados_lista",
                "documentos_faltantes_lista",
                "Responsável pelo Preenchimento",
                "Status Preenchimento",
            ]
        ].copy()
        report_df["% Recebido"] = report_df["% documentação recebida"].map(lambda value: f"{value:.1f}%")
        report_df = report_df.rename(
            columns={
                "documentos_enviados_lista": "Documentos recebidos",
                "documentos_faltantes_lista": "Documentos faltantes",
            }
        )[
            [
                "NOME",
                "Grupo",
                "Documentação",
                "Recebidos / Total",
                "% Recebido",
                "Documentos recebidos",
                "Documentos faltantes",
                "Responsável pelo Preenchimento",
                "Status Preenchimento",
            ]
        ]
        doc_filter = st.multiselect(
            "Filtrar por documentação",
            options=sorted(report_df["Documentação"].unique()),
            default=sorted(report_df["Documentação"].unique()),
            key="commercial_doc_filter",
        )
        filtered_df = report_df[report_df["Documentação"].isin(doc_filter)].copy()
        st.dataframe(filtered_df, use_container_width=True, hide_index=True)

        export_df = filtered_df.copy()
        export_df["Documentos recebidos"] = export_df["Documentos recebidos"].map(
            lambda value: normalize_text(str(value).replace("\n", " | "))
        )
        export_df["Documentos faltantes"] = export_df["Documentos faltantes"].map(
            lambda value: normalize_text(str(value).replace("\n", " | "))
        )
        st.download_button(
            "Exportar relatório comercial",
            data=export_df.to_csv(index=False, sep=";").encode("utf-8-sig"),
            file_name="relatorio_comercial_documentacao.csv",
            mime="text/csv",
        )

    with operation_tab:
        render_registry_page(
            supabase_client,
            people_df,
            documents_df,
            team_df,
            user_profile,
            show_header=False,
        )

 

def render_registry_page(
    supabase_client: Client | None,
    people_df: pd.DataFrame,
    documents_df: pd.DataFrame,
    team_df: pd.DataFrame,
    user_profile: dict[str, object],
    show_header: bool = True,
) -> None:
    return render_registry_page_clean(
        supabase_client,
        people_df,
        documents_df,
        team_df,
        user_profile,
        show_header,
    )
    if show_header:
        st.header("Cadastros")
    if supabase_client is None:
        st.info("Use o modo Supabase com login para consultar e manter os cadastros.")
        return
    if not user_profile.get("can_manage_records", False):
        st.warning("Seu acesso permite consulta operacional, mas não manutenção de cadastros.")
        return

    search_col_1, search_col_2, search_col_3 = st.columns(3)
    with search_col_1:
        search_name = st.text_input("Buscar por nome")
    with search_col_2:
        group_filter = st.multiselect(
            "Filtrar por grupo",
            options=sorted(people_df["Grupo"].dropna().unique()),
            default=[],
        )
    with search_col_3:
        documentation_filter = st.multiselect(
            "Filtrar por documentação",
            options=sorted(people_df["Documentação"].dropna().unique()),
            default=[],
        )

    registry_df = people_df.copy()
    if search_name:
        registry_df = registry_df[
            registry_df["NOME"].map(lambda value: search_name.upper() in normalize_text(value).upper())
        ].copy()
    if group_filter:
        registry_df = registry_df[registry_df["Grupo"].isin(group_filter)].copy()
    if documentation_filter:
        registry_df = registry_df[registry_df["Documentação"].isin(documentation_filter)].copy()

    st.dataframe(
        registry_df[
            [
                "NOME",
                "Grupo",
                "Status Preenchimento",
                "Documentação",
                "Recebidos / Total",
                "Responsável pelo Preenchimento",
                "Nivel de Complexidade",
            ]
        ].sort_values(["Grupo", "NOME"]),
        use_container_width=True,
        hide_index=True,
    )

    selected_name = st.selectbox(
        "Cliente para manutenção",
        options=["Novo cliente"] + sorted(registry_df["NOME"].dropna().unique().tolist()),
    )
    selected_row = None if selected_name == "Novo cliente" else registry_df[registry_df["NOME"] == selected_name].iloc[0]
    client_id = int(selected_row["client_id"]) if selected_row is not None else None
    assigned_options = ["Não atribuído"] + sorted(
        set(team_df["name"].dropna().tolist() + ["Wanessa", "Paulo", "Valdivone", "Michelle", "Erlane", "Duda", "Malu", "Heverton", "Renato"])
    )

    with st.form("client_maintenance_form"):
        col_1, col_2 = st.columns(2)
        with col_1:
            full_name = st.text_input("Nome completo", value=selected_row["NOME"] if selected_row is not None else "")
            group_name = st.text_input("Grupo", value=selected_row["Grupo"] if selected_row is not None else "")
            complexity = st.text_input(
                "Nível de complexidade",
                value=selected_row["Nivel de Complexidade"] if selected_row is not None else "",
            )
            meeting_status = st.text_input("Reunião", value=selected_row["Reunião"] if selected_row is not None else "")
            assigned_preparer = st.selectbox(
                "Responsável pelo preenchimento",
                options=assigned_options,
                index=assigned_options.index(selected_row["Responsável pelo Preenchimento"])
                if selected_row is not None and selected_row["Responsável pelo Preenchimento"] in assigned_options
                else 0,
            )
        with col_2:
            tax_status = st.selectbox(
                "Status da declaração",
                options=STATUS_OPTIONS,
                index=STATUS_OPTIONS.index(selected_row["Status Preenchimento"])
                if selected_row is not None and selected_row["Status Preenchimento"] in STATUS_OPTIONS
                else 0,
            )
            post_filing_status = st.text_input(
                "Status pós-envio",
                value=selected_row["Status Pós-Envio"] if selected_row is not None else "",
            )
            cpf = st.text_input("CPF", value=selected_row.get("CPF", "") if selected_row is not None else "")
            phone = st.text_input("Telefone", value=selected_row.get("Telefone", "") if selected_row is not None else "")
            gov_password = st.text_input(
                "Senha Gov",
                value=selected_row.get("Senha Gov", "") if selected_row is not None else "",
            )
            has_digital_certificate = st.checkbox(
                "Tem certificado digital",
                value=bool(selected_row.get("Tem Certificado Digital", False)) if selected_row is not None else False,
            )
            power_of_attorney = st.text_input(
                "Cadastro de procuração",
                value=selected_row.get("Cadastro de Procuração", "") if selected_row is not None else "",
            )

        saved_client = st.form_submit_button("Salvar cliente", use_container_width=True)

    if saved_client:
        try:
            normalized_name = normalize_key(full_name)
            saved_id = save_client_record(
                supabase_client,
                {
                    "normalized_name": normalized_name,
                    "full_name": normalize_text(full_name),
                    "group_name": normalize_text(group_name),
                    "meeting_status": normalize_text(meeting_status),
                    "complexity_level": normalize_text(complexity),
                    "tax_status": normalize_text(tax_status),
                    "assigned_preparer": normalize_text(assigned_preparer),
                    "post_filing_status": normalize_text(post_filing_status),
                    "documentation_status": selected_row["Documentação"] if selected_row is not None else "Sem documentação",
                    "active": True,
                },
                {
                    "cpf": normalize_cpf(cpf),
                    "phone": normalize_phone(phone),
                    "gov_password": gov_password,
                    "has_digital_certificate": has_digital_certificate,
                    "power_of_attorney": power_of_attorney,
                },
                client_id=client_id,
            )
            st.success(f"Cliente salvo com sucesso. ID {saved_id}.")
            st.rerun()
        except Exception as exc:
            st.error(f"Não foi possível salvar o cliente: {exc}")

    if selected_row is None:
        return

    st.markdown("**Documentos do cliente**")
    client_documents_df = documents_df[documents_df["client_id"] == client_id].copy() if "client_id" in documents_df.columns else pd.DataFrame()
    if not client_documents_df.empty:
        for _, doc_row in client_documents_df.sort_values(["Tipo Documento", "Instituição"]).iterrows():
            status_label = normalize_text(doc_row["Status"]) or "SEM STATUS"
            last_update = doc_row["Última Atualização"]
            if pd.isna(last_update):
                last_update_label = "sem data"
            else:
                last_update_label = pd.to_datetime(last_update).strftime("%d/%m/%Y")
            st.markdown(
                f"- **{normalize_text(doc_row['documento_descricao'])}** | status: `{status_label}` | atualização: `{last_update_label}`"
            )

        bulk_doc_editor = client_documents_df[
            ["document_id", "Tipo Documento", "Instituição", "Status", "Última Atualização"]
        ].copy()
        bulk_doc_editor["Última Atualização"] = pd.to_datetime(
            bulk_doc_editor["Última Atualização"], errors="coerce"
        ).dt.date
        edited_docs_df = st.data_editor(
            bulk_doc_editor,
            use_container_width=True,
            hide_index=True,
            disabled=["document_id", "Tipo Documento", "Instituição"],
            column_config={
                "Status": st.column_config.SelectboxColumn(
                    "Status",
                    options=["PENDENTE", "RECEBIDO", "SEM STATUS"],
                ),
                "Última Atualização": st.column_config.DateColumn("Última Atualização", format="DD/MM/YYYY"),
            },
            key=f"bulk_docs_editor_{client_id}",
        )
        if st.button("Salvar checklist de documentos", use_container_width=True, key=f"save_bulk_docs_{client_id}"):
            try:
                save_document_bulk_updates(
                    supabase_client,
                    [
                        {
                            "document_id": row["document_id"],
                            "Status": row["Status"],
                            "last_update": row["Última Atualização"].isoformat() if pd.notna(row["Última Atualização"]) else None,
                        }
                        for _, row in edited_docs_df.iterrows()
                    ],
                    client_id,
                )
                st.success("Checklist de documentos atualizado com sucesso.")
                st.rerun()
            except Exception as exc:
                st.error(f"Não foi possível atualizar o checklist em lote: {exc}")
    else:
        st.caption("Esse cliente ainda não tem documentos cadastrados.")

    with st.form(f"document_add_form_{client_id}"):
        doc_col_1, doc_col_2 = st.columns(2)
        with doc_col_1:
            new_document_type = st.text_input("Tipo de documento")
            new_institution = st.text_input("Instituição")
            new_status = st.selectbox("Status do documento", options=["PENDENTE", "RECEBIDO", "SEM STATUS"])
        with doc_col_2:
            new_last_update = st.date_input("Última atualização", value=date.today())
            new_control_key = st.text_input("Chave de controle")
        add_document = st.form_submit_button("Adicionar documento", use_container_width=True)

    if add_document:
        try:
            save_document_record(
                supabase_client,
                client_id=client_id,
                document_type=new_document_type,
                institution=new_institution,
                status=new_status,
                last_update=new_last_update,
                control_key=new_control_key,
            )
            st.success("Documento adicionado com sucesso.")
            st.rerun()
        except Exception as exc:
            st.error(f"Não foi possível adicionar o documento: {exc}")

    if not client_documents_df.empty and "document_id" in client_documents_df.columns:
        editable_docs = {
            f"{row['Tipo Documento']} - {row['Instituição']} ({row['Status']})": row
            for _, row in client_documents_df.iterrows()
        }
        selected_doc_label = st.selectbox("Documento para editar ou remover", options=list(editable_docs.keys()))
        selected_doc_row = editable_docs[selected_doc_label]

        with st.form(f"document_edit_form_{client_id}_{int(selected_doc_row['document_id'])}"):
            edit_col_1, edit_col_2 = st.columns(2)
            with edit_col_1:
                edit_document_type = st.text_input("Tipo de documento atual", value=selected_doc_row["Tipo Documento"])
                edit_institution = st.text_input("Instituição atual", value=selected_doc_row["Instituição"])
                edit_status = st.selectbox(
                    "Status atual",
                    options=["PENDENTE", "RECEBIDO", "SEM STATUS"],
                    index=["PENDENTE", "RECEBIDO", "SEM STATUS"].index(selected_doc_row["Status"])
                    if selected_doc_row["Status"] in ["PENDENTE", "RECEBIDO", "SEM STATUS"]
                    else 0,
                )
            with edit_col_2:
                existing_doc_date = selected_doc_row["Última Atualização"]
                if pd.isna(existing_doc_date):
                    existing_doc_date = date.today()
                else:
                    existing_doc_date = pd.to_datetime(existing_doc_date).date()
                edit_last_update = st.date_input("Última atualização atual", value=existing_doc_date)
                edit_control_key = st.text_input("Chave de controle atual", value=selected_doc_row["chave_controle"])
            update_document = st.form_submit_button("Salvar alteração do documento", use_container_width=True)

        if update_document:
            try:
                update_document_record(
                    supabase_client,
                    document_id=int(selected_doc_row["document_id"]),
                    client_id=client_id,
                    document_type=edit_document_type,
                    institution=edit_institution,
                    status=edit_status,
                    last_update=edit_last_update,
                    control_key=edit_control_key,
                )
                st.success("Documento atualizado com sucesso.")
                st.rerun()
            except Exception as exc:
                st.error(f"Não foi possível atualizar o documento: {exc}")

        if st.button("Remover documento selecionado", use_container_width=True, type="secondary"):
            try:
                delete_document_record(supabase_client, int(selected_doc_row["document_id"]), client_id)
                st.success("Documento removido com sucesso.")
                st.rerun()
            except Exception as exc:
                st.error(f"Não foi possível remover o documento: {exc}")

    st.divider()
    confirm_delete = st.checkbox("Confirmo que quero excluir este cliente e seus documentos vinculados")
    if st.button("Excluir cliente selecionado", use_container_width=True, type="secondary", disabled=not confirm_delete):
        try:
            delete_client_record(supabase_client, client_id)
            st.success("Cliente excluído com sucesso.")
            st.rerun()
        except Exception as exc:
            st.error(f"Não foi possível excluir o cliente: {exc}")


def render_registry_page_clean(
    supabase_client: Client | None,
    people_df: pd.DataFrame,
    documents_df: pd.DataFrame,
    team_df: pd.DataFrame,
    user_profile: dict[str, object],
    show_header: bool = True,
) -> None:
    if show_header:
        st.header("Atendimento comercial")
    if supabase_client is None:
        st.info("Faça login para consultar e manter os cadastros.")
        return

    can_manage_records = bool(user_profile.get("can_manage_records", False))
    assigned_options = ["Não atribuído"] + sorted(
        set(
            team_df["name"].dropna().tolist()
            + ["Wanessa", "Paulo", "Valdivone", "Michelle", "Erlane", "Duda", "Malu", "Heverton", "Renato"]
        )
    )

    consult_tab, client_tab, docs_tab = st.tabs(
        ["Consultar clientes", "Cadastrar ou editar cliente", "Atualizar documentos"]
    )

    with consult_tab:
        st.markdown("**Consulta rápida**")
        filter_col_1, filter_col_2, filter_col_3, filter_col_4 = st.columns(4)
        with filter_col_1:
            search_name = st.text_input("Nome", key="registry_search_name")
        with filter_col_2:
            group_filter = st.multiselect(
                "Grupo",
                options=sorted(people_df["Grupo"].dropna().unique()),
                default=[],
                key="registry_group_filter",
            )
        with filter_col_3:
            documentation_filter = st.multiselect(
                "Documentação",
                options=sorted(people_df["Documentação"].dropna().unique()),
                default=[],
                key="registry_documentation_filter",
            )
        with filter_col_4:
            status_filter = st.multiselect(
                "Status da declaração",
                options=sorted(people_df["Status Preenchimento"].dropna().unique()),
                default=[],
                key="registry_status_filter",
            )

        registry_df = people_df.copy()
        if search_name:
            registry_df = registry_df[
                registry_df["NOME"].map(lambda value: search_name.upper() in normalize_text(value).upper())
            ].copy()
        if group_filter:
            registry_df = registry_df[registry_df["Grupo"].isin(group_filter)].copy()
        if documentation_filter:
            registry_df = registry_df[registry_df["Documentação"].isin(documentation_filter)].copy()
        if status_filter:
            registry_df = registry_df[registry_df["Status Preenchimento"].isin(status_filter)].copy()

        consult_columns = [
            "NOME",
            "Grupo",
            "Status Preenchimento",
            "Documentação",
            "Recebidos / Total",
            "% documentação recebida",
            "Responsável pelo Preenchimento",
            "Nivel de Complexidade",
            "CPF",
            "Telefone",
            "Tem Certificado Digital",
        ]
        consult_df = registry_df[consult_columns].sort_values(["Grupo", "NOME"]).copy()
        consult_df["Tem Certificado Digital"] = consult_df["Tem Certificado Digital"].map(
            lambda value: "Sim" if bool(value) else "Não"
        )
        st.dataframe(consult_df, use_container_width=True, hide_index=True)

    with client_tab:
        if not can_manage_records:
            st.warning("Seu usuário pode consultar, mas não alterar cadastros.")
        selected_name = st.selectbox(
            "Cliente",
            options=["Novo cliente"] + sorted(people_df["NOME"].dropna().unique().tolist()),
            key="registry_client_select",
            disabled=not can_manage_records,
        )
        selected_row = None if selected_name == "Novo cliente" else people_df[people_df["NOME"] == selected_name].iloc[0]
        client_id = int(selected_row["client_id"]) if selected_row is not None else None

        with st.form("client_maintenance_form"):
            col_1, col_2 = st.columns(2)
            with col_1:
                full_name = st.text_input("Nome completo", value=selected_row["NOME"] if selected_row is not None else "")
                group_name = st.text_input("Grupo", value=selected_row["Grupo"] if selected_row is not None else "")
                complexity = st.text_input(
                    "Nível de complexidade",
                    value=selected_row["Nivel de Complexidade"] if selected_row is not None else "",
                )
                meeting_status = st.text_input("Reunião", value=selected_row["Reunião"] if selected_row is not None else "")
                assigned_preparer = st.selectbox(
                    "Responsável pelo preenchimento",
                    options=assigned_options,
                    index=assigned_options.index(selected_row["Responsável pelo Preenchimento"])
                    if selected_row is not None and selected_row["Responsável pelo Preenchimento"] in assigned_options
                    else 0,
                )
            with col_2:
                tax_status = st.selectbox(
                    "Status da declaração",
                    options=STATUS_OPTIONS,
                    index=STATUS_OPTIONS.index(selected_row["Status Preenchimento"])
                    if selected_row is not None and selected_row["Status Preenchimento"] in STATUS_OPTIONS
                    else 0,
                )
                post_filing_status = st.text_input(
                    "Status pós-envio",
                    value=selected_row["Status Pós-Envio"] if selected_row is not None else "",
                )
                cpf = st.text_input("CPF", value=selected_row.get("CPF", "") if selected_row is not None else "")
                phone = st.text_input("Telefone", value=selected_row.get("Telefone", "") if selected_row is not None else "")
                gov_password = st.text_input(
                    "Senha Gov",
                    value=selected_row.get("Senha Gov", "") if selected_row is not None else "",
                )
                has_digital_certificate = st.checkbox(
                    "Tem certificado digital",
                    value=bool(selected_row.get("Tem Certificado Digital", False)) if selected_row is not None else False,
                )
                power_of_attorney = st.text_input(
                    "Cadastro de procuração",
                    value=selected_row.get("Cadastro de Procuração", "") if selected_row is not None else "",
                )

            saved_client = st.form_submit_button(
                "Salvar cliente",
                use_container_width=True,
                disabled=not can_manage_records,
            )

        if saved_client:
            try:
                normalized_name = normalize_key(full_name)
                saved_id = save_client_record(
                    supabase_client,
                    {
                        "normalized_name": normalized_name,
                        "full_name": normalize_text(full_name),
                        "group_name": normalize_text(group_name),
                        "meeting_status": normalize_text(meeting_status),
                        "complexity_level": normalize_text(complexity),
                        "tax_status": normalize_text(tax_status),
                        "assigned_preparer": normalize_text(assigned_preparer),
                        "post_filing_status": normalize_text(post_filing_status),
                        "documentation_status": selected_row["Documentação"] if selected_row is not None else "Sem documentação",
                        "active": True,
                    },
                    {
                        "cpf": normalize_cpf(cpf),
                        "phone": normalize_phone(phone),
                        "gov_password": gov_password,
                        "has_digital_certificate": has_digital_certificate,
                        "power_of_attorney": power_of_attorney,
                    },
                    client_id=client_id,
                )
                st.success(f"Cliente salvo com sucesso. ID {saved_id}.")
                st.rerun()
            except Exception as exc:
                st.error(f"Não foi possível salvar o cliente: {exc}")

        if selected_row is not None:
            with st.expander("Excluir cliente"):
                confirm_delete = st.checkbox(
                    "Confirmo que quero excluir este cliente e seus documentos vinculados",
                    key=f"confirm_delete_client_{client_id}",
                )
                if st.button(
                    "Excluir cliente selecionado",
                    use_container_width=True,
                    type="secondary",
                    disabled=not confirm_delete or not can_manage_records,
                    key=f"delete_client_{client_id}",
                ):
                    try:
                        delete_client_record(supabase_client, client_id)
                        st.success("Cliente excluído com sucesso.")
                        st.rerun()
                    except Exception as exc:
                        st.error(f"Não foi possível excluir o cliente: {exc}")

    with docs_tab:
        if not can_manage_records:
            st.warning("Seu usuário pode consultar, mas não alterar documentos.")
        if people_df.empty:
            st.info("Cadastre um cliente antes de montar o checklist de documentos.")
            return

        doc_client_name = st.selectbox(
            "Cliente para checklist",
            options=sorted(people_df["NOME"].dropna().unique().tolist()),
            key="registry_docs_client_select",
        )
        doc_client_row = people_df[people_df["NOME"] == doc_client_name].iloc[0]
        doc_client_id = int(doc_client_row["client_id"])
        st.metric("Documentação", doc_client_row["Documentação"], doc_client_row["Recebidos / Total"])

        client_documents_df = (
            documents_df[documents_df["client_id"] == doc_client_id].copy()
            if "client_id" in documents_df.columns
            else pd.DataFrame()
        )
        st.markdown("**Lista atual de documentos**")
        if client_documents_df.empty:
            st.caption("Esse cliente ainda não tem documentos cadastrados.")
        else:
            for _, doc_row in client_documents_df.sort_values(["Tipo Documento", "Instituição"]).iterrows():
                status_label = normalize_text(doc_row["Status"]) or "SEM STATUS"
                last_update = doc_row["Última Atualização"]
                last_update_label = "sem data" if pd.isna(last_update) else pd.to_datetime(last_update).strftime("%d/%m/%Y")
                st.markdown(
                    f"- **{normalize_text(doc_row['documento_descricao'])}** | status: `{status_label}` | atualização: `{last_update_label}`"
                )

            checklist_df = client_documents_df[
                ["document_id", "documento_descricao", "Status", "Última Atualização"]
            ].copy()
            checklist_df["Recebido"] = checklist_df["Status"].map(
                lambda value: normalize_text(value).upper() == "RECEBIDO"
            )
            checklist_df["Última Atualização"] = pd.to_datetime(
                checklist_df["Última Atualização"], errors="coerce"
            ).dt.date
            checklist_df = checklist_df.rename(columns={"documento_descricao": "Documento"})
            edited_docs_df = st.data_editor(
                checklist_df[["document_id", "Documento", "Recebido", "Última Atualização"]],
                use_container_width=True,
                hide_index=True,
                disabled=["document_id", "Documento"],
                column_config={
                    "Recebido": st.column_config.CheckboxColumn("Recebido?"),
                    "Última Atualização": st.column_config.DateColumn("Última atualização", format="DD/MM/YYYY"),
                },
                key=f"bulk_docs_editor_clean_{doc_client_id}",
            )
            if st.button(
                "Salvar checklist",
                use_container_width=True,
                key=f"save_bulk_docs_clean_{doc_client_id}",
                disabled=not can_manage_records,
            ):
                try:
                    save_document_bulk_updates(
                        supabase_client,
                        [
                            {
                                "document_id": row["document_id"],
                                "Status": "RECEBIDO" if bool(row["Recebido"]) else "PENDENTE",
                                "last_update": row["Última Atualização"].isoformat()
                                if pd.notna(row["Última Atualização"])
                                else None,
                            }
                            for _, row in edited_docs_df.iterrows()
                        ],
                        doc_client_id,
                    )
                    st.success("Checklist de documentos atualizado com sucesso.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"Não foi possível atualizar o checklist: {exc}")

        st.markdown("**Adicionar documento**")
        with st.form(f"document_add_form_clean_{doc_client_id}"):
            doc_col_1, doc_col_2 = st.columns(2)
            with doc_col_1:
                new_document_type = st.text_input("Tipo de documento")
                new_institution = st.text_input("Instituição")
                new_status = st.selectbox("Status do documento", options=["PENDENTE", "RECEBIDO", "SEM STATUS"])
            with doc_col_2:
                new_last_update = st.date_input("Última atualização", value=date.today())
                new_control_key = st.text_input("Chave de controle")
            add_document = st.form_submit_button(
                "Adicionar documento",
                use_container_width=True,
                disabled=not can_manage_records,
            )

        if add_document:
            try:
                save_document_record(
                    supabase_client,
                    client_id=doc_client_id,
                    document_type=new_document_type,
                    institution=new_institution,
                    status=new_status,
                    last_update=new_last_update,
                    control_key=new_control_key,
                )
                st.success("Documento adicionado com sucesso.")
                st.rerun()
            except Exception as exc:
                st.error(f"Não foi possível adicionar o documento: {exc}")

        if not client_documents_df.empty and "document_id" in client_documents_df.columns:
            with st.expander("Editar ou remover documento específico"):
                editable_docs = {
                    f"{row['Tipo Documento']} - {row['Instituição']} ({row['Status']})": row
                    for _, row in client_documents_df.iterrows()
                }
                selected_doc_label = st.selectbox("Documento", options=list(editable_docs.keys()))
                selected_doc_row = editable_docs[selected_doc_label]

                with st.form(f"document_edit_form_clean_{doc_client_id}_{int(selected_doc_row['document_id'])}"):
                    edit_col_1, edit_col_2 = st.columns(2)
                    with edit_col_1:
                        edit_document_type = st.text_input("Tipo de documento atual", value=selected_doc_row["Tipo Documento"])
                        edit_institution = st.text_input("Instituição atual", value=selected_doc_row["Instituição"])
                        edit_status = st.selectbox(
                            "Status atual",
                            options=["PENDENTE", "RECEBIDO", "SEM STATUS"],
                            index=["PENDENTE", "RECEBIDO", "SEM STATUS"].index(selected_doc_row["Status"])
                            if selected_doc_row["Status"] in ["PENDENTE", "RECEBIDO", "SEM STATUS"]
                            else 0,
                        )
                    with edit_col_2:
                        existing_doc_date = selected_doc_row["Última Atualização"]
                        existing_doc_date = date.today() if pd.isna(existing_doc_date) else pd.to_datetime(existing_doc_date).date()
                        edit_last_update = st.date_input("Última atualização atual", value=existing_doc_date)
                        edit_control_key = st.text_input("Chave de controle atual", value=selected_doc_row["chave_controle"])
                    update_document = st.form_submit_button(
                        "Salvar alteração do documento",
                        use_container_width=True,
                        disabled=not can_manage_records,
                    )

                if update_document:
                    try:
                        update_document_record(
                            supabase_client,
                            document_id=int(selected_doc_row["document_id"]),
                            client_id=doc_client_id,
                            document_type=edit_document_type,
                            institution=edit_institution,
                            status=edit_status,
                            last_update=edit_last_update,
                            control_key=edit_control_key,
                        )
                        st.success("Documento atualizado com sucesso.")
                        st.rerun()
                    except Exception as exc:
                        st.error(f"Não foi possível atualizar o documento: {exc}")

                if st.button(
                    "Remover documento selecionado",
                    use_container_width=True,
                    type="secondary",
                    disabled=not can_manage_records,
                    key=f"remove_doc_clean_{int(selected_doc_row['document_id'])}",
                ):
                    try:
                        delete_document_record(supabase_client, int(selected_doc_row["document_id"]), doc_client_id)
                        st.success("Documento removido com sucesso.")
                        st.rerun()
                    except Exception as exc:
                        st.error(f"Não foi possível remover o documento: {exc}")


def save_preparation_updates(
    client: Client,
    client_id: int,
    assigned_preparer: str,
    tax_status: str,
    acting_as: str,
    steps_payload: list[dict],
    allow_checkpoint_updates: bool = True,
) -> None:
    timestamp = datetime.utcnow().replace(microsecond=0).isoformat()
    client.table("clients").update(
        {
            "assigned_preparer": assigned_preparer,
            "tax_status": tax_status,
            "updated_at": timestamp,
        }
    ).eq("id", client_id).execute()
    if allow_checkpoint_updates and steps_payload:
        payload = [
            {
                "client_id": client_id,
                "step_key": item["step_key"],
                "step_label": item["step_label"],
                "completed": item["completed"],
                "note": item["note"],
                "updated_by": acting_as,
                "updated_at": timestamp,
            }
            for item in steps_payload
        ]
        client.table("declaration_checkpoints").upsert(
            payload,
            on_conflict="client_id,step_key",
        ).execute()
    invalidate_data_cache()


def render_preparation_editor(
    supabase_client: Client | None,
    people_df: pd.DataFrame,
    documents_df: pd.DataFrame,
    checkpoints_df: pd.DataFrame,
    team_df: pd.DataFrame,
    user_profile: dict[str, object],
) -> None:
    return render_preparation_editor_clean(
        supabase_client,
        people_df,
        documents_df,
        checkpoints_df,
        team_df,
        user_profile,
    )
    st.subheader("Controle do preenchimento")
    preparers = (
        team_df[team_df["role"].astype(str).str.contains("preenchimento", case=False, na=False)]["name"].tolist()
        if not team_df.empty and "role" in team_df.columns
        else []
    )
    if not preparers:
        preparers = sorted(
            name for name in people_df["Responsável pelo Preenchimento"].dropna().unique() if name != "Não atribuído"
        )
    acting_as = normalize_text(user_profile.get("display_name", ""))
    if not acting_as or acting_as == normalize_text(user_profile.get("email", "")):
        acting_as = "Equipe"
    can_edit_full_preparation = user_profile.get("permission_level") == "full"
    can_edit_preparation = user_profile.get("permission_level") in {"full", "status_only"}

    working_df = people_df.copy()
    filter_col_1, filter_col_2, filter_col_3, filter_col_4 = st.columns(4)
    with filter_col_1:
        complexity_filter = st.multiselect(
            "Filtrar por complexidade",
            options=sorted(people_df["Nivel de Complexidade"].dropna().unique()),
            default=[],
        )
    with filter_col_2:
        documentation_filter = st.multiselect(
            "Filtrar por documentação",
            options=sorted(people_df["Documentação"].dropna().unique()),
            default=[],
        )
    with filter_col_3:
        group_filter = st.multiselect(
            "Filtrar por grupo",
            options=sorted(people_df["Grupo"].dropna().unique()),
            default=[],
        )
    with filter_col_4:
        status_filter = st.multiselect(
            "Status do preenchimento",
            options=sorted(people_df["Status Preenchimento"].dropna().unique()),
            default=[],
        )

    if complexity_filter:
        working_df = working_df[working_df["Nivel de Complexidade"].isin(complexity_filter)].copy()
    if documentation_filter:
        working_df = working_df[working_df["Documentação"].isin(documentation_filter)].copy()
    if group_filter:
        working_df = working_df[working_df["Grupo"].isin(group_filter)].copy()
    if status_filter:
        working_df = working_df[working_df["Status Preenchimento"].isin(status_filter)].copy()

    display_df = working_df[
        [
            "NOME",
            "Grupo",
            "Nivel de Complexidade",
            "Status Preenchimento",
            "Documentação",
            "Recebidos / Total",
            "Responsável pelo Preenchimento",
            "CPF",
            "Telefone",
            "Senha Gov",
            "Tem Certificado Digital",
            "Cadastro de Procuração",
            "documentos_enviados_lista",
            "documentos_faltantes_lista",
            "last_activity_at",
        ]
    ].copy()
    display_df["Tem Certificado Digital"] = display_df["Tem Certificado Digital"].map(lambda value: "Sim" if bool(value) else "Não")
    display_df = display_df.rename(
        columns={
            "documentos_enviados_lista": "Documentos recebidos",
            "documentos_faltantes_lista": "Documentos faltantes",
        }
    )
    st.dataframe(
        display_df.sort_values(["Grupo", "Nivel de Complexidade", "NOME"]),
        use_container_width=True,
        hide_index=True,
    )
    if supabase_client is not None and can_edit_preparation:
        bulk_status_df = working_df[
            ["client_id", "NOME", "Grupo", "Responsável pelo Preenchimento", "Status Preenchimento"]
        ].copy()
        edited_status_df = st.data_editor(
            bulk_status_df,
            use_container_width=True,
            hide_index=True,
            disabled=["client_id", "NOME", "Grupo"],
            column_config={
                "Responsável pelo Preenchimento": st.column_config.SelectboxColumn(
                    "Responsável pelo Preenchimento",
                    options=sorted(set(preparers + ["Heverton", "Renato", "Não atribuído"])),
                ),
                "Status Preenchimento": st.column_config.SelectboxColumn(
                    "Status Preenchimento",
                    options=STATUS_OPTIONS,
                ),
            },
            key="bulk_preparation_editor",
        )
        if st.button("Salvar alterações em lote", use_container_width=True):
            try:
                save_batch_client_updates(
                    supabase_client,
                    edited_status_df.to_dict("records"),
                    acting_as,
                )
                st.success("Alterações em lote salvas com sucesso.")
                st.rerun()
            except Exception as exc:
                st.error(f"Não foi possível salvar as alterações em lote: {exc}")

    editable_df = working_df.copy()
    if editable_df.empty:
        st.info("Nenhum cliente encontrado com os filtros atuais.")
        return

    client_options = editable_df[["client_id", "NOME"]].dropna().drop_duplicates().sort_values("NOME")
    selected_label = st.selectbox(
        "Cliente para atualizar andamento",
        options=client_options["NOME"].tolist(),
    )
    selected_row = editable_df[editable_df["NOME"] == selected_label].iloc[0]
    checkpoint_state = build_checkpoint_editor_state(checkpoints_df, int(selected_row["client_id"]))
    document_sections = build_document_sections(
        documents_df=documents_df,
        checkpoints_df=checkpoints_df,
        client_id=int(selected_row["client_id"]),
    )

    info_col_1, info_col_2 = st.columns(2)
    with info_col_1:
        st.markdown("**Dados principais**")
        st.write(f"Grupo: {selected_row['Grupo']}")
        st.write(f"Nível de complexidade: {selected_row['Nivel de Complexidade']}")
        st.write(f"Status da documentação: {selected_row['Documentação']}")
        st.write(f"Recebimento: {selected_row['Recebidos / Total']} ({selected_row['% documentação recebida']:.1f}%)")
        st.write(f"Status do preenchimento: {selected_row['Status Preenchimento']}")
        st.write("Documentos recebidos:")
        st.write(selected_row.get("documentos_enviados_lista", "") or "Nenhum documento marcado como recebido.")
        st.write("Documentos faltantes:")
        st.write(selected_row.get("documentos_faltantes_lista", "") or "Nenhum documento faltante.")
    with info_col_2:
        st.markdown("**Dados sensíveis / apoio**")
        st.write(f"CPF: {selected_row.get('CPF', '') or 'Não informado'}")
        st.write(f"Telefone: {selected_row.get('Telefone', '') or 'Não informado'}")
        st.write(f"Senha Gov: {selected_row.get('Senha Gov', '') or 'Não informada'}")
        st.write(
            "Certificado digital: "
            + ("Sim" if bool(selected_row.get("Tem Certificado Digital", False)) else "Não")
        )
        st.write(
            f"Procuração: {selected_row.get('Cadastro de Procuração', '') or 'Não informada'}"
        )
        st.write(f"Última movimentação: {selected_row.get('last_activity_at', '') or 'Sem registro'}")
        st.write(f"Andamento do checklist: {selected_row.get('Progresso Geral', '0/4 (0.0%)')}")

    st.markdown("**Atualização do preenchimento**")
    with st.form(f"prep_form_{int(selected_row['client_id'])}"):
        assigned_options = ["Não atribuído"] + sorted(set(preparers + ["Heverton", "Renato"]))
        assigned_preparer = st.selectbox(
            "Responsável pelo preenchimento",
            options=assigned_options,
            index=assigned_options.index(selected_row["Responsável pelo Preenchimento"])
            if selected_row["Responsável pelo Preenchimento"] in assigned_options
            else 0,
            disabled=not can_edit_preparation,
        )
        status_options = STATUS_OPTIONS + sorted(
            status
            for status in people_df["Status Preenchimento"].dropna().unique()
            if status not in STATUS_OPTIONS
        )
        selected_status = st.selectbox(
            "Status do preenchimento",
            options=status_options,
            index=status_options.index(selected_row["Status Preenchimento"])
            if selected_row["Status Preenchimento"] in status_options
            else 0,
            disabled=not can_edit_preparation,
        )

        payload = []
        for step in checkpoint_state:
            answer_options = ["Não", "Sim"]
            checked = st.radio(
                step["step_label"],
                options=answer_options,
                index=1 if step["completed"] else 0,
                horizontal=True,
                key=f"{selected_row['client_id']}_{step['step_key']}_checked",
                disabled=not can_edit_full_preparation,
            ) == "Sim"
            note_label = "Observação"
            if step["step_key"] == "dividas_emprestimos":
                note_label = "Se sim, especifique"
            note = st.text_input(
                f"{note_label} - {step['step_label']}",
                value=step["note"],
                key=f"{selected_row['client_id']}_{step['step_key']}_note",
                disabled=not can_edit_full_preparation,
            )
            payload.append(
                {
                    "step_key": step["step_key"],
                    "step_label": step["step_label"],
                    "completed": checked,
                    "note": normalize_text(note),
                }
            )

        for section in document_sections:
            st.markdown(f"**{section['section_label']}**")
            for item in section["items"]:
                label = f"{item['step_label']} | documento: {item['document_status']}"
                checked = st.checkbox(
                    f"{label} | lançado",
                    value=item["completed"],
                    key=f"{selected_row['client_id']}_{item['step_key']}_checked",
                    disabled=not can_edit_full_preparation,
                )
                note = st.text_input(
                    f"Observação - {item['step_label']}",
                    value=item["note"],
                    key=f"{selected_row['client_id']}_{item['step_key']}_note",
                    disabled=not can_edit_full_preparation,
                )
                payload.append(
                    {
                        "step_key": item["step_key"],
                        "step_label": item["step_label"],
                        "completed": checked,
                        "note": normalize_text(note),
                    }
                )
        submitted = st.form_submit_button(
            "Salvar andamento",
            use_container_width=True,
            disabled=not can_edit_preparation,
        )

    if submitted:
        if supabase_client is None:
            st.warning("Para salvar andamento, use o modo Supabase com login.")
        else:
            try:
                save_preparation_updates(
                    supabase_client,
                    int(selected_row["client_id"]),
                    assigned_preparer,
                    selected_status,
                    acting_as,
                    payload if can_edit_full_preparation else [],
                    allow_checkpoint_updates=can_edit_full_preparation,
                )
                st.success("Andamento salvo com sucesso.")
                st.rerun()
            except Exception as exc:
                st.error(f"Não foi possível salvar o andamento: {exc}")


def render_preparation_editor_clean(
    supabase_client: Client | None,
    people_df: pd.DataFrame,
    documents_df: pd.DataFrame,
    checkpoints_df: pd.DataFrame,
    team_df: pd.DataFrame,
    user_profile: dict[str, object],
) -> None:
    st.header("Preenchimento")
    preparers = (
        team_df[team_df["role"].astype(str).str.contains("preenchimento", case=False, na=False)]["name"].tolist()
        if not team_df.empty and "role" in team_df.columns
        else []
    )
    if not preparers:
        preparers = sorted(
            name for name in people_df["Responsável pelo Preenchimento"].dropna().unique() if name != "Não atribuído"
        )

    acting_as = normalize_text(user_profile.get("display_name", "")) or "Equipe"
    can_edit_full_preparation = user_profile.get("permission_level") == "full"
    can_edit_preparation = user_profile.get("permission_level") in {"full", "status_only"}

    list_tab, update_tab = st.tabs(["Lista geral", "Atualizar preenchimento"])

    with list_tab:
        st.markdown("**Filtrar declarações**")
        filter_col_1, filter_col_2, filter_col_3, filter_col_4 = st.columns(4)
        with filter_col_1:
            complexity_filter = st.multiselect(
                "Complexidade",
                options=sorted(people_df["Nivel de Complexidade"].dropna().unique()),
                default=[],
                key="prep_complexity_filter",
            )
        with filter_col_2:
            documentation_filter = st.multiselect(
                "Documentação",
                options=sorted(people_df["Documentação"].dropna().unique()),
                default=[],
                key="prep_documentation_filter",
            )
        with filter_col_3:
            group_filter = st.multiselect(
                "Grupo",
                options=sorted(people_df["Grupo"].dropna().unique()),
                default=[],
                key="prep_group_filter",
            )
        with filter_col_4:
            status_filter = st.multiselect(
                "Status do preenchimento",
                options=sorted(people_df["Status Preenchimento"].dropna().unique()),
                default=[],
                key="prep_status_filter",
            )

        working_df = people_df.copy()
        if complexity_filter:
            working_df = working_df[working_df["Nivel de Complexidade"].isin(complexity_filter)].copy()
        if documentation_filter:
            working_df = working_df[working_df["Documentação"].isin(documentation_filter)].copy()
        if group_filter:
            working_df = working_df[working_df["Grupo"].isin(group_filter)].copy()
        if status_filter:
            working_df = working_df[working_df["Status Preenchimento"].isin(status_filter)].copy()

        display_columns = [
            "NOME",
            "Grupo",
            "Nivel de Complexidade",
            "Status Preenchimento",
            "Documentação",
            "Recebidos / Total",
            "% documentação recebida",
            "Responsável pelo Preenchimento",
            "CPF",
            "Telefone",
            "Senha Gov",
            "Tem Certificado Digital",
            "Cadastro de Procuração",
            "documentos_enviados_lista",
            "documentos_faltantes_lista",
            "last_activity_at",
        ]
        display_df = working_df[display_columns].copy()
        display_df["Tem Certificado Digital"] = display_df["Tem Certificado Digital"].map(
            lambda value: "Sim" if bool(value) else "Não"
        )
        display_df = display_df.rename(
            columns={
                "documentos_enviados_lista": "Documentos recebidos",
                "documentos_faltantes_lista": "Documentos faltantes",
            }
        )
        st.dataframe(
            display_df.sort_values(["Grupo", "Nivel de Complexidade", "NOME"]),
            use_container_width=True,
            hide_index=True,
        )

        if supabase_client is not None and can_edit_preparation and not working_df.empty:
            st.markdown("**Alteração rápida em lote**")
            bulk_status_df = working_df[
                ["client_id", "NOME", "Grupo", "Responsável pelo Preenchimento", "Status Preenchimento"]
            ].copy()
            edited_status_df = st.data_editor(
                bulk_status_df,
                use_container_width=True,
                hide_index=True,
                disabled=["client_id", "NOME", "Grupo"],
                column_config={
                    "Responsável pelo Preenchimento": st.column_config.SelectboxColumn(
                        "Responsável pelo Preenchimento",
                        options=sorted(set(preparers + ["Heverton", "Renato", "Não atribuído"])),
                    ),
                    "Status Preenchimento": st.column_config.SelectboxColumn(
                        "Status Preenchimento",
                        options=STATUS_OPTIONS,
                    ),
                },
                key="bulk_preparation_editor_clean",
            )
            if st.button("Salvar alterações em lote", use_container_width=True, key="save_bulk_preparation_clean"):
                try:
                    save_batch_client_updates(
                        supabase_client,
                        edited_status_df.to_dict("records"),
                        acting_as,
                    )
                    st.success("Alterações em lote salvas com sucesso.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"Não foi possível salvar as alterações em lote: {exc}")

    with update_tab:
        editable_df = people_df.copy()
        if editable_df.empty:
            st.info("Nenhum cliente encontrado.")
            return

        selected_label = st.selectbox(
            "Cliente para atualizar andamento",
            options=editable_df[["client_id", "NOME"]].dropna().drop_duplicates().sort_values("NOME")["NOME"].tolist(),
            key="prep_update_client_select",
        )
        selected_row = editable_df[editable_df["NOME"] == selected_label].iloc[0]
        client_id = int(selected_row["client_id"])
        checkpoint_state = build_checkpoint_editor_state(checkpoints_df, client_id)
        document_sections = build_document_sections(
            documents_df=documents_df,
            checkpoints_df=checkpoints_df,
            client_id=client_id,
        )

        summary_col_1, summary_col_2, summary_col_3, summary_col_4 = st.columns(4)
        with summary_col_1:
            st.metric("Documentação", selected_row["Documentação"], selected_row["Recebidos / Total"])
        with summary_col_2:
            st.metric("Status", selected_row["Status Preenchimento"])
        with summary_col_3:
            st.metric("Responsável", selected_row["Responsável pelo Preenchimento"])
        with summary_col_4:
            st.metric("Progresso", selected_row.get("Progresso Geral", "0/0 (0.0%)"))

        info_col_1, info_col_2 = st.columns(2)
        with info_col_1:
            with st.expander("Documentos recebidos e faltantes", expanded=True):
                st.markdown("**Recebidos**")
                st.write(selected_row.get("documentos_enviados_lista", "") or "Nenhum documento marcado como recebido.")
                st.markdown("**Faltantes**")
                st.write(selected_row.get("documentos_faltantes_lista", "") or "Nenhum documento faltante.")
        with info_col_2:
            with st.expander("Dados de apoio", expanded=True):
                st.write(f"Grupo: {selected_row['Grupo']}")
                st.write(f"Nível de complexidade: {selected_row['Nivel de Complexidade']}")
                st.write(f"CPF: {selected_row.get('CPF', '') or 'Não informado'}")
                st.write(f"Telefone: {selected_row.get('Telefone', '') or 'Não informado'}")
                st.write(f"Senha Gov: {selected_row.get('Senha Gov', '') or 'Não informada'}")
                st.write("Certificado digital: " + ("Sim" if bool(selected_row.get("Tem Certificado Digital", False)) else "Não"))
                st.write(f"Procuração: {selected_row.get('Cadastro de Procuração', '') or 'Não informada'}")

        with st.form(f"prep_form_clean_{client_id}"):
            assigned_options = ["Não atribuído"] + sorted(set(preparers + ["Heverton", "Renato"]))
            assigned_preparer = st.selectbox(
                "Responsável pelo preenchimento",
                options=assigned_options,
                index=assigned_options.index(selected_row["Responsável pelo Preenchimento"])
                if selected_row["Responsável pelo Preenchimento"] in assigned_options
                else 0,
                disabled=not can_edit_preparation,
            )
            status_options = STATUS_OPTIONS + sorted(
                status
                for status in people_df["Status Preenchimento"].dropna().unique()
                if status not in STATUS_OPTIONS
            )
            selected_status = st.selectbox(
                "Status do preenchimento",
                options=status_options,
                index=status_options.index(selected_row["Status Preenchimento"])
                if selected_row["Status Preenchimento"] in status_options
                else 0,
                disabled=not can_edit_preparation,
            )

            st.markdown("**Confirmações gerais**")
            payload = []
            for step in checkpoint_state:
                checked = st.radio(
                    step["step_label"],
                    options=["Não", "Sim"],
                    index=1 if step["completed"] else 0,
                    horizontal=True,
                    key=f"clean_{client_id}_{step['step_key']}_checked",
                    disabled=not can_edit_full_preparation,
                ) == "Sim"
                note_label = "Observação"
                if step["step_key"] == "dividas_emprestimos":
                    note_label = "Se sim, especifique o que"
                note = st.text_input(
                    f"{note_label} - {step['step_label']}",
                    value=step["note"],
                    key=f"clean_{client_id}_{step['step_key']}_note",
                    disabled=not can_edit_full_preparation,
                )
                payload.append(
                    {
                        "step_key": step["step_key"],
                        "step_label": step["step_label"],
                        "completed": checked,
                        "note": normalize_text(note),
                    }
                )

            st.markdown("**Checklist de lançamentos por documento**")
            if not document_sections:
                st.caption("Nenhum documento cadastrado para este cliente.")
            for section in document_sections:
                st.markdown(f"**{section['section_label']}**")
                for item in section["items"]:
                    checked = st.checkbox(
                        f"{item['step_label']} | lançado",
                        value=item["completed"],
                        help=f"Status do documento: {item['document_status']}",
                        key=f"clean_{client_id}_{item['step_key']}_checked",
                        disabled=not can_edit_full_preparation,
                    )
                    note = st.text_input(
                        f"Observação se não lançado - {item['step_label']}",
                        value=item["note"],
                        key=f"clean_{client_id}_{item['step_key']}_note",
                        disabled=not can_edit_full_preparation,
                    )
                    payload.append(
                        {
                            "step_key": item["step_key"],
                            "step_label": item["step_label"],
                            "completed": checked,
                            "note": normalize_text(note),
                        }
                    )

            submitted = st.form_submit_button(
                "Salvar andamento",
                use_container_width=True,
                disabled=not can_edit_preparation,
            )

        if submitted:
            if supabase_client is None:
                st.warning("Para salvar andamento, use o login do Supabase.")
            else:
                try:
                    save_preparation_updates(
                        supabase_client,
                        client_id,
                        assigned_preparer,
                        selected_status,
                        acting_as,
                        payload if can_edit_full_preparation else [],
                        allow_checkpoint_updates=can_edit_full_preparation,
                    )
                    st.success("Andamento salvo com sucesso.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"Não foi possível salvar o andamento: {exc}")


def render_review_page(people_df: pd.DataFrame, snapshot_df: pd.DataFrame, supabase_client: Client | None) -> None:
    st.header("Revisão")
    counts = people_df["Status Preenchimento"].value_counts()
    metric_1, metric_2, metric_3, metric_4, metric_5 = st.columns(5)
    with metric_1:
        st.metric("Declarações", len(people_df))
    with metric_2:
        st.metric("Pendentes", int(counts.get("PENDENTE", 0)))
    with metric_3:
        st.metric("Em preenchimento", int(counts.get("EM PREENCHIMENTO", 0)))
    with metric_4:
        st.metric("Em revisão", int(snapshot_df.loc[0, "em_revisao"]))
    with metric_5:
        st.metric("Transmitidas", int(snapshot_df.loc[0, "transmitidas"]))

    filter_col_1, filter_col_2, filter_col_3 = st.columns(3)
    with filter_col_1:
        name_filter = st.text_input("Filtrar por nome")
        group_filter = st.multiselect("Filtrar por grupo", options=sorted(people_df["Grupo"].dropna().unique()))
    with filter_col_2:
        status_filter = st.multiselect(
            "Status do preenchimento",
            options=sorted(people_df["Status Preenchimento"].dropna().unique()),
            default=sorted(people_df["Status Preenchimento"].dropna().unique()),
        )
        documentation_filter = st.multiselect(
            "Status da documentação",
            options=sorted(people_df["Documentação"].dropna().unique()),
            default=sorted(people_df["Documentação"].dropna().unique()),
        )
    with filter_col_3:
        responsible_filter = st.multiselect(
            "Responsável",
            options=sorted(people_df["Responsável pelo Preenchimento"].dropna().unique()),
            default=sorted(people_df["Responsável pelo Preenchimento"].dropna().unique()),
        )

    filtered_people_df = people_df.copy()
    if name_filter:
        filtered_people_df = filtered_people_df[
            filtered_people_df["NOME"].map(lambda value: name_filter.upper() in normalize_text(value).upper())
        ].copy()
    if group_filter:
        filtered_people_df = filtered_people_df[filtered_people_df["Grupo"].isin(group_filter)].copy()
    if status_filter:
        filtered_people_df = filtered_people_df[filtered_people_df["Status Preenchimento"].isin(status_filter)].copy()
    if documentation_filter:
        filtered_people_df = filtered_people_df[filtered_people_df["Documentação"].isin(documentation_filter)].copy()
    if responsible_filter:
        filtered_people_df = filtered_people_df[
            filtered_people_df["Responsável pelo Preenchimento"].isin(responsible_filter)
        ].copy()

    consolidated_df = filtered_people_df[
        [
            "NOME",
            "Grupo",
            "Status Preenchimento",
            "Documentação",
            "Recebidos / Total",
            "documentos_enviados_lista",
            "documentos_faltantes_lista",
            "Responsável pelo Preenchimento",
            "Progresso Geral",
            "last_activity_at",
        ]
    ].sort_values(["Status Preenchimento", "Responsável pelo Preenchimento", "NOME"])
    consolidated_df = consolidated_df.rename(
        columns={
            "documentos_enviados_lista": "Documentos recebidos",
            "documentos_faltantes_lista": "Documentos faltantes",
        }
    )
    st.dataframe(consolidated_df, use_container_width=True, hide_index=True)
    export_review_df = consolidated_df.copy()
    for column in ["Documentos recebidos", "Documentos faltantes"]:
        export_review_df[column] = export_review_df[column].map(
            lambda value: normalize_text(str(value).replace("\n", " | "))
        )
    st.download_button(
        "Exportar tabela da revisão",
        data=export_review_df.to_csv(index=False, sep=";").encode("utf-8-sig"),
        file_name="revisao_filtrada.csv",
        mime="text/csv",
    )

    st.markdown("**Progresso consolidado**")
    history_df = load_history_remote(supabase_client) if supabase_client is not None else load_history()
    current_date = snapshot_df.loc[0, "data_referencia"].date()
    previous_snapshot = history_df[history_df["data_referencia"].dt.date < current_date].tail(1)
    if previous_snapshot.empty:
        st.caption("Ainda não há comparação diária anterior salva.")
    else:
        prev = previous_snapshot.iloc[0]
        current = snapshot_df.iloc[0]
        st.write(
            f"Transmitidas: {int(prev['transmitidas'])} -> {int(current['transmitidas'])} | "
            f"Em revisão: {int(prev['em_revisao'])} -> {int(current['em_revisao'])} | "
            f"Docs completos: {int(prev['clientes_docs_completos'])} -> {int(current['clientes_docs_completos'])}"
        )

    action_col_1, action_col_2 = st.columns([1, 3])
    with action_col_1:
        if st.button("Salvar posição do dia", use_container_width=True):
            if supabase_client is not None:
                save_snapshot_remote(supabase_client, snapshot_df)
            else:
                save_snapshot(snapshot_df)
            st.success("Snapshot salvo.")
    with action_col_2:
        if not history_df.empty:
            history_chart_df = pd.concat([history_df, snapshot_df], ignore_index=True).drop_duplicates(
                subset=["data_referencia"], keep="last"
            )
            st.bar_chart(
                history_chart_df.set_index("data_referencia")[
                    ["transmitidas", "em_revisao", "clientes_docs_completos"]
                ]
            )


def render_import_page(
    supabase_client: Client | None,
    people_df: pd.DataFrame,
    documents_df: pd.DataFrame,
    user_profile: dict[str, object],
) -> None:
    st.header("Cadastros")
    allowed_import_emails = {"paulo.nunes@gestaocontabil.com", "heverton@gestaocontabil.com"}
    user_email = normalize_text(user_profile.get("email", "")).lower()
    if user_email not in allowed_import_emails:
        st.warning("Esta área é restrita ao Paulo e ao Heverton.")
        return
    if supabase_client is None:
        st.info("Faça login para importar dados para o banco.")
        return

    st.markdown("**Importação e conferência do banco**")
    template_df = build_standard_template()
    st.download_button(
        "Baixar planilha padrão",
        data=template_df.to_csv(index=False, sep=";").encode("utf-8-sig"),
        file_name="modelo_importacao_irpf.csv",
        mime="text/csv",
        use_container_width=True,
    )

    upload_col_1, upload_col_2, upload_col_3 = st.columns(3)
    with upload_col_1:
        standard_upload = st.file_uploader(
            "Planilha padrão",
            type=["csv", "xlsx", "xls"],
            key="standard_import_upload",
        )
    with upload_col_2:
        clients_upload = st.file_uploader(
            "Planilha de clientes",
            type=["csv", "xlsx", "xls"],
            key="clients_import_upload",
        )
    with upload_col_3:
        documents_upload = st.file_uploader(
            "Planilha de documentos",
            type=["csv", "xlsx", "xls"],
            key="documents_import_upload",
        )

    if standard_upload is None and clients_upload is None and documents_upload is None:
        st.info("Envie a planilha padrão ou uma das planilhas atuais para comparar com o banco.")
        return

    try:
        if standard_upload is not None:
            imported_clients_df, imported_documents_df = parse_standard_import(
                standard_upload.getvalue(),
                standard_upload.name,
            )
        else:
            if clients_upload is not None:
                imported_clients_df = parse_clients(clients_upload.getvalue(), clients_upload.name)
            else:
                imported_clients_df = pd.DataFrame(
                    columns=CLIENT_COLUMNS + ["Documentação Informada", "Tem Certificado Digital", "chave_pessoa"]
                )
            if documents_upload is not None:
                imported_documents_df = parse_documents(documents_upload.getvalue(), documents_upload.name)
            else:
                imported_documents_df = pd.DataFrame(columns=DOCUMENT_COLUMNS + ["documento_descricao", "chave_pessoa"])
    except Exception as exc:
        st.error(f"Não foi possível ler a importação: {exc}")
        return

    imported_clients_df = imported_clients_df[
        imported_clients_df["chave_pessoa"].map(lambda value: normalize_text(value) not in ["", "SEM NOME IDENTIFICADO"])
    ].copy()
    imported_documents_df = imported_documents_df[
        imported_documents_df["chave_pessoa"].map(lambda value: normalize_text(value) not in ["", "SEM NOME IDENTIFICADO"])
    ].copy()

    comparison = build_import_comparison(imported_clients_df, imported_documents_df, people_df, documents_df)
    metric_1, metric_2, metric_3, metric_4 = st.columns(4)
    with metric_1:
        st.metric("Novos clientes", len(comparison["new_clients"]))
    with metric_2:
        st.metric("Clientes com alteração", len(comparison["changed_clients"]))
    with metric_3:
        st.metric("Novos documentos", len(comparison["new_documents"]))
    with metric_4:
        st.metric("Documentos alterados", len(comparison["changed_documents"]))

    diff_tabs = st.tabs(["Novos clientes", "Clientes alterados", "Novos documentos", "Documentos alterados"])
    with diff_tabs[0]:
        if comparison["new_clients"].empty:
            st.caption("Nenhum cliente novo encontrado.")
        else:
            st.dataframe(comparison["new_clients"], use_container_width=True, hide_index=True)
    with diff_tabs[1]:
        if comparison["changed_clients"].empty:
            st.caption("Nenhuma alteração cadastral encontrada.")
        else:
            st.dataframe(comparison["changed_clients"], use_container_width=True, hide_index=True)
    with diff_tabs[2]:
        if comparison["new_documents"].empty:
            st.caption("Nenhum documento novo encontrado.")
        else:
            st.dataframe(comparison["new_documents"], use_container_width=True, hide_index=True)
    with diff_tabs[3]:
        if comparison["changed_documents"].empty:
            st.caption("Nenhuma alteração de documento encontrada.")
        else:
            st.dataframe(comparison["changed_documents"], use_container_width=True, hide_index=True)

    total_changes = sum(len(df) for df in comparison.values())
    confirm_import = st.checkbox("Conferi as diferenças e quero atualizar o banco")
    if st.button(
        "Aplicar atualização no banco",
        use_container_width=True,
        disabled=not confirm_import or total_changes == 0,
    ):
        try:
            updated_clients, updated_documents = apply_import_updates(
                supabase_client,
                imported_clients_df,
                imported_documents_df,
                people_df,
                documents_df,
            )
            st.success(
                f"Importação aplicada. Clientes processados: {updated_clients}. Documentos processados: {updated_documents}."
            )
            st.rerun()
        except Exception as exc:
            st.error(f"Não foi possível aplicar a importação: {exc}")


def main() -> None:
    st.set_page_config(page_title="IRPF - Controle de Declarações", layout="wide")

    supabase_client = build_supabase_client()
    if supabase_client is None or not st.session_state.get("supabase_user_email"):
        render_login_page()
        st.stop()

    try:
        bundle = load_supabase_bundle_cached(supabase_client)
        clients_df = bundle["clients_df"]
        documents_df = bundle["documents_df"]
        private_df = bundle["private_df"]
        team_df = bundle["team_df"]
        checkpoints_df = bundle["checkpoints_df"]
        user_profile = get_user_profile(team_df, st.session_state.get("supabase_user_email", ""), "Supabase")
    except Exception as exc:
        st.error(f"Não foi possível carregar o banco de dados: {exc}")
        st.stop()

    if not user_profile.get("allowed_sectors"):
        st.error("Seu usuário está autenticado, mas ainda não foi liberado para nenhum setor.")
        st.stop()

    render_app_header(user_profile)
    selected_sector = render_sector_selector(user_profile)

    people_df = build_people_summary(clients_df, documents_df)
    people_df = attach_private_data(people_df, private_df)
    people_df = attach_progress(people_df, build_checkpoint_summary(checkpoints_df, documents_df))
    snapshot_df = build_snapshot(date.today(), clients_df, people_df)
    auto_snapshot_saved = ensure_daily_snapshot(snapshot_df, supabase_client)

    if auto_snapshot_saved:
        st.success("Posição do dia salva automaticamente após as 17h.")

    if selected_sector == "Comercial":
        render_commercial_page(
            people_df,
            supabase_client,
            documents_df,
            team_df,
            user_profile,
        )
    elif selected_sector == "Preenchimento":
        render_preparation_editor(
            supabase_client,
            people_df,
            documents_df,
            checkpoints_df,
            team_df,
            user_profile,
        )
    elif selected_sector == "Revisão":
        render_review_page(people_df, snapshot_df, supabase_client)
    else:
        render_import_page(
            supabase_client,
            people_df,
            documents_df,
            user_profile,
        )


if __name__ == "__main__":
    main()
