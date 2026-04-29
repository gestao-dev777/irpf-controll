from __future__ import annotations

import argparse

from database import get_client, get_client_documents, get_connection, list_clients, now_iso, upsert_client, delete_client


def command_list(args: argparse.Namespace) -> None:
    with get_connection() as connection:
        rows = list_clients(
            connection,
            search=args.search,
            tax_status=args.status,
            assigned_preparer=args.assigned_to,
            documentation_status=args.documentation,
        )
    for row in rows:
        print(
            f"[{row['id']}] {row['full_name']} | grupo={row['group_name']} | "
            f"status={row['tax_status']} | docs={row['documentation_status']} | "
            f"responsavel={row['assigned_preparer']}"
        )


def command_show(args: argparse.Namespace) -> None:
    with get_connection() as connection:
        client = get_client(connection, args.client_id)
        documents = get_client_documents(connection, args.client_id)
    if client is None:
        print("Cliente não encontrado.")
        return
    print(dict(client))
    print("\nDocumentos:")
    for document in documents:
        print(dict(document))


def command_upsert(args: argparse.Namespace) -> None:
    normalized_name = " ".join(args.name.upper().split())
    with get_connection() as connection:
        client_id = upsert_client(
            connection,
            normalized_name=normalized_name,
            full_name=args.name,
            group_name=args.group,
            meeting_status=args.meeting,
            complexity_level=args.complexity,
            tax_status=args.status,
            assigned_preparer=args.assigned_to,
            post_filing_status=args.post_filing_status,
            documentation_status=args.documentation,
            cpf=args.cpf,
            phone=args.phone,
            gov_password=args.gov_password,
            has_digital_certificate=args.has_digital_certificate,
            power_of_attorney=args.power_of_attorney,
            notes=f"Atualizado via CLI em {now_iso()}",
        )
    print(f"Cliente salvo com id {client_id}.")


def command_delete(args: argparse.Namespace) -> None:
    with get_connection() as connection:
        delete_client(connection, args.client_id)
    print("Cliente desativado com sucesso.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Operações básicas de cliente no banco IRPF.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="Lista ou busca clientes.")
    list_parser.add_argument("--search", default="", help="Busca por nome.")
    list_parser.add_argument("--status", default="", help="Filtro por status da declaração.")
    list_parser.add_argument("--assigned-to", default="", help="Filtro por responsável.")
    list_parser.add_argument("--documentation", default="", help="Filtro por documentação.")
    list_parser.set_defaults(func=command_list)

    show_parser = subparsers.add_parser("show", help="Mostra detalhes de um cliente.")
    show_parser.add_argument("client_id", type=int)
    show_parser.set_defaults(func=command_show)

    upsert_parser = subparsers.add_parser("upsert", help="Insere ou atualiza cliente.")
    upsert_parser.add_argument("--name", required=True)
    upsert_parser.add_argument("--group", default="")
    upsert_parser.add_argument("--meeting", default="")
    upsert_parser.add_argument("--complexity", default="")
    upsert_parser.add_argument("--status", default="")
    upsert_parser.add_argument("--assigned-to", default="")
    upsert_parser.add_argument("--post-filing-status", default="")
    upsert_parser.add_argument("--documentation", default="")
    upsert_parser.add_argument("--cpf", default="")
    upsert_parser.add_argument("--phone", default="")
    upsert_parser.add_argument("--gov-password", default="")
    upsert_parser.add_argument("--has-digital-certificate", action="store_true")
    upsert_parser.add_argument("--power-of-attorney", default="")
    upsert_parser.set_defaults(func=command_upsert)

    delete_parser = subparsers.add_parser("delete", help="Desativa cliente.")
    delete_parser.add_argument("client_id", type=int)
    delete_parser.set_defaults(func=command_delete)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
