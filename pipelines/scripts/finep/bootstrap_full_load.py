"""Download and load the full public FINEP datasets into PostgreSQL."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from finep.downloader import FINEP_DATASETS, collect_dataset
from finep.loader_ancine import load_ancine
from finep.loader_contratacao import load_operacao_direta
from finep.loader_credito_descentralizado import load_credito_descentralizado
from finep.loader_investimento import load_investimento
from finep.loader_liberacoes_ancine import load_liberacoes_ancine
from finep.loader_liberacoes_credito_descentralizado import load_liberacoes_credito_descentralizado
from finep.loader_liberacoes_operacao_direta import load_liberacoes_operacao_direta


def bootstrap_full_finep(output_dir: Path, force_download: bool = True) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)

    download_results = {}
    for dataset_type, url in FINEP_DATASETS.items():
        download_results[dataset_type] = collect_dataset(
            dataset_type=dataset_type,
            url=url,
            output_dir=output_dir,
            force=force_download,
        )

    load_results = {
        "projetos_operacao_direta": load_operacao_direta(),
        "projetos_credito_descentralizado": load_credito_descentralizado(),
        "projetos_investimento": load_investimento(),
        "projetos_ancine": load_ancine(),
        "liberacoes_operacao_direta": load_liberacoes_operacao_direta(),
        "liberacoes_credito_descentralizado": load_liberacoes_credito_descentralizado(),
        "liberacoes_ancine": load_liberacoes_ancine(),
    }

    return {
        "downloads": download_results,
        "loads": load_results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        default="/opt/airflow/data/finep/raw",
        help="Directory where the downloaded FINEP xlsx files will be stored.",
    )
    parser.add_argument(
        "--no-force-download",
        action="store_true",
        help="Reuse the latest remote copy when the HTTP headers indicate the file did not change.",
    )
    args = parser.parse_args()

    result = bootstrap_full_finep(
        output_dir=Path(args.output_dir),
        force_download=not args.no_force_download,
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
