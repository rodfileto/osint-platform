import tempfile
import unittest
from pathlib import Path

import duckdb

from cleaners import build_reference_query


class ReferenceQueryBuildersTest(unittest.TestCase):
    def test_reference_queries_read_latin1_for_all_reference_types(self):
        cases = [
            (
                'cnaes',
                [('0113000', 'Cultivo de cana-de-acucar'), ('6463800', 'Outras sociedades de participação, exceto holdings')],
                'cnae_fiscal',
                'descricao',
                '6463800',
                'Outras sociedades de participação, exceto holdings',
            ),
            (
                'motivos',
                [('1', 'Extinção por encerramento liquidação voluntária'), ('63', 'Falência')],
                'codigo',
                'descricao',
                '01',
                'Extinção por encerramento liquidação voluntária',
            ),
            (
                'municipios',
                [('1', 'Guaíba'), ('9997', 'Exterior')],
                'codigo',
                'nome',
                '0001',
                'Guaíba',
            ),
            (
                'naturezas',
                [('1015', 'Órgão Público do Poder Executivo Federal'), ('3999', 'Associação Privada')],
                'codigo',
                'descricao',
                1015,
                'Órgão Público do Poder Executivo Federal',
            ),
            (
                'paises',
                [('13', 'Afeganistão'), ('158', 'Brasil')],
                'codigo',
                'nome',
                '013',
                'Afeganistão',
            ),
            (
                'qualificacoes',
                [('5', 'Administrador'), ('49', 'Sócio-Administrador')],
                'codigo',
                'descricao',
                '05',
                'Administrador',
            ),
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            con = duckdb.connect()
            try:
                for ref_type, rows, pk_column, desc_column, expected_code, expected_desc in cases:
                    csv_path = temp_path / f'{ref_type}.csv'
                    parquet_path = temp_path / f'{ref_type}.parquet'

                    csv_lines = [f'"{code}";"{description}"' for code, description in rows]
                    csv_path.write_text('\n'.join(csv_lines) + '\n', encoding='latin-1')

                    query = build_reference_query(str(csv_path), str(parquet_path), ref_type)
                    con.execute(query)

                    loaded_rows = con.execute(f"SELECT * FROM '{parquet_path}' ORDER BY 1").fetchall()
                    self.assertEqual(len(loaded_rows), len(rows), ref_type)

                    match = con.execute(
                        f"SELECT {pk_column}, {desc_column} FROM '{parquet_path}' WHERE {pk_column} = ?",
                        [expected_code],
                    ).fetchone()

                    self.assertIsNotNone(match, ref_type)
                    self.assertEqual(match[0], expected_code, ref_type)
                    self.assertEqual(match[1], expected_desc, ref_type)
            finally:
                con.close()


if __name__ == '__main__':
    unittest.main()