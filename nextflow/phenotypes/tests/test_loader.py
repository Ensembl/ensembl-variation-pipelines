import unittest
from unittest.mock import Mock, call

from phenotypes.loader import PhenotypeLoader


class PhenotypeLookupTests(unittest.TestCase):
    def setUp(self):
        self.cursor = Mock()
        self.cursor.execute.return_value = iter(())
        self.loader = PhenotypeLoader(self.cursor)
        self.loader.insert = Mock(side_effect=range(100_000, 120_000))

    def test_new_names_are_inserted_once_and_matched_exactly(self):
        first = self.loader.phenotype(1, "Neurofibroma", None, None)
        self.assertEqual(self.loader.phenotype(1, "Neurofibroma", None, None), first)
        self.assertNotEqual(self.loader.phenotype(1, "neurofibroma", None, None), first)
        self.assertEqual(self.loader.insert.call_args_list, [
            call("phenotype", {"phenotype_name": "Neurofibroma"}),
            call("phenotype", {"phenotype_name": "neurofibroma"}),
        ])
        self.cursor.execute.assert_called_once()
        self.assertEqual(self.cursor.execute.call_args.args[1], (1,))

    def test_existing_phenotypes_keep_measurement_identity(self):
        self.cursor.execute.return_value = iter([
            (7, "Gene expression", None, None),
            (8, "Gene expression", "expression", 101),
            (9, "Gene expression", "expression", 102),
            (10, "Gene expression", "protein_abundance", 101),
        ])
        for expected, kind, entity_id in (
            (7, None, None), (8, "expression", 101),
            (9, "expression", 102), (10, "protein_abundance", 101),
        ):
            with self.subTest(kind=kind, entity_id=entity_id):
                self.assertEqual(
                    self.loader.phenotype(1, "Gene expression", kind, entity_id), expected,
                )
        self.loader.insert.assert_not_called()
        self.cursor.execute.assert_called_once()

    def test_new_measurements_are_inserted_once_per_key(self):
        first = self.loader.phenotype(1, "Gene expression", "expression", 101)
        self.assertEqual(self.loader.phenotype(1, "Gene expression", "expression", 101), first)
        second = self.loader.phenotype(1, "Gene expression", "expression", 102)
        third = self.loader.phenotype(1, "Gene expression", "protein_abundance", 101)
        self.assertEqual(len({first, second, third}), 3)
        self.assertEqual(self.loader.created["phenotype_measurement"], 3)
        self.assertEqual(
            [item.args[1] for item in self.cursor.execute.call_args_list[1:]],
            [(first, "expression", 101), (second, "expression", 102),
             (third, "protein_abundance", 101)],
        )
        self.assertEqual(self.cursor.execute.call_count, 4)

    def test_identical_names_from_different_sources_are_not_merged(self):
        self.cursor.execute.side_effect = [iter([(7, "Neurofibroma", None, None)]), iter(())]
        first = self.loader.phenotype(1, "Neurofibroma", None, None)
        second = self.loader.phenotype(2, "Neurofibroma", None, None)
        self.assertNotEqual(first, second)
        self.assertEqual(self.loader.phenotype(1, "Neurofibroma", None, None), first)
        self.assertEqual(self.loader.phenotype(2, "Neurofibroma", None, None), second)
        self.assertEqual(
            [item.args[1] for item in self.cursor.execute.call_args_list], [(1,), (2,)],
        )
        self.loader.insert.assert_called_once()

    def test_phenotypes_are_retained_beyond_ten_thousand_names(self):
        first = self.loader.phenotype(1, "Condition 0", None, None)
        for number in range(1, 10_001):
            self.loader.phenotype(1, f"Condition {number}", None, None)
        self.assertEqual(self.loader.phenotype(1, "Condition 0", None, None), first)
        self.assertEqual(self.loader.insert.call_count, 10_001)
        self.cursor.execute.assert_called_once()


class LoaderStatisticsTests(unittest.TestCase):
    def test_no_written_tables_does_not_analyze_the_database(self):
        cursor = Mock()
        PhenotypeLoader(cursor).analyze("phenotypes")
        cursor.execute.assert_not_called()

    def test_analyze_only_written_tables_in_the_configured_schema(self):
        cursor = Mock()
        loader = PhenotypeLoader(cursor)
        loader.created.update({"source_report": 1_000, "phenotype_evidence": 1_000})
        loader.analyze('test"schema')
        cursor.execute.assert_called_once()
        self.assertEqual(
            cursor.execute.call_args.args[0].as_string(),
            'ANALYZE "test""schema"."source_report", "test""schema"."phenotype_evidence"',
        )
        self.assertEqual(loader.created, {"source_report": 1_000, "phenotype_evidence": 1_000})
        cursor.connection.commit.assert_not_called()
