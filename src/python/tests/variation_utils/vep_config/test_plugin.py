import os
import pytest
from unittest import mock

from ensembl.variation_utils.vep_config import plugin

class TestAbstractClass():
    def test_plugin_args_builder(self):
        with pytest.raises(TypeError) as excinfo:
            plugin.PluginArgsBuilder()
        assert "Can't instantiate abstract class PluginArgsBuilder" in str(
            excinfo.value
        )

class TestPluginBuilderFactory():
    @pytest.fixture()
    def plugin_builder_factory(self):
        return plugin.PluginArgsBuilderFactory()

    def test_defaults(self, plugin_builder_factory):
        assert plugin_builder_factory._builder is None

        builder = plugin_builder_factory.set_builder()
        assert isinstance(builder, plugin.CurrentPluginArgsBuilder)
        assert isinstance(
            plugin_builder_factory._builder, plugin.CurrentPluginArgsBuilder
        )

class TestPluginBuilder():
    # @pytest.fixture()
    # def set_env(self, monkeypatch, plugin_data_dir, repo_dir):
    #     with mock.patch.dict(os.environ, clear=True):
    #         monkeypatch.setenv("PLUGIN_DATA_DIR", plugin_data_dir)
    #         monkeypatch.setenv("ENSEMBL_ROOT_DIR", repo_dir)
    #         yield

    @pytest.fixture()
    def plugin_builder(self, setenv):
        # monkeypatch.setenv("ENSEMBL_ROOT_DIR", "hulala")
        return plugin.CurrentPluginArgsBuilder()

    def test_default(self, plugin_builder, plugin_data_dir):
        assert isinstance(plugin_builder.matcher, plugin.RepoPluginConfigMatcher)
        assert plugin_builder.species is None
        assert plugin_builder.assembly is None
        assert plugin_builder.version is None
        assert plugin_builder.base_path == os.path.join(plugin_data_dir, "grch38/eNone/vep/plugin_data")

    def test_match(self, plugin_builder):
        plugin_builder.species = "homo_sapiens"

        assert plugin_builder.match("CADD")
        assert plugin_builder.match("Downstream")
        assert plugin_builder.match("AlphaMissense")

        assert not plugin_builder.match("NonExisting")

    def test_get_args(self, plugin_builder, plugin_data_dir):
        plugin_builder.species = "homo_sapiens"
        plugin_builder.assembly = "GRCh38"
        plugin_builder.version = 114

        assert plugin_builder.get_args("CADD") == {
            "snv": f"{plugin_data_dir}/grch38/e114/vep/plugin_data/CADD_GRCh38_1.7_whole_genome_SNVs.tsv.gz",
            "indels": f"{plugin_data_dir}/grch38/e114/vep/plugin_data/CADD_GRCh38_1.7_InDels.tsv.gz"
        }

class TestRepoPluginConfigMatcher():
    @pytest.fixture()
    def set_env(self, monkeypatch, repo_dir):
        with mock.patch.dict(os.environ, clear=True):
            monkeypatch.setenv("ENSEMBL_ROOT_DIR", repo_dir)
            yield

    @pytest.fixture()
    def matcher(self, set_env):
        print("DEBUG", os.environ["ENSEMBL_ROOT_DIR"])
        return plugin.RepoPluginConfigMatcher()
    
    def test_defaults(self, matcher, repo_dir):
        print("DEBUG 2", repo_dir)
        print("DEBUG 3", matcher.repo_dir)
        assert matcher.repo_dir == repo_dir
        assert matcher.config_file == os.path.join(repo_dir, "VEP_plugins", "plugin_config.txt")

    def test_match(self, matcher):
        species = "homo_sapiens"

        assert matcher.match("CADD", species)
        assert matcher.match("Downstream", species)
        assert matcher.match("AlphaMissense", species)

        assert not matcher.match("NonExisting", species)