import pytest

from ensembl.variation_utils.file_locator import file_locator

class TestAbstractClass():
    def test_file_locator(self):
        with pytest.raises(TypeError) as excinfo:
            file_locator.FileLocator()
        assert "Can't instantiate abstract class FileLocator" in str(excinfo.value)

    def test_file_locator_factory(self):
        with pytest.raises(TypeError) as excinfo:
            file_locator.FileLocatorFactory()
        assert "Can't instantiate abstract class FileLocatorFactory" in str(excinfo.value)
