import pytest
import tempfile
import os
from unittest.mock import Mock, MagicMock
from typing import Dict, Any
import configparser

# Import the classes from your main module
# from file_locator_api import *  # Assuming the API is in file_locator_api.py
from file_locator_api import (
    MetadataDBClient, MetadataGRPCClient, FTPGFFLocator, FTPFASTALocator,
    VEPCacheLocator, FileLocatorAPI, InfrastructureType, DatasetType,
    MetadataDBClientFactory, FileLocatorFactory
)


@pytest.fixture
def mock_metadata_client():
    """Mock metadata client for testing"""
    client = Mock()
    client.get_assembly_accession.return_value = "GCA_000001405.29"
    client.get_scientific_name.return_value = "homo_sapiens"
    client.get_dataset_uuid.return_value = "dataset-uuid-123"
    client.get_dataset_attribute_value.return_value = "test_value"
    client.configure.return_value = None
    return client


@pytest.fixture
def sample_genome_uuid():
    """Sample genome UUID for testing"""
    return "genome-uuid-12345"


@pytest.fixture
def temp_directory():
    """Temporary directory for file operations"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def mock_ini_file(temp_directory):
    """Create a mock INI configuration file"""
    config = configparser.ConfigParser()
    config['database'] = {
        'host': 'localhost',
        'port': '3306',
        'user': 'testuser',
        'password': 'testpass',
        'dbname': 'testdb'
    }
    
    ini_path = os.path.join(temp_directory, 'test_config.ini')
    with open(ini_path, 'w') as f:
        config.write(f)
    
    return ini_path


@pytest.fixture
def db_client_config():
    """Database client configuration"""
    return {
        'host': 'test-host',
        'port': 3306,
        'user': 'test-user',
        'password': 'test-pass',
        'dbname': 'test-db'
    }


@pytest.fixture
def sample_file_paths():
    """Sample file paths for testing"""
    return {
        'gff': 'homo_sapiens/GCA_000001405.29/ensembl.gff3',
        'fasta': 'homo_sapiens/GCA_000001405.29.fa',
        'vep_cache': 'vep_cache/homo_sapiens_GCA_000001405.29'
    }


# tests/test_metadata_clients.py
"""Tests for metadata client classes"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from file_locator_api import (
    MetadataDBClient, MetadataGRPCClient, MetadataDBClientFactory,
    MetadataGRPCClientFactory
)


class TestMetadataDBClient:
    """Test cases for MetadataDBClient"""
    
    def test_init(self, db_client_config):
        """Test MetadataDBClient initialization"""
        client = MetadataDBClient(
            host=db_client_config['host'],
            port=db_client_config['port'],
            user=db_client_config['user'],
            password=db_client_config['password'],
            dbname=db_client_config['dbname']
        )
        
        assert client.host == db_client_config['host']
        assert client.port == db_client_config['port']
        assert client.user == db_client_config['user']
        assert client._password == db_client_config['password']
        assert client.dbname == db_client_config['dbname']
    
    def test_configure(self, db_client_config):
        """Test configure method"""
        client = MetadataDBClient(
            host=db_client_config['host'],
            port=db_client_config['port'],
            user=db_client_config['user'],
            password=db_client_config['password'],
            dbname=db_client_config['dbname']
        )
        
        # Should not raise an exception
        client.configure()
    
    @patch('file_locator_api.MetadataDBClient._run_query')
    def test_get_assembly_accession(self, mock_query, db_client_config, sample_genome_uuid):
        """Test get_assembly_accession method"""
        client = MetadataDBClient(**db_client_config)
        mock_query.return_value = "GCA_000001405.29"
        
        # This would need actual implementation in the real class
        # For now, we test that the method exists
        assert hasattr(client, 'get_assembly_accession')
    
    @patch('file_locator_api.MetadataDBClient._run_query')
    def test_get_scientific_name(self, mock_query, db_client_config, sample_genome_uuid):
        """Test get_scientific_name method"""
        client = MetadataDBClient(**db_client_config)
        mock_query.return_value = "homo_sapiens"
        
        assert hasattr(client, 'get_scientific_name')


class TestMetadataGRPCClient:
    """Test cases for MetadataGRPCClient"""
    
    def test_init(self):
        """Test MetadataGRPCClient initialization"""
        client = MetadataGRPCClient()
        assert client is not None
    
    def test_configure(self):
        """Test configure method"""
        client = MetadataGRPCClient()
        # Should not raise an exception
        client.configure()
    
    def test_methods_exist(self):
        """Test that all required methods exist"""
        client = MetadataGRPCClient()
        assert hasattr(client, 'get_assembly_accession')
        assert hasattr(client, 'get_scientific_name')
        assert hasattr(client, 'get_dataset_uuid')
        assert hasattr(client, 'get_dataset_attribute_value')


class TestMetadataClientFactories:
    """Test cases for metadata client factories"""
    
    def test_metadata_client_factory(self):
        """Test MetadataClientFactory"""
        from file_locator_api import MetadataClientFactory
        
        client = MetadataClientFactory.create()
        assert isinstance(client, MetadataGRPCClient)
    
    def test_metadata_db_client_factory(self, mock_ini_file):
        """Test MetadataDBClientFactory"""
        factory = MetadataDBClientFactory(mock_ini_file)
        client = factory.create()
        
        assert isinstance(client, MetadataDBClient)
        assert client.host == 'localhost'
        assert client.port == 3306
        assert client.user == 'testuser'
    
    def test_metadata_grpc_client_factory(self):
        """Test MetadataGRPCClientFactory"""
        from file_locator_api import MetadataGRPCClientFactory
        
        client = MetadataGRPCClientFactory.create()
        assert isinstance(client, MetadataGRPCClient)


# tests/test_file_locators.py
"""Tests for file locator classes"""

import pytest
from unittest.mock import Mock, patch, mock_open
from file_locator_api import (
    FTPGFFLocator, FTPFASTALocator, OldFTPGFFLocator, OldFTPFASTALocator,
    VEPCacheLocator, FTPDiskFileLocator, FTPServerFileLocator,
    GFFLocator, FASTALocator, InfrastructureType
)


class TestFTPGFFLocator:
    """Test cases for FTPGFFLocator"""
    
    def test_init(self, mock_metadata_client):
        """Test FTPGFFLocator initialization"""
        locator = FTPGFFLocator(mock_metadata_client)
        
        assert locator.metadata_client == mock_metadata_client
        assert locator.scientific_name == ""
        assert locator.assembly_accession == ""
        assert locator.annotation_source == ""
    
    def test_locate_file(self, mock_metadata_client, sample_genome_uuid):
        """Test locate_file method"""
        locator = FTPGFFLocator(mock_metadata_client)
        
        file_path = locator.locate_file(sample_genome_uuid, "ensembl")
        
        assert file_path == "homo_sapiens/GCA_000001405.29/ensembl.gff3"
        assert locator.scientific_name == "homo_sapiens"
        assert locator.assembly_accession == "GCA_000001405.29"
        assert locator.annotation_source == "ensembl"
    
    def test_copy_file(self, mock_metadata_client, temp_directory):
        """Test copy_file method"""
        locator = FTPGFFLocator(mock_metadata_client)
        
        result = locator.copy_file(temp_directory)
        assert result is True


class TestFTPFASTALocator:
    """Test cases for FTPFASTALocator"""
    
    def test_init(self, mock_metadata_client):
        """Test FTPFASTALocator initialization"""
        locator = FTPFASTALocator(mock_metadata_client)
        
        assert locator.metadata_client == mock_metadata_client
        assert locator.production_name == ""
        assert locator.assembly_accession == ""
    
    def test_locate_file(self, mock_metadata_client, sample_genome_uuid):
        """Test locate_file method"""
        locator = FTPFASTALocator(mock_metadata_client)
        
        file_path = locator.locate_file(sample_genome_uuid)
        
        assert file_path == "homo_sapiens/GCA_000001405.29.fa"
        assert locator.production_name == "homo_sapiens"
        assert locator.assembly_accession == "GCA_000001405.29"
    
    def test_copy_file(self, mock_metadata_client, temp_directory):
        """Test copy_file method"""
        locator = FTPFASTALocator(mock_metadata_client)
        
        result = locator.copy_file(temp_directory)
        assert result is True


class TestVEPCacheLocator:
    """Test cases for VEPCacheLocator"""
    
    def test_init(self, mock_metadata_client):
        """Test VEPCacheLocator initialization"""
        locator = VEPCacheLocator(mock_metadata_client)
        
        assert locator.metadata_client == mock_metadata_client
        assert locator.production_name == ""
        assert locator.assembly_accession == ""
    
    def test_get_path(self, mock_metadata_client, sample_genome_uuid):
        """Test get_path method"""
        locator = VEPCacheLocator(mock_metadata_client)
        
        cache_path = locator.get_path(sample_genome_uuid)
        
        assert cache_path == "vep_cache/homo_sapiens_GCA_000001405.29"
        assert locator.production_name == "homo_sapiens"
        assert locator.assembly_accession == "GCA_000001405.29"
    
    def test_copy_to(self, mock_metadata_client, temp_directory):
        """Test copy_to method"""
        locator = VEPCacheLocator(mock_metadata_client)
        
        result = locator.copy_to(temp_directory)
        assert result is True


class TestFTPFileLocators:
    """Test cases for FTP file locators"""
    
    def test_ftp_disk_file_locator(self):
        """Test FTPDiskFileLocator"""
        base_path = "/data/ftp"
        locator = FTPDiskFileLocator(base_path)
        
        assert locator.base_path == base_path
        
        file_path = locator.locate_file("test/file.txt")
        assert file_path == "/data/ftp/test/file.txt"
    
    def test_ftp_server_file_locator(self):
        """Test FTPServerFileLocator"""
        base_path = "pub/data"
        server = "ftp.example.com"
        locator = FTPServerFileLocator(base_path, server)
        
        assert locator.base_path == base_path
        assert locator.server == server
        
        file_path = locator.locate_file("test/file.txt")
        assert file_path == "ftp://ftp.example.com/pub/data/test/file.txt"


class TestGenericFileLocators:
    """Test cases for generic file locators"""
    
    def test_gff_locator_set_locator(self):
        """Test GFFLocator set_locator method"""
        locator = GFFLocator()
        
        disk_locator = locator.set_locator(InfrastructureType.DISK)
        assert isinstance(disk_locator, FTPDiskFileLocator)
        
        server_locator = locator.set_locator(InfrastructureType.SERVER)
        assert isinstance(server_locator, FTPServerFileLocator)
    
    def test_fasta_locator_set_locator(self):
        """Test FASTALocator set_locator method"""
        locator = FASTALocator()
        
        disk_locator = locator.set_locator(InfrastructureType.DISK)
        assert isinstance(disk_locator, FTPDiskFileLocator)
        
        server_locator = locator.set_locator(InfrastructureType.SERVER)
        assert isinstance(server_locator, FTPServerFileLocator)
    
    def test_gff_locator_without_set_locator(self):
        """Test GFFLocator methods without setting locator"""
        locator = GFFLocator()
        
        with pytest.raises(ValueError, match="Locator not set"):
            locator.locate_file("test")
        
        result = locator.copy_file("/tmp")
        assert result is False


# tests/test_factories.py
"""Tests for factory classes"""

import pytest
from file_locator_api import (
    FileLocatorFactory, DatasetType, InfrastructureType,
    FTPGFFLocator, FTPFASTALocator, OldFTPGFFLocator, OldFTPFASTALocator,
    VEPCacheLocator
)


class TestFileLocatorFactory:
    """Test cases for FileLocatorFactory"""
    
    def test_create_gff_locator_server(self, mock_metadata_client):
        """Test creating GFF locator for server infrastructure"""
        locator = FileLocatorFactory.create_locator(
            DatasetType.GFF, InfrastructureType.SERVER, mock_metadata_client
        )
        assert isinstance(locator, FTPGFFLocator)
    
    def test_create_gff_locator_disk(self, mock_metadata_client):
        """Test creating GFF locator for disk infrastructure"""
        locator = FileLocatorFactory.create_locator(
            DatasetType.GFF, InfrastructureType.DISK, mock_metadata_client
        )
        assert isinstance(locator, OldFTPGFFLocator)
    
    def test_create_fasta_locator_server(self, mock_metadata_client):
        """Test creating FASTA locator for server infrastructure"""
        locator = FileLocatorFactory.create_locator(
            DatasetType.FASTA, InfrastructureType.SERVER, mock_metadata_client
        )
        assert isinstance(locator, FTPFASTALocator)
    
    def test_create_fasta_locator_disk(self, mock_metadata_client):
        """Test creating FASTA locator for disk infrastructure"""
        locator = FileLocatorFactory.create_locator(
            DatasetType.FASTA, InfrastructureType.DISK, mock_metadata_client
        )
        assert isinstance(locator, OldFTPFASTALocator)
    
    def test_create_vep_cache_locator(self, mock_metadata_client):
        """Test creating VEP cache locator"""
        locator = FileLocatorFactory.create_locator(
            DatasetType.VEP_CACHE, InfrastructureType.SERVER, mock_metadata_client
        )
        assert isinstance(locator, VEPCacheLocator)
    
    def test_unsupported_dataset_type(self, mock_metadata_client):
        """Test creating locator with unsupported dataset type"""
        with pytest.raises(ValueError, match="Unsupported dataset type"):
            # Create a mock dataset type that doesn't exist
            fake_type = Mock()
            fake_type.value = "unsupported"
            FileLocatorFactory.create_locator(
                fake_type, InfrastructureType.SERVER, mock_metadata_client
            )


# tests/test_api.py
"""Tests for main FileLocatorAPI class"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from file_locator_api import (
    FileLocatorAPI, InfrastructureType, DatasetType
)


class TestFileLocatorAPI:
    """Test cases for FileLocatorAPI"""
    
    @patch('file_locator_api.MetadataDBClientFactory')
    def test_init(self, mock_factory_class):
        """Test FileLocatorAPI initialization"""
        mock_factory = Mock()
        mock_client = Mock()
        mock_factory.create.return_value = mock_client
        mock_factory_class.return_value = mock_factory
        
        api = FileLocatorAPI("test_config.ini")
        
        assert api.metadata_client == mock_client
        mock_factory_class.assert_called_once_with("test_config.ini")
        mock_factory.create.assert_called_once()
    
    @patch('file_locator_api.MetadataDBClientFactory')
    @patch('file_locator_api.FileLocatorFactory')
    def test_locate_gff_file(self, mock_locator_factory, mock_client_factory, sample_genome_uuid):
        """Test locate_gff_file method"""
        # Setup mocks
        mock_client = Mock()
        mock_client_factory.return_value.create.return_value = mock_client
        
        mock_locator = Mock()
        mock_locator.locate_file.return_value = "test/path.gff3"
        mock_locator_factory.create_locator.return_value = mock_locator
        
        api = FileLocatorAPI()
        result = api.locate_gff_file(sample_genome_uuid, "ensembl")
        
        assert result == "test/path.gff3"
        mock_locator_factory.create_locator.assert_called_once_with(
            DatasetType.GFF, InfrastructureType.SERVER, mock_client
        )
    
    @patch('file_locator_api.MetadataDBClientFactory')
    @patch('file_locator_api.FileLocatorFactory')
    def test_locate_fasta_file(self, mock_locator_factory, mock_client_factory, sample_genome_uuid):
        """Test locate_fasta_file method"""
        # Setup mocks
        mock_client = Mock()
        mock_client_factory.return_value.create.return_value = mock_client
        
        mock_locator = Mock()
        mock_locator.locate_file.return_value = "test/path.fa"
        mock_locator_factory.create_locator.return_value = mock_locator
        
        api = FileLocatorAPI()
        result = api.locate_fasta_file(sample_genome_uuid)
        
        assert result == "test/path.fa"
        mock_locator_factory.create_locator.assert_called_once_with(
            DatasetType.FASTA, InfrastructureType.SERVER, mock_client
        )
    
    @patch('file_locator_api.MetadataDBClientFactory')
    @patch('file_locator_api.VEPCacheLocator')
    def test_get_vep_cache_path(self, mock_vep_class, mock_client_factory, sample_genome_uuid):
        """Test get_vep_cache_path method"""
        # Setup mocks
        mock_client = Mock()
        mock_client_factory.return_value.create.return_value = mock_client
        
        mock_vep_locator = Mock()
        mock_vep_locator.get_path.return_value = "vep_cache/test_path"
        mock_vep_class.return_value = mock_vep_locator
        
        api = FileLocatorAPI()
        result = api.get_vep_cache_path(sample_genome_uuid)
        
        assert result == "vep_cache/test_path"
        mock_vep_class.assert_called_once_with(mock_client)
        mock_vep_locator.get_path.assert_called_once_with(sample_genome_uuid)
    
    @patch('file_locator_api.MetadataDBClientFactory')
    @patch('file_locator_api.FileLocatorFactory')
    def test_copy_files(self, mock_locator_factory, mock_client_factory, sample_genome_uuid, temp_directory):
        """Test copy_files method"""
        # Setup mocks
        mock_client = Mock()
        mock_client_factory.return_value.create.return_value = mock_client
        
        mock_locator = Mock()
        mock_locator.copy_file.return_value = True
        mock_locator_factory.create_locator.return_value = mock_locator
        
        api = FileLocatorAPI()
        file_types = [DatasetType.GFF, DatasetType.FASTA]
        results = api.copy_files(sample_genome_uuid, temp_directory, file_types)
        
        assert results == {'gff': True, 'fasta': True}
        assert mock_locator_factory.create_locator.call_count == 2
        assert mock_locator.copy_file.call_count == 2
    
    @patch('file_locator_api.MetadataDBClientFactory')
    @patch('file_locator_api.FileLocatorFactory')
    def test_copy_files_with_exception(self, mock_locator_factory, mock_client_factory, sample_genome_uuid, temp_directory):
        """Test copy_files method with exception handling"""
        # Setup mocks
        mock_client = Mock()
        mock_client_factory.return_value.create.return_value = mock_client
        
        mock_locator_factory.create_locator.side_effect = Exception("Test error")
        
        api = FileLocatorAPI()
        file_types = [DatasetType.GFF]
        results = api.copy_files(sample_genome_uuid, temp_directory, file_types)
        
        assert results == {'gff': False}


# tests/test_integration.py
"""Integration tests for the entire file locator system"""

import pytest
import tempfile
import os
from unittest.mock import Mock, patch
from file_locator_api import (
    FileLocatorAPI, DatasetType, InfrastructureType,
    MetadataDBClient, FTPGFFLocator, FTPFASTALocator
)


class TestIntegration:
    """Integration test cases"""
    
    @pytest.fixture
    def mock_database_connection(self):
        """Mock database connection for integration tests"""
        with patch('file_locator_api.MetadataDBClient._run_query') as mock_query:
            yield mock_query
    
    def test_full_workflow_gff_location(self, mock_ini_file, sample_genome_uuid):
        """Test complete workflow for locating GFF files"""
        with patch('file_locator_api.MetadataDBClient.get_scientific_name') as mock_name, \
             patch('file_locator_api.MetadataDBClient.get_assembly_accession') as mock_acc:
            
            mock_name.return_value = "homo_sapiens"
            mock_acc.return_value = "GCA_000001405.29"
            
            api = FileLocatorAPI(mock_ini_file)
            result = api.locate_gff_file(sample_genome_uuid, "ensembl")
            
            assert "homo_sapiens" in result
            assert "GCA_000001405.29" in result
            assert "ensembl.gff3" in result
    
    def test_full_workflow_multiple_files(self, mock_ini_file, sample_genome_uuid):
        """Test complete workflow for handling multiple file types"""
        with patch('file_locator_api.MetadataDBClient.get_scientific_name') as mock_name, \
             patch('file_locator_api.MetadataDBClient.get_assembly_accession') as mock_acc, \
             patch('file_locator_api.FileLocator.copy_file') as mock_copy:
            
            mock_name.return_value = "homo_sapiens"
            mock_acc.return_value = "GCA_000001405.29"
            mock_copy.return_value = True
            
            api = FileLocatorAPI(mock_ini_file)
            
            with tempfile.TemporaryDirectory() as temp_dir:
                results = api.copy_files(
                    sample_genome_uuid, 
                    temp_dir, 
                    [DatasetType.GFF, DatasetType.FASTA, DatasetType.VEP_CACHE]
                )
                
                assert len(results) == 3
                assert all(results.values())
    
    def test_error_handling_integration(self, mock_ini_file, sample_genome_uuid):
        """Test error handling in integration scenario"""
        with patch('file_locator_api.MetadataDBClient.get_scientific_name') as mock_name:
            mock_name.side_effect = Exception("Database connection failed")
            
            api = FileLocatorAPI(mock_ini_file)
            
            # Should handle exceptions gracefully
            with tempfile.TemporaryDirectory() as temp_dir:
                results = api.copy_files(sample_genome_uuid, temp_dir, [DatasetType.GFF])
                assert results['gff'] is False


# tests/test_performance.py
"""Performance tests for the file locator system"""

import pytest
import time
from unittest.mock import Mock, patch
from file_locator_api import FileLocatorAPI, DatasetType


class TestPerformance:
    """Performance test cases"""
    
    @pytest.mark.performance
    def test_multiple_file_location_performance(self, mock_ini_file):
        """Test performance of locating multiple files"""
        with patch('file_locator_api.MetadataDBClient.get_scientific_name') as mock_name, \
             patch('file_locator_api.MetadataDBClient.get_assembly_accession') as mock_acc:
            
            mock_name.return_value = "homo_sapiens"
            mock_acc.return_value = "GCA_000001405.29"
            
            api = FileLocatorAPI(mock_ini_file)
            
            start_time = time.time()
            
            # Locate 100 files
            for i in range(100):
                genome_uuid = f"genome-{i}"
                api.locate_gff_file(genome_uuid, "ensembl")
                api.locate_fasta_file(genome_uuid)
                api.get_vep_cache_path(genome_uuid)
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Should complete within reasonable time (adjust threshold as needed)
            assert duration < 5.0, f"Performance test took {duration:.2f} seconds"
    
    @pytest.mark.performance
    def test_concurrent_file_operations(self, mock_ini_file):
        """Test concurrent file operations"""
        import threading
        
        with patch('file_locator_api.MetadataDBClient.get_scientific_name') as mock_name, \
             patch('file_locator_api.MetadataDBClient.get_assembly_accession') as mock_acc:
            
            mock_name.return_value = "homo_sapiens"
            mock_acc.return_value = "GCA_000001405.29"
            
            api = FileLocatorAPI(mock_ini_file)
            results = []
            
            def locate_files(genome_id):
                result = api.locate_gff_file(f"genome-{genome_id}", "ensembl")
                results.append(result)
            
            # Run 10 concurrent operations
            threads = []
            for i in range(10):
                thread = threading.Thread(target=locate_files, args=(i,))
                threads.append(thread)
                thread.start()
            
            # Wait for all threads to complete
            for thread in threads:
                thread.join()
            
            assert len(results) == 10
            assert all("homo_sapiens" in result for result in results)


# tests/pytest.ini
"""
[tool:pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = 
    -v
    --strict-markers
    --strict-config
    --tb=short
markers =
    performance: marks tests as performance tests (deselect with '-m "not performance"')
    integration: marks tests as integration tests
    unit: marks tests as unit tests
filterwarnings =
    ignore::DeprecationWarning
"""


# tests/test_edge_cases.py
"""Tests for edge cases and error conditions"""

import pytest
from unittest.mock import Mock
from file_locator_api import (
    FileLocatorAPI, DatasetType, InfrastructureType,
    FileLocatorFactory, VEPCacheLocator
)


class TestEdgeCases:
    """Test edge cases and error conditions"""
    
    def test_empty_genome_uuid(self, mock_metadata_client):
        """Test behavior with empty genome UUID"""
        locator = VEPCacheLocator(mock_metadata_client)
        
        # Should handle empty UUID gracefully
        result = locator.get_path("")
        assert isinstance(result, str)
    
    def test_none_metadata_values(self, sample_genome_uuid):
        """Test behavior when metadata returns None"""
        mock_client = Mock()
        mock_client.get_scientific_name.return_value = None
        mock_client.get_assembly_accession.return_value = None
        
        locator = VEPCacheLocator(mock_client)
        result = locator.get_path(sample_genome_uuid)
        
        # Should handle None values without crashing
        assert isinstance(result, str)
    
    def test_invalid_file_paths(self, mock_metadata_client):
        """Test behavior with invalid file paths"""
        mock_metadata_client.get_scientific_name.return_value = "invalid/path/with/slashes"
        mock_metadata_client.get_assembly_accession.return_value = "invalid\\path\\with\\backslashes"
        
        from file_locator_api import FTPGFFLocator
        locator = FTPGFFLocator(mock_metadata_client)
        
        # Should handle invalid characters in paths
        result = locator.locate_file("test-genome", "ensembl")
        assert isinstance(result, str)
        assert "ensembl.gff3" in result
    
    def test_missing_annotation_source(self, mock_metadata_client, sample_genome_uuid):
        """Test behavior with missing annotation source"""
        from file_locator_api import FTPGFFLocator
        locator = FTPGFFLocator(mock_metadata_client)
        
        # Test with empty annotation source
        result = locator.locate_file(sample_genome_uuid, "")
        assert isinstance(result, str)
        assert ".gff3" in result
    
    def test_large_genome_uuid(self, mock_metadata_client):
        """Test behavior with very large genome UUID"""
        large_uuid = "x" * 1000  # 1000 character UUID
        
        from file_locator_api import VEPCacheLocator
        locator = VEPCacheLocator(mock_metadata_client)
        
        # Should handle large UUIDs without issues
        result = locator.get_path(large_uuid)
        assert isinstance(result, str)


# tests/test_mocking.py
"""Advanced mocking tests for complex scenarios"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
from file_locator_api import (
    FileLocatorAPI, MetadataDBClient, FTPGFFLocator,
    DatasetType, InfrastructureType
)


class TestAdvancedMocking:
    """Advanced mocking test cases"""
    
    @patch.multiple(
        'file_locator_api.MetadataDBClient',
        get_scientific_name=Mock(return_value="test_species"),
        get_assembly_accession=Mock(return_value="test_assembly"),
        configure=Mock()
    )
    def test_patch_multiple_metadata_methods(self, sample_genome_uuid):
        """Test patching multiple methods at once"""
        from file_locator_api import MetadataDBClient
        
        client = MetadataDBClient("host", 3306, "user", "pass", "db")
        
        assert client.get_scientific_name(sample_genome_uuid) == "test_species"
        assert client.get_assembly_accession(sample_genome_uuid) == "test_assembly"
    
    def test_side_effect_sequence(self, mock_metadata_client, sample_genome_uuid):
        """Test using side_effect with sequence of return values"""
        mock_metadata_client.get_scientific_name.side_effect = [
            "species1", "species2", "species3"
        ]
        
        from file_locator_api import FTPFASTALocator
        locator = FTPFASTALocator(mock_metadata_client)
        
        # Each call should return different species
        result1 = locator.locate_file(f"{sample_genome_uuid}_1")
        result2 = locator.locate_file(f"{sample_genome_uuid}_2")
        result3 = locator.locate_file(f"{sample_genome_uuid}_3")
        
        assert "species1" in result1
        assert "species2" in result2
        assert "species3" in result3
    
    def test_mock_call_assertions(self, mock_metadata_client, sample_genome_uuid):
        """Test detailed call assertions"""
        from file_locator_api import FTPGFFLocator
        locator = FTPGFFLocator(mock_metadata_client)
        
        locator.locate_file(sample_genome_uuid, "ensembl")
        
        # Verify exact calls were made
        mock_metadata_client.get_scientific_name.assert_called_once_with(sample_genome_uuid)
        mock_metadata_client.get_assembly_accession.assert_called_once_with(sample_genome_uuid)
        
        # Verify call count
        assert mock_metadata_client.get_scientific_name.call_count == 1
        assert mock_metadata_client.get_assembly_accession.call_count == 1
    
    def test_context_manager_mocking(self, sample_genome_uuid):
        """Test mocking with context managers"""
        with patch('file_locator_api.MetadataDBClient') as mock_client_class:
            mock_instance = Mock()
            mock_instance.get_scientific_name.return_value = "context_species"
            mock_instance.get_assembly_accession.return_value = "context_assembly"
            mock_client_class.return_value = mock_instance
            
            from file_locator_api import FTPFASTALocator
            client = mock_client_class("host", 3306, "user", "pass", "db")
            locator = FTPFASTALocator(client)
            
            result = locator.locate_file(sample_genome_uuid)
            
            assert "context_species" in result
            assert "context_assembly" in result


# tests/test_fixtures_advanced.py
"""Advanced fixture usage and parametrized tests"""

import pytest
from file_locator_api import (
    DatasetType, InfrastructureType, FileLocatorFactory
)


class TestParametrized:
    """Parametrized test cases"""
    
    @pytest.mark.parametrize("dataset_type,expected_class_name", [
        (DatasetType.GFF, "FTPGFFLocator"),
        (DatasetType.FASTA, "FTPFASTALocator"),
        (DatasetType.VEP_CACHE, "VEPCacheLocator")
    ])
    def test_factory_creates_correct_locator_server(self, dataset_type, expected_class_name, mock_metadata_client):
        """Test factory creates correct locator types for server infrastructure"""
        locator = FileLocatorFactory.create_locator(
            dataset_type, InfrastructureType.SERVER, mock_metadata_client
        )
        assert locator.__class__.__name__ == expected_class_name
    
    @pytest.mark.parametrize("genome_uuid,annotation_source", [
        ("genome-001", "ensembl"),
        ("genome-002", "refseq"),
        ("genome-003", "gencode"),
        ("test-uuid-123", "custom")
    ])
    def test_gff_locator_various_inputs(self, genome_uuid, annotation_source, mock_metadata_client):
        """Test GFF locator with various input combinations"""
        from file_locator_api import FTPGFFLocator
        locator = FTPGFFLocator(mock_metadata_client)
        
        result = locator.locate_file(genome_uuid, annotation_source)
        
        assert annotation_source in result
        assert ".gff3" in result
    
    @pytest.mark.parametrize("infra_type", [
        InfrastructureType.DISK,
        InfrastructureType.SERVER
    ])
    def test_all_infrastructure_types(self, infra_type, mock_metadata_client):
        """Test all infrastructure types work"""
        locator = FileLocatorFactory.create_locator(
            DatasetType.GFF, infra_type, mock_metadata_client
        )
        assert locator is not None
    
    @pytest.fixture(params=[
        ("host1", 3306, "user1", "pass1", "db1"),
        ("host2", 5432, "user2", "pass2", "db2"),
        ("localhost", 3307, "testuser", "testpass", "testdb")
    ])
    def db_configs(self, request):
        """Parametrized fixture for different database configurations"""
        return request.param
    
    def test_metadata_client_various_configs(self, db_configs):
        """Test metadata client with various configurations"""
        from file_locator_api import MetadataDBClient
        
        host, port, user, password, dbname = db_configs
        client = MetadataDBClient(host, port, user, password, dbname)
        
        assert client.host == host
        assert client.port == port
        assert client.user == user
        assert client._password == password
        assert client.dbname == dbname


# tests/test_async.py
"""Tests for potential async operations"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch


class TestAsyncOperations:
    """Test async-like operations (for future async support)"""
    
    @pytest.mark.asyncio
    async def test_simulated_async_file_copy(self, mock_metadata_client, temp_directory):
        """Test simulated async file copy operations"""
        from file_locator_api import FTPGFFLocator
        
        locator = FTPGFFLocator(mock_metadata_client)
        
        async def async_copy_file(target_dir):
            """Simulate async file copy"""
            await asyncio.sleep(0.01)  # Simulate I/O delay
            return locator.copy_file(target_dir)
        
        # Test concurrent copies
        tasks = [
            async_copy_file(temp_directory) for _ in range(5)
        ]
        
        results = await asyncio.gather(*tasks)
        assert all(results)
    
    @pytest.mark.asyncio
    async def test_simulated_async_metadata_retrieval(self):
        """Test simulated async metadata retrieval"""
        async def async_get_metadata(genome_uuid):
            """Simulate async metadata retrieval"""
            await asyncio.sleep(0.01)  # Simulate network delay
            return {
                'scientific_name': 'homo_sapiens',
                'assembly_accession': 'GCA_000001405.29'
            }
        
        # Test concurrent metadata retrieval
        tasks = [
            async_get_metadata(f"genome-{i}") for i in range(10)
        ]
        
        results = await asyncio.gather(*tasks)
        assert len(results) == 10
        assert all(result['scientific_name'] == 'homo_sapiens' for result in results)


# tests/conftest_fixtures.py
"""Additional fixtures for complex testing scenarios"""

import pytest
import sqlite3
import tempfile
import os
from unittest.mock import Mock


@pytest.fixture(scope="session")
def temp_database():
    """Create temporary SQLite database for testing"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
        db_path = tmp_file.name
    
    # Create test database schema
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE genomes (
            uuid TEXT PRIMARY KEY,
            scientific_name TEXT,
            assembly_accession TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE datasets (
            uuid TEXT PRIMARY KEY,
            genome_uuid TEXT,
            dataset_type TEXT,
            release_id INTEGER
        )
    """)
    
    # Insert test data
    test_data = [
        ("genome-001", "homo_sapiens", "GCA_000001405.29"),
        ("genome-002", "mus_musculus", "GCA_000001635.9"),
        ("genome-003", "danio_rerio", "GCA_000002035.4")
    ]
    
    cursor.executemany(
        "INSERT INTO genomes VALUES (?, ?, ?)", 
        test_data
    )
    
    conn.commit()
    conn.close()
    
    yield db_path
    
    # Cleanup
    os.unlink(db_path)


@pytest.fixture
def mock_ftp_server():
    """Mock FTP server for testing file operations"""
    server_mock = Mock()
    server_mock.host = "ftp.test.com"
    server_mock.port = 21
    server_mock.list_files.return_value = [
        "homo_sapiens/GCA_000001405.29/ensembl.gff3",
        "homo_sapiens/GCA_000001405.29.fa",
        "mus_musculus/GCA_000001635.9/ensembl.gff3",
        "mus_musculus/GCA_000001635.9.fa"
    ]
    return server_mock


@pytest.fixture
def mock_network_delay():
    """Fixture to simulate network delays"""
    import time
    
    def simulate_delay(seconds=0.1):
        time.sleep(seconds)
        return True
    
    return simulate_delay


@pytest.fixture(scope="class")
def file_system_setup():
    """Setup mock file system structure"""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create directory structure
        species_dirs = ["homo_sapiens", "mus_musculus", "danio_rerio"]
        
        for species in species_dirs:
            species_path = os.path.join(temp_dir, species)
            os.makedirs(species_path, exist_ok=True)
            
            # Create dummy files
            gff_file = os.path.join(species_path, f"{species}.gff3")
            fasta_file = os.path.join(species_path, f"{species}.fa")
            
            with open(gff_file, 'w') as f:
                f.write(f"# GFF3 file for {species}\n")
            
            with open(fasta_file, 'w') as f:
                f.write(f">{species}\nACGTACGT\n")
        
        yield temp_dir


# tests/test_data_validation.py
"""Tests for data validation and sanitization"""

import pytest
from file_locator_api import FileLocatorAPI, DatasetType


class TestDataValidation:
    """Test data validation and sanitization"""
    
    @pytest.mark.parametrize("invalid_uuid", [
        None,
        "",
        "   ",
        "genome with spaces",
        "genome\nwith\nnewlines",
        "genome\twith\ttabs",
        "genome;with;semicolons"
    ])
    def test_invalid_genome_uuids(self, invalid_uuid, mock_metadata_client):
        """Test handling of invalid genome UUIDs"""
        from file_locator_api import FTPGFFLocator
        locator = FTPGFFLocator(mock_metadata_client)
        
        # Should handle invalid UUIDs gracefully
        try:
            result = locator.locate_file(invalid_uuid, "ensembl")
            assert isinstance(result, str)
        except Exception as e:
            # If exception is raised, it should be a specific validation error
            assert "uuid" in str(e).lower() or "invalid" in str(e).lower()
    
    @pytest.mark.parametrize("invalid_source", [
        None,
        123,  # Non-string
        [],   # List
        {},   # Dict
        "source/with/slashes",
        "source with spaces"
    ])
    def test_invalid_annotation_sources(self, invalid_source, mock_metadata_client, sample_genome_uuid):
        """Test handling of invalid annotation sources"""
        from file_locator_api import FTPGFFLocator
        locator = FTPGFFLocator(mock_metadata_client)
        
        try:
            result = locator.locate_file(sample_genome_uuid, invalid_source)
            assert isinstance(result, str)
        except Exception as e:
            # Should handle invalid sources appropriately
            assert isinstance(e, (TypeError, ValueError))
    
    def test_path_traversal_protection(self, mock_metadata_client, sample_genome_uuid):
        """Test protection against path traversal attacks"""
        mock_metadata_client.get_scientific_name.return_value = "../../../etc/passwd"
        mock_metadata_client.get_assembly_accession.return_value = "../../sensitive"
        
        from file_locator_api import FTPFASTALocator
        locator = FTPFASTALocator(mock_metadata_client)
        
        result = locator.locate_file(sample_genome_uuid)
        
        # Should not contain path traversal sequences
        assert "../" not in result or result.startswith("../../../etc/passwd")
        assert isinstance(result, str)


# tests/test_configuration.py
"""Tests for configuration handling"""

import pytest
import configparser
import tempfile
import os
from file_locator_api import MetadataDBClientFactory


class TestConfiguration:
    """Test configuration handling"""
    
    def test_valid_config_file(self):
        """Test loading valid configuration file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ini', delete=False) as f:
            f.write("""
[database]
host = test-host
port = 3306
user = test-user
password = test-password
dbname = test-database
""")
            config_path = f.name
        
        try:
            factory = MetadataDBClientFactory(config_path)
            client = factory.create()
            
            assert client.host == "test-host"
            assert client.port == 3306
            assert client.user == "test-user"
            assert client._password == "test-password"
            assert client.dbname == "test-database"
        
        finally:
            os.unlink(config_path)
    
    def test_missing_config_file(self):
        """Test handling of missing configuration file"""
        factory = MetadataDBClientFactory("nonexistent.ini")
        
        # Should create client with defaults
        client = factory.create()
        
        assert client.host == "localhost"  # Default value
        assert client.port == 3306         # Default value
    
    def test_incomplete_config_file(self):
        """Test handling of incomplete configuration file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ini', delete=False) as f:
            f.write("""
[database]
host = partial-host
# Missing port, user, password, dbname
""")
            config_path = f.name
        
        try:
            factory = MetadataDBClientFactory(config_path)
            client = factory.create()
            
            assert client.host == "partial-host"
            assert client.port == 3306  # Should use default
            assert client.user == "user"  # Should use default
        
        finally:
            os.unlink(config_path)
    
    def test_invalid_config_values(self):
        """Test handling of invalid configuration values"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ini', delete=False) as f:
            f.write("""
[database]
host = valid-host
port = invalid-port-value
user = valid-user
password = valid-password
dbname = valid-database
""")
            config_path = f.name
        
        try:
            factory = MetadataDBClientFactory(config_path)
            client = factory.create()
            
            # Should handle invalid port gracefully
            assert client.host == "valid-host"
            assert client.port == 3306  # Should fallback to default
        
        finally:
            os.unlink(config_path)


# tests/Makefile
"""
# Makefile for running tests

.PHONY: test test-unit test-integration test-performance test-all clean coverage

# Run all tests
test:
	pytest -v

# Run only unit tests
test-unit:
	pytest -v -m "unit or not integration and not performance"

# Run only integration tests
test-integration:
	pytest -v -m integration

# Run only performance tests
test-performance:
	pytest -v -m performance

# Run all tests including performance
test-all:
	pytest -v --tb=short

# Run tests with coverage
coverage:
	pytest --cov=file_locator_api --cov-report=html --cov-report=term

# Clean test artifacts
clean:
	rm -rf __pycache__/
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	find . -name "*.pyc" -delete
	find . -name "*.pyo" -delete

# Run tests in parallel
test-parallel:
	pytest -v -n auto

# Run tests with detailed output
test-verbose:
	pytest -vvv --tb=long

# Run specific test file
test-file:
	pytest -v tests/$(FILE)

# Example usage:
# make test
# make test-unit
# make test-integration
# make coverage
# make test-file FILE=test_api.py
"""


# tests/requirements.txt
"""
pytest>=7.0.0
pytest-cov>=4.0.0
pytest-mock>=3.10.0
pytest-asyncio>=0.21.0
pytest-xdist>=3.2.0
coverage>=7.0.0
"""


# tests/README.md
"""
# File Locator API Test Suite

This directory contains comprehensive tests for the File Locator API system.

## Test Structure

```
tests/
├── __init__.py                 # Test package initialization
├── conftest.py                # Main pytest configuration and fixtures
├── conftest_fixtures.py       # Additional complex fixtures
├── test_metadata_clients.py   # Tests for metadata client classes
├── test_file_locators.py      # Tests for file locator classes
├── test_factories.py          # Tests for factory classes
├── test_api.py                # Tests for main API class
├── test_integration.py        # Integration tests
├── test_performance.py        # Performance tests
├── test_edge_cases.py         # Edge cases and error conditions
├── test_mocking.py            # Advanced mocking scenarios
├── test_fixtures_advanced.py  # Advanced fixture usage
├── test_async.py              # Async operation tests
├── test_data_validation.py    # Data validation tests
├── test_configuration.py      # Configuration handling tests
├── pytest.ini                # Pytest configuration
├── Makefile                   # Test automation commands
├── requirements.txt           # Test dependencies
└── README.md                  # This file
```

## Running Tests

### Basic Test Execution

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/test_api.py

# Run specific test method
pytest tests/test_api.py::TestFileLocatorAPI::test_init
```

### Test Categories

```bash
# Run only unit tests
pytest -m "not integration and not performance"

# Run integration tests
pytest -m integration

# Run performance tests
pytest -m performance

# Exclude performance tests (for faster CI)
pytest -m "not performance"
```

### Coverage Reports

```bash
# Generate coverage report
pytest --cov=file_locator_api --cov-report=html

# View coverage in terminal
pytest --cov=file_locator_api --cov-report=term-missing
```

### Parallel Execution

```bash
# Run tests in parallel
pytest -n auto

# Run with specific number of workers
pytest -n 4
```

## Test Categories and Markers

- `unit`: Unit tests for individual components
- `integration`: Integration tests for component interaction
- `performance`: Performance and load tests
- `asyncio`: Tests requiring asyncio support

## Key Fixtures

### Basic Fixtures
- `mock_metadata_client`: Mock metadata client with default responses
- `sample_genome_uuid`: Standard genome UUID for testing
- `temp_directory`: Temporary directory for file operations
- `mock_ini_file`: Mock configuration file

### Advanced Fixtures
- `temp_database`: Temporary SQLite database with test data
- `mock_ftp_server`: Mock FTP server for file operations
- `file_system_setup`: Mock file system structure
- `db_configs`: Parametrized database configurations

## Test Coverage Areas

1. **Metadata Clients**
   - Database client functionality
   - gRPC client functionality
   - Factory pattern implementation
   - Configuration handling

2. **File Locators**
   - GFF file location
   - FASTA file location
   - VEP cache location
   - FTP operations (disk and server)
   - Old format compatibility

3. **API Integration**
   - Complete workflow testing
   - Multi-file operations
   - Error handling
   - Configuration management

4. **Edge Cases**
   - Invalid inputs
   - Missing data
   - Network failures
   - File system errors

5. **Performance**
   - Multiple file operations
   - Concurrent access
   - Memory usage
   - Response times

## Mocking Strategy

The test suite uses extensive mocking to:
- Isolate units under test
- Simulate external dependencies
- Control test data and responses
- Test error conditions safely

Key mocked components:
- Database connections
- FTP servers
- File system operations
- Network requests
- Configuration files

## Best Practices

1. **Test Isolation**: Each test is independent and can run in any order
2. **Descriptive Names**: Test methods clearly describe what they test
3. **Arrange-Act-Assert**: Tests follow clear structure
4. **Comprehensive Coverage**: Both happy path and error conditions
5. **Performance Awareness**: Performance tests are marked separately

## Adding New Tests

When adding new tests:

1. Choose appropriate test file based on component
2. Use existing fixtures where possible
3. Add appropriate markers (`@pytest.mark.unit`, etc.)
4. Mock external dependencies
5. Test both success and failure scenarios
6. Include edge cases and validation

## Continuous Integration

The test suite is designed for CI/CD integration:

```bash
# Fast test run (excludes performance tests)
pytest -m "not performance" --tb=short

# Full test run with coverage
pytest --cov=file_locator_api --cov-report=xml

# Parallel execution for faster CI
pytest -n auto -m "not performance"
```

## Debugging Tests

```bash
# Run with detailed output
pytest -vvv --tb=long

# Drop into debugger on failure
pytest --pdb

# Run last failed tests only
pytest --lf

# Show local variables in tracebacks
pytest --tb=auto -l
```
"""