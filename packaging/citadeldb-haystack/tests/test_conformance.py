"""deepset's DocumentStoreBaseTests run against CitadelDocumentStore."""
import pytest
from haystack.dataclasses import Document
from haystack.document_stores.errors import DuplicateDocumentError
from haystack.testing.document_store import DocumentStoreBaseTests

from citadeldb_haystack import CitadelDocumentStore

# The conformance fixtures embed at 768.
DIM = 768


class TestCitadelDocumentStore(DocumentStoreBaseTests):
    @pytest.fixture
    def document_store(self, tmp_path) -> CitadelDocumentStore:
        return CitadelDocumentStore(str(tmp_path / "conformance.cdl"), "pw", dim=DIM)

    def test_write_documents(self, document_store: CitadelDocumentStore):
        """NONE falls back to FAIL, so an accidental re-write is reported."""
        doc = Document(content="test doc")
        assert document_store.write_documents([doc]) == 1
        with pytest.raises(DuplicateDocumentError):
            document_store.write_documents([doc])
