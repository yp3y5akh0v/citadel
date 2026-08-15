"""LangChain's VectorStoreIntegrationTests run against CitadelVectorStore."""
import tempfile
import uuid

import pytest
from langchain_core.embeddings import DeterministicFakeEmbedding, Embeddings
from langchain_core.vectorstores import VectorStore
from langchain_tests.integration_tests.vectorstores import VectorStoreIntegrationTests

from citadeldb_langchain import CitadelVectorStore

# The suite's own embeddings are 6-dimensional; the store is opened to match.
DIM = 6


class TestCitadelVectorStore(VectorStoreIntegrationTests):
    @property
    def has_async(self) -> bool:
        return True

    def get_embeddings(self) -> Embeddings:
        return DeterministicFakeEmbedding(size=DIM)

    @pytest.fixture()
    def vectorstore(self) -> VectorStore:
        """A fresh, empty store per test, as the suite requires."""
        path = f"{tempfile.mkdtemp()}/{uuid.uuid4().hex}.cdl"
        return CitadelVectorStore(
            self.get_embeddings(), path, key="conformance", dim=DIM
        )
