import hashlib

from haystack import component, default_from_dict, default_to_dict


@component
class DeterministicTextEmbedder:
    def __init__(self, dim: int = 8) -> None:
        self.dim = dim
        self.model = f"test-{dim}"

    @component.output_types(embedding=list[float])
    def run(self, text: str) -> dict[str, list[float]]:
        digest = hashlib.sha256(text.lower().encode()).digest()
        return {"embedding": [digest[i % len(digest)] / 255.0 for i in range(self.dim)]}

    def to_dict(self) -> dict:
        return default_to_dict(self, dim=self.dim)

    @classmethod
    def from_dict(cls, data: dict) -> "DeterministicTextEmbedder":
        return default_from_dict(cls, data)
