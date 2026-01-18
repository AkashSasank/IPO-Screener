from abc import ABC, abstractmethod
from typing import Any, Optional, Protocol, Union


class Extractor(ABC):
    @abstractmethod
    def extract(self, filepath: str) -> dict[str, Any]:
        """
        Custom logic to extract information from raw data.
        Return extracted data.
        :param filepath:
        :return:
        """


class Fetcher(Protocol):
    def fetch(
        self,
        url: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
    ) -> Union[str, dict[str, Any]]: ...
