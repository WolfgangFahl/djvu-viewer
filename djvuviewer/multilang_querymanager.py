"""
Created on 2024-09-12
@author: wf
"""

from typing import List, Optional

from lodstorage.query import EndpointManager, Query, QueryManager
from lodstorage.sql_backend import get_sql_backend
from lodstorage.yaml_path import YamlPath


class MultiLanguageQueryManager:
    """
    Query manager for multiple languages with optional SQL backend support.
    If endpoint_name is given, queries are executed via get_sql_backend.

    endpoints_path: project-specific endpoints.yaml (e.g. djvuviewer_examples/endpoints.yaml).
    If None, falls back to ~/.pylodstorage/endpoints.yaml.
    """

    def __init__(
        self,
        yaml_path: Optional[str] = None,
        endpoint_name: Optional[str] = None,
        endpoints_path: Optional[str] = None,
        languages: list = ["sql", "sparql", "ask"],
        debug: bool = False,
    ):
        """
        Initialize the query manager.

        Args:
            yaml_path: Path to the project YAML queries file.
                       If None, only the lodstorage default queries.yaml is loaded.
            endpoint_name: Name of the endpoint to use for queries.
            endpoints_path: Path to project endpoints.yaml, or None for user default.
            languages: Query languages to load (sql, sparql, ask).
            debug: Enable debug output.
        """
        self.languages = languages
        self.endpoint_name = endpoint_name
        self.debug = debug
        self.qms = {}
        for lang in languages:
            qm = QueryManager(
                lang=lang,
                queriesPath=yaml_path,
                with_default=(yaml_path is None),
                debug=self.debug,
            )
            self.qms[lang] = qm

        self.query_names = []
        for qm in self.qms.values():
            self.query_names.extend(list(qm.queriesByName.keys()))

        self._backend = None
        if endpoint_name:
            yaml = endpoints_path or YamlPath.getDefaultPath("endpoints.yaml")
            em = EndpointManager.of_yaml(yaml_path=yaml)
            endpoint = em.get_endpoint(endpoint_name)
            self._backend = get_sql_backend(endpoint)

    def query4Name(self, name: str) -> Query:
        """
        Return the Query object for the given name.

        Args:
            name: query name as defined in the YAML file

        Returns:
            Query object or None
        """
        result = None
        for qm in self.qms.values():
            if name in qm.queriesByName:
                result = qm.queriesByName[name]
                break
        return result

    def store_lod(
        self, lod: List[dict], entity_name: str, primary_key: Optional[str] = None
    ) -> None:
        """
        Store a list of dicts into the configured backend (SQLite only).

        Args:
            lod: List of dicts to store.
            entity_name: Table name to create/populate.
            primary_key: Column to use as primary key.
        """
        non_none_first = sorted(lod, key=lambda r: sum(v is None for v in r.values()))
        entity_info = self._backend.createTable(
            non_none_first, entity_name, primary_key, withCreate=True, withDrop=True
        )
        self._backend.store(
            lod, entityInfo=entity_info, executeMany=True, fixNone=True, replace=True
        )

    def query(self, name: str) -> List[dict]:
        """
        Execute a named SQL query via the configured backend.

        Args:
            name: query name as defined in the YAML file

        Returns:
            List of result dicts
        """
        q = self.query4Name(name)
        if q is None:
            raise ValueError(f"Query '{name}' not found")
        if self._backend is None:
            raise ValueError(
                f"No endpoint configured — pass endpoint_name to MultiLanguageQueryManager"
            )
        rows = self._backend.query(q.query)
        return rows
