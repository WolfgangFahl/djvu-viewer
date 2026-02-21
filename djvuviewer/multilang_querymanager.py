"""
Created on 2024-09-12

@author: wf
"""

import os
from typing import List, Optional

from lodstorage.query import EndpointManager, Query, QueryManager
from lodstorage.sql_backend import get_sql_backend


class MultiLanguageQueryManager:
    """
    Query manager for multiple languages with optional SQL backend support.
    If endpoint_name is given, queries are executed via get_sql_backend.
    """

    def __init__(
        self,
        yaml_path: str,
        endpoint_name: Optional[str] = None,
        languages: list = ["sql", "sparql", "ask"],
        debug: bool = False,
    ):
        self.languages = languages
        self.endpoint_name = endpoint_name
        self.debug = debug
        self.qms = {}
        for lang in languages:
            qm = QueryManager(
                lang=lang, queriesPath=yaml_path, with_default=False, debug=self.debug
            )
            self.qms[lang] = qm

        self.query_names = []
        for qm in self.qms.values():
            self.query_names.extend(list(qm.queriesByName.keys()))

        self._backend = None
        if endpoint_name:
            yaml_path = os.path.expanduser("~/.pylodstorage/endpoints.yaml")
            em = EndpointManager.of_yaml(yaml_path=yaml_path)
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
