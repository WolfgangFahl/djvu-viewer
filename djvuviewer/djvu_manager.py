"""
Created on 2025-02-24

@author: wf
"""

from basemkit.profiler import Profiler
from lodstorage.sql import SQLDB

from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.multilang_querymanager import MultiLanguageQueryManager

class DjVuManager:
    """
    manager for DjVu files
    """

    def __init__(self, config: DjVuConfig):
        """
        Initialize a DjVuManager instance.

        Args:
           config:DjVuConfig - the DjVu configuration to use
        """
        self.config = config
        self.mlqm = MultiLanguageQueryManager(yaml_path=self.config.queries_path)
        self.sql_db = SQLDB(self.config.db_path, check_same_thread=False)

    def query(self, query_name: str, param_dict=None):
        """
        Execute a predefined SQL query based on its name and parameters.

        Args:
            query_name: Name of the query as defined in the YAML configuration.
            param_dict: Dictionary of parameters to substitute into the query.

        Returns:
            A list of dictionaries representing the query result rows.
        """
        if param_dict is None:
            param_dict = {}
        query = self.mlqm.query4Name(query_name)
        sql_query = query.params.apply_parameters_with_check(param_dict)
        lod = self.sql_db.query(sql_query, params=param_dict)
        return lod

    def store(
        self,
        lod,
        entity_name: str,
        primary_key: str,
        with_drop: bool = False,
        profile: bool = True,
        sampleRecordCount: int = 20,
    ):
        """
        Store a list of records (list of dicts) into the database.

        Args:
            lod: List of records to be stored.
            entity_name: Name of the target SQL table.
            primary_key: Column name to use as the table’s primary key.
            with_drop: If True, the existing table (if any) is dropped before creation.
            profile: If True, logs performance information using Profiler.
            sampleRecordCount: minimum number of samples
        """
        profiler = Profiler(
            f"storing {len(lod)} {entity_name} records  to SQL", profile=profile
        )
        if with_drop:
            self.sql_db.execute(f"DROP TABLE IF EXISTS {entity_name}")
        self.entity_info = self.sql_db.createTable(
            listOfRecords=lod,
            entityName=entity_name,
            primaryKey=primary_key,
            withCreate=True,
            withDrop=True,
            sampleRecordCount=sampleRecordCount,
        )
        self.sql_db.store(
            listOfRecords=lod,
            entityInfo=self.entity_info,
            executeMany=True,
            fixNone=True,
            replace=True,  # avoid UNIQUE constraint errors
        )
        profiler.time()

    def migrate_to_package_fields(self, table_name: str = "djvu"):
        """
        Migrate tar-specific fields to package-generic fields if needed.
        """
        # Check if old columns exist
        cursor = self.sql_db.c.execute(f"PRAGMA table_info({table_name})")
        columns = [row[1] for row in cursor.fetchall()]

        fields = ["filesize", "iso_date"]

        # Check if migration needed
        if not any(f"tar_{field}" in columns for field in fields):
            return  # Already migrated or new database

        print(f"Migrating {table_name} index table from tar to generic package ...")
        # Rename columns
        for field in fields:
            old_name = f"tar_{field}"
            new_name = f"package_{field}"
            if old_name in columns:
                self.sql_db.c.execute(f"ALTER TABLE {table_name} RENAME COLUMN {old_name} TO {new_name}")

        self.sql_db.c.commit()
