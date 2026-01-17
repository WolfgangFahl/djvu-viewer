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
            # @FIXME should be with_create
            self.sql_db.execute(f"DROP TABLE IF EXISTS {entity_name}")
        self.entity_info = self.sql_db.createTable(
            listOfRecords=lod,
            entityName=entity_name,
            primaryKey=primary_key,
            withCreate=with_drop,
            withDrop=with_drop,
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

    def migrate_to_package_fields(
        self, table_name: str = "djvu", field_map: dict = None, new_columns: dict = None
    ):
        """
        Migrate fields based on a field mapping and add new columns.

        Args:
            table_name: Name of the table to migrate
            field_map: Dictionary mapping old field names to new field names.
                       Defaults to tar_ prefix to package_ prefix migration.
            new_columns: Dictionary of new columns to add {column_name: sql_type}
        """
        if field_map is None:
            # Default mapping for backward compatibility of tar/zip handling
            field_map = {
                "relpath": "path",
                "tar_filesize": "package_filesize",
                "tar_iso_date": "package_iso_date",
            }

        if new_columns is None:
            new_columns = {"filename": "TEXT"}

        # Check if table exists
        cursor = self.sql_db.c.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND LOWER(name)=LOWER(?)",
            (table_name,),
        )
        if not cursor.fetchone():
            return  # Table doesn't exist yet, nothing to migrate

        # Check existing columns
        cursor = self.sql_db.c.execute(f"PRAGMA table_info({table_name})")
        columns = [row[1] for row in cursor.fetchall()]

        # Check if migration needed
        needs_rename = any(old_name in columns for old_name in field_map.keys())
        needs_new_columns = any(
            col_name not in columns for col_name in new_columns.keys()
        )

        if not needs_rename and not needs_new_columns:
            return  # Already migrated

        print(f"Migrating {table_name} index table fields...")

        # Rename columns
        for old_name, new_name in field_map.items():
            if old_name in columns:
                self.sql_db.c.execute(
                    f"ALTER TABLE {table_name} RENAME COLUMN {old_name} TO {new_name}"
                )

        # Add new columns
        for col_name, col_type in new_columns.items():
            if col_name not in columns:
                self.sql_db.c.execute(
                    f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}"
                )

        self.sql_db.commit()
