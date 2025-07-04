import logging

import psycopg2 as pg
from datetime import datetime
from utils import Schema, Attribute
import time
from config import ConfigV1


class Database:
    type_map = {"int64": "BIGINT",
                "int16": "INT",
                "int32": "INT",
                "database.nullint16": "INT",
                "int": "INT",
                "string": "VARCHAR",
                "float64": "REAL",
                "float32": "REAL",
                "uuid": "UUID",
                "time": "TIMESTAMP",
                "datetime": "TIMESTAMP",
                "date": "DATE",
                "[]string": "TEXT[]",
                "bytes": "BYTEA",
                "bool": "BOOL",
                "json": "JSON"

                }

    def __init__(self, config: ConfigV1, logger: logging.Logger):
        self.config = config
        self.conn = None
        self.cur = None
        self.schema: Schema = None
        self.logger = logger

    def __enter__(self):
        self.conn = pg.connect(self.config.connection_string)
        self.cur = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cur and self.cur is not None:
            self.cur.close()

        if self.conn and self.conn is not None:
            self.conn.close()

    # add methods required:
    def insert(self, resource,  data: list[dict]):
        """insert new data to database"""
        try:
            # convert date times
            # self.type_converter(resource, data)
            # for d in data:
            #     self._datetime_conv(d)
            # extract column names
            columns = list(data[0].keys())
            values_placeholder = ", ".join(f"%({col})s" for col in columns)
            stmt = f"INSERT INTO {resource} ({', '.join(columns)}) VALUES ({values_placeholder})"

            self.cur.executemany(stmt, data)
            self.conn.commit()
        except Exception as e:
            self.logger.error(f"inserting into {resource}: with failed: {e}")
            # self.logger.error(data[0])
            self.conn.rollback()
            # print(f"Error in PgRepo.insert for {resource_name} -> {e}")
            # raise
        finally:
            return

    def upsert(self, resource, data: list[dict]):
        """upsert data to database
            this is important to keep consistency on data corrections.
            Promotions that are corrected and therefore become part of the scope are not yet
            in the client's database. Therefore a standard update could lead to inconsistencies
        """
        try:
            # convert date times
            # self.type_converter(resource, data)
            # for d in data:
            #     self._datetime_conv(d)
            # extract column names
            columns = list(data[0].keys())
            values_placeholder = ", ".join(f"%({col})s" for col in columns)
            stmt = f"""INSERT INTO {resource} ({', '.join(columns)}) VALUES ({values_placeholder})
                        ON CONFLICT (id)
                        DO UPDATE SET
                            {', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != "id"])}
            """

            self.cur.executemany(stmt, data)
            self.conn.commit()
        except Exception as e:
            self.logger.error("UPSERT ERROR")
            self.logger.error(f"upserting {resource}: with failed: {e}")
            self.logger.error("UPSERT ERROR")
            # self.logger.error(data[0])
            self.conn.rollback()
            # print(f"Error in PgRepo.insert for {resource_name} -> {e}")
            # raise
        finally:
            return

    def delete(self, resource, data: list[dict]):
        """delete data that is nor relevant"""
        try:
            ids = [x["id"] for x in data]
            stmt = f"DELETE FROM {resource} WHERE id in %(ids)s"
            self.cur.executemany(stmt, {"ids": tuple(ids)})
            self.conn.commit()
        except Exception as e:
            self.logger.error(f"deleting {resource}: with failed: {e}")
            self.conn.rollback()
        finally:
            return

    def latest_state(self, resource):
        stmt = f"SELECT MAX(GREATEST(created_at, updated_at)) FROM {resource}"
        self.cur.execute(stmt)
        response = self.cur.fetchone()
        if response is None:
            return None
        return response[0]

    def total_client_items(self, resource):
        stmt = f"SELECT COUNT(*) FROM {resource}"
        self.cur.execute(stmt)
        response = self.cur.fetchone()
        if response is None:
            return None
        return response[0]

    def type_converter(self, resource_name,  data: list[dict]):

        tzinfo = None
        col_types_dict = self.schema.lookup[resource_name]
        for row in data:
            for k, v in row.items():
                if col_types_dict[k] == "time":
                    if v is None:
                        continue
                    # v = v.replace("Z", "+00:00")
                    v = v.replace("Z", "")
                    dt = datetime.fromisoformat(v)
                    row[k] = dt.replace(tzinfo=tzinfo)

                elif col_types_dict[k] == "date":
                    if v is None:
                        continue
                    # v = v.replace("Z", "+00:00")
                    v = v.replace("Z", "")
                    dt = datetime.fromisoformat(v).date()
                    row[k] = dt
                elif col_types_dict[k] == "datetime":
                    if v is None:
                        continue
                    # v = v.replace("Z", "+00:00")
                    v = v.replace("Z", "")
                    dt = datetime.fromisoformat(v)
                    row[k] = dt.replace(tzinfo=tzinfo)

    def create_tables(self, schema: Schema):
        try:
            for resource in schema.resources:
                stmt = f"CREATE TABLE IF NOT EXISTS {resource.name}("

                for idx, attr in enumerate(resource.attributes):
                    attr: Attribute
                    add = attr.name + " " + self._get_type_def(attr)
                    if idx == 0:
                        stmt += add
                    else:
                        stmt += "," + add
                stmt += ");"
                self.cur.execute(stmt)
                self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error creating tables with: {e}")
            raise e

        # use the schema
        return None

    def _get_type_def(self, attr: Attribute):
        db_type = self.type_map[attr.type.lower()]
        if attr.primary_key is True:
            if db_type == "BIGINT":
                db_type = "BIGSERIAL"
                default = ""
            elif db_type == "INT":
                db_type = "SERIAL"
                default = ""
            elif db_type == "UUID":
                default = "DEFAULT GEN_RANDOM_UUID()"
            else:
                default = ""
            return db_type +" PRIMARY KEY " + default

        return db_type

    def _get_resource_by_name(self, name):
        for r in self.schema.resources:
            if r.name == name:
                return r

