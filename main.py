
#---------------------------------------------------------------------------------------------------------------------
#    Focus Api Client Flow SYNC
#      this is a synchronous client flow. While productive systems are probably better off, implementing a parallel
#      approach, the synchronous flow is easier to follow and therefore better suited as an example
#
#----------------------------------------------------------------------------------------------------------------------
from datetime import datetime
import sys
import logging
import httpx
from functools import wraps
from config import Configuration
from config import ConfigV1
from utils import Schema, parse_schema_resources
import json
from utils import Database

# Basic stdout logger config
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
# Optional: set formatter if required
formatter = logging.Formatter('[%(levelname)s] %(message)s')
handler.setFormatter(formatter)
# finally configure the logger here
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Configure the logging level
logger.addHandler(handler)
logger.propagate = False


def require_auth(func):
    """
    Wrapper function so endpoints issue new access token if the current one is expiring soon
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.token_creation or (datetime.now() - self.token_creation).seconds >= 29 * 60:
            self.authenticate()
        return func(self, *args, **kwargs)
    return wrapper


class ApiReaderSync:
    """
    data exchange manager between client state and server state.
    It is an example application to visualise the sequential steps required.


    ATTENTION:

    ApiReaderSync is synchronous. It is not meant for productive purposes.
    We suggest a concurrent flow.
    """

    def __init__(self, config: ConfigV1, db: Database):
        self.config = config
        self.db = db
        self.token: str = ""
        self.token_creation: datetime = None

    def authenticate(self):
        """issues a new access token"""
        auth_endpoint = self.config.auth
        logger.info(f"authenticate using {auth_endpoint}")
        body = {"username": self.config.user, "password": self.config.password}
        with httpx.Client() as session:
            req = session.post(auth_endpoint, json=body)
            if req.status_code != 201:
                logger.error(f"Authentication failed with status [{req.status_code}]")
                raise Exception("Invalid request")
            self.token = req.json()["token"]
            self.token_creation = datetime.now()
            logger.info("authentication succeeded")

    @require_auth
    def fetch_schema(self):
        schema_endpoint = self.config.host + "/resource/schema"
        print(f"schema_endpoint: {schema_endpoint}")
        headers = {"authorization": self.token}
        with httpx.Client() as session:
            req = session.get(schema_endpoint, headers=headers)
            if req.status_code != 200:
                logger.error(f"fetching schema information failed with status [{req.status_code}]")
                raise Exception(f"Could not collect schema with status: {req.status_code}")
            # add schema to the database so we can actually create all the tables from it

            return self._parse_schema_resources(req.json())

    @require_auth
    def fetch_resource(self, resource_name: str):
        """
        decides whether to do a full synchronisation or a delta load.
        it does so by checking the client state
        :param resource_name:
        :return:
        """

        # load the latest change time on the client store
        state_ts = self.db.latest_state(resource_name)
        if not state_ts:
            # an initial full synchronisation is required
            logger.info(f"client store {resource_name} empty -> full sync for resource {resource_name}")
            self._fetch_resource_full(resource_name)
            return

        # client has state. Only new items, updated items and deleted once are required to get back in sync
        logger.info(f"client store {resource_name} not empty -> delta sync for resource {resource_name} since {state_ts}")
        self._fetch_resource_new(resource_name, state_ts)
        self._fetch_resource_updates(resource_name, state_ts)
        self.fetch_resource_deletes(resource_name, state_ts)
        return

    @require_auth
    def _fetch_resource_full(self, resource_name):
        """
        fetches all items of the provided resource and stores them to the database
        :param resource_name: str: name or the resource that should be fetched
        """
        headers = {"authorization": self.token}
        endpoint = self.config.host + f"/resource/{resource_name}/{self.config.country}/full/lines"
        params = {"lang": self.config.lang if self.config.lang else None}

        logger.info(f"fetch all items: {endpoint}, params:{params}")

        try:
            with httpx.Client(timeout=1000, headers=headers, params=params) as session:
                with session.stream("GET", endpoint) as req:
                    if req.status_code != 200:
                        logger.error(f"fetching {resource_name} failed with status_code {req.status_code}")
                        return
                    item_counter = 0
                    counter = 0
                    runs = 0
                    buffer = []

                    for line in req.iter_lines():
                        item_counter += 1
                        counter += 1
                        buffer.append(json.loads(line))
                        if len(buffer) == 5000:
                            runs += 1
                            # potentially dangerous. I don't know if queue.put creates a data copy or not
                            self.db.insert(resource=resource_name, data=[x for x in buffer])
                            logger.debug(f"[{item_counter}] items inserted successfully")
                            buffer = []
                            counter = 0

                     # send rest of the buffer again
                    if len(buffer) > 0:
                        self.db.insert(resource=resource_name, data=[x for x in buffer])
            logger.info(f"{item_counter} new items")
            return None
        except Exception as e:
            logger.error(f"fetching {resource_name} failed with with error: {e}")
            raise e

    @require_auth
    def _fetch_resource_new(self, resource_name, ts):
        """
        fetches all the new items of a given resource since the provided timestamp ts and stores them to the database
        :param resource_name: name of the resource of interest
        :param ts: timestamp: latest resource change on the client side

        """

        headers = {"authorization": self.token}
        endpoint = self.config.host + f"/resource/{resource_name}/{self.config.country}/created/lines"
        params = {"delta_timestamp": ts, "lang": self.config.lang if self.config.lang else None}
        logger.info(f"fetch new items: {endpoint}, params:{params}")

        try:
            with httpx.Client(timeout=1000, headers=headers) as session:
                with session.stream("GET", endpoint, params=params) as req:
                    if req.status_code != 200:
                        logger.error(f"fetching {resource_name} failed with status_code {req.status_code}")
                        return
                    item_counter = 0
                    counter = 0
                    runs = 0
                    buffer = []

                    for line in req.iter_lines():
                        item_counter += 1
                        counter += 1
                        buffer.append(json.loads(line))
                        if len(buffer) == 5000:
                            runs += 1
                            # potentially dangerous. I don't know if queue.put creates a data copy or not
                            self.db.insert(resource=resource_name, data=[x for x in buffer])
                            logger.debug(f"[{item_counter}] successfully inserted")
                            buffer = []
                            counter = 0
                    # send rest of the buffer again
                    if len(buffer) > 0:
                        self.db.insert(resource=resource_name, data=[x for x in buffer])


            logger.info(f"{item_counter} new items")

            return None
        except Exception as e:
            logger.error(f"fetching {resource_name} failed with with error: {e}")
            raise e

    @require_auth
    def _fetch_resource_updates(self, resource_name, ts):
        """
        fetches all updated items of the resource since the provided timestamp ts and stores them to the database
        :param resource_name: str: name or the resource that should be fetched
        :param ts: timestamp: latest resource change on the client side
        :return:
        """
        headers = {"authorization": self.token}
        endpoint = self.config.host + f"/resource/{resource_name}/{self.config.country}/updated/lines"
        params = {"delta_timestamp": ts, "lang": self.config.lang if self.config.lang else None}
        logger.info(f"fetch updated items: {endpoint}, params:{params}")

        try:
            with httpx.Client(timeout=1000, headers=headers) as session:
                with session.stream("GET", endpoint, params=params) as req:
                    if req.status_code != 200:
                        logger.error(f"fetching {resource_name} failed with status_code {req.status_code}")
                        return
                    item_counter = 0
                    counter = 0
                    runs = 0
                    buffer = []

                    for line in req.iter_lines():
                        item_counter += 1
                        counter += 1
                        buffer.append(json.loads(line))
                        if len(buffer) == 5000:
                            runs += 1
                            # potentially dangerous. I don't know if queue.put creates a data copy or not
                            self.db.upsert(resource=resource_name, data=[x for x in buffer])
                            logger.debug(f"[{item_counter}] successfully upserted")
                            buffer = []
                            counter = 0
                    # send rest of the buffer again
                    if len(buffer) > 0:
                        self.db.insert(resource=resource_name, data=[x for x in buffer])

            logger.info(f"{item_counter} upserted items")

            return None
        except Exception as e:
            logger.error(f"fetching {resource_name} failed with with error: {e}")
            raise e

    @require_auth
    def fetch_resource_deletes(self, resource_name, ts):
        """
        Loads all the ids that have been deleted since the provided timestamp ts from the server and stores deletes them in the database
        :param resource_name: str: name or the resource that should be fetched
        :param ts: timestamp: latest resource change on the client side
        """
        headers = {"authorization": self.token}
        endpoint = self.config.host + f"/resource/{resource_name}/{self.config.country}/deleted/lines"
        params = {"delta_timestamp": ts, "lang": self.config.lang if self.config.lang else None}
        logger.info(f"fetch deleted items: {endpoint}, params:{params}")

        try:
            with httpx.Client(timeout=1000, headers=headers) as session:
                with session.stream("GET", endpoint, params=params) as req:
                    if req.status_code != 200:
                        logger.error(f"fetching {resource_name} failed with status_code {req.status_code}")
                        return
                    item_counter = 0
                    counter = 0
                    runs = 0
                    buffer = []

                    for line in req.iter_lines():
                        item_counter += 1
                        counter += 1
                        buffer.append(json.loads(line))
                        if len(buffer) == 5000:
                            runs += 1
                            # potentially dangerous. I don't know if queue.put creates a data copy or not
                            self.db.delete(resource=resource_name, data=[x for x in buffer])
                            logger.debug(f"[{item_counter}] successfully deleted")
                            buffer = []
                            counter = 0
                    # send rest of the buffer again
                    if len(buffer) > 0:
                        self.db.insert(resource=resource_name, data=[x for x in buffer])
            logger.info(f"{item_counter} deleted items")

            return None
        except Exception as e:
            logger.error(f"fetching {resource_name} failed with with error: {e}")
            raise e

    @require_auth
    def fetch_state(self, resource_name, ts: datetime) -> int:
        """
        fetches the state on the server and returns the server's total number of items for a given resource
        :param resource_name: str: name or the resource that should be fetched
        :param ts: timestamp: latest resource change on the client side
        :return: int: number of total items on the server at present
        """
        headers = {"authorization": self.token}
        endpoint = self.config.host + f"/resource/{resource_name}/{self.config.country}/state"

        if ts is not None:
            ts = ts.isoformat()
        params = {"delta_timestamp": ts}
        with httpx.Client(timeout=1000, headers=headers) as session:
            req = session.get(endpoint, headers=headers, params=params)
            if req.status_code != 200:
                logger.error(f"fetching state for {resource_name} failed with status_code {req.status_code}")
                logger.error(req.text)
                return -10
            return req.json()["total_items"]

    @staticmethod
    def _parse_schema_resources(data: dict) -> Schema:
        """helper function to not have to work with dicts on schema"""
        return parse_schema_resources(data)


if __name__ == '__main__':

    with Database(Configuration, logger) as db:

        api_reader = ApiReaderSync(Configuration, db)

        # step 1:
        api_reader.authenticate()

        # step2: load the resource schema
        schema = api_reader.fetch_schema()

        # step3: build up the database schema if it does not exist (idempotent)
        api_reader.db.schema = schema
        api_reader.db.create_tables(schema)

        # step4: generally this could be done for multiple resources in parallel but for
        #        the demonstration it only regards a single resource promotions
        #      extract the latest state from the client side for the resource promotions
        promotions = api_reader.db.schema.resources[0]
        change_ts = api_reader.db.latest_state(promotions.name)
        logger.info(f"client state timestamp: {change_ts}")

        # step 5: based on the change_ts decide whether a full load or a delta load is required and perform it.
        #         generally it does not matter in which sequence the delta load is performed, as no item is in multiple
        #         endpoints
        api_reader.fetch_resource(promotions.name)

        # step 6: Check the client state vs the server state to see if both are in sync.
        client_item_count = api_reader.db.total_client_items(promotions.name)
        server_item_count = api_reader.fetch_state(promotions.name, change_ts)
        if client_item_count != server_item_count:
            logger.error("client server not in sync")
            logger.error(f"Client Items: {client_item_count}")
            logger.error(f"Server Items: {server_item_count}")
        else:
            # the resource promotions was successfully synced between server and client
            logger.info(f"client server are in sync with item count: {server_item_count}")
















